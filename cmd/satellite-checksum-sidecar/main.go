// satellite-checksum-sidecar independently audits completed satellite
// archives. It NEVER enters the live download or verification pipelines.
// Results are checkpointed to its own SQLite DB before acknowledging the
// official checksum evidence over the downloader's local Unix socket.
package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/transfer"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS results (
  task_id INTEGER PRIMARY KEY,
  source_id TEXT NOT NULL,
  status TEXT NOT NULL,
  algorithm TEXT NOT NULL DEFAULT '',
  official_value TEXT NOT NULL DEFAULT '',
  local_sha256 TEXT NOT NULL DEFAULT '',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_retry_ns INTEGER NOT NULL DEFAULT 0,
  detail TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS cursors (
  source_id TEXT PRIMARY KEY,
  last_id INTEGER NOT NULL DEFAULT 0
);
`

var providers = []string{"modis", "viirs", "calipso", "goes", "mtg", "himawari9", "himawari8", "earthcare", "cloudsat", "era5"}

type candidate struct {
	id                                                      int64
	source, collection, granule, name, url, archive, sha256 string
	size                                                    int64
}

type sidecar struct {
	queue, state *sql.DB
	resolver     *transfer.OfficialResolver
	client       *http.Client
	socket       string
	rate         int64
	metaDelay    time.Duration
}

func main() {
	home, err := os.UserHomeDir()
	if err != nil {
		slog.Error("find home", "error", err)
		os.Exit(1)
	}
	dbpath := flag.String("queue-db", filepath.Join(home, ".local/state/satellite-data-downloader/downloader.sqlite3"), "read-only live downloader database")
	statepath := flag.String("state-db", filepath.Join(home, ".local/state/satellite-data-checksum-sidecar/audit.sqlite3"), "exclusive sidecar checkpoint database")
	socket := flag.String("socket", filepath.Join("/run/user", strconv.Itoa(os.Getuid()), "satellite-data-downloader/downloader.sock"), "live downloader Unix socket for tiny result updates")
	limit := flag.Int64("read-mibps", 8, "maximum historic archive read rate in MiB/s (one worker)")
	interval := flag.Duration("metadata-interval", 500*time.Millisecond, "minimum spacing between public metadata lookups")
	flag.Parse()
	if *limit < 1 || *limit > 256 || *interval < 50*time.Millisecond {
		slog.Error("invalid sidecar resource limit")
		os.Exit(2)
	}
	if err := os.MkdirAll(filepath.Dir(*statepath), 0700); err != nil {
		slog.Error("create state directory", "error", err)
		os.Exit(1)
	}
	queue, err := sql.Open("sqlite", "file:"+*dbpath+"?mode=ro")
	if err != nil {
		slog.Error("open read-only downloader database", "error", err)
		os.Exit(1)
	}
	defer queue.Close()
	queue.SetMaxOpenConns(1)
	if _, err := queue.Exec("PRAGMA query_only=ON"); err != nil {
		slog.Error("lock downloader database read-only", "error", err)
		os.Exit(1)
	}
	state, err := sql.Open("sqlite", *statepath)
	if err != nil {
		slog.Error("open sidecar database", "error", err)
		os.Exit(1)
	}
	defer state.Close()
	state.SetMaxOpenConns(1)
	if _, err := state.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=3000; " + schema); err != nil {
		slog.Error("initialize sidecar database", "error", err)
		os.Exit(1)
	}
	_ = os.Chmod(*statepath, 0600)
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, "unix", *socket)
		},
	}
	processor := &sidecar{queue: queue, state: state, resolver: transfer.NewOfficialResolver(),
		client: &http.Client{Transport: transport, Timeout: 12 * time.Second}, socket: *socket,
		rate: *limit << 20, metaDelay: *interval}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	slog.Info("historic verification sidecar started", "read_mibps", *limit, "metadata_interval", *interval)
	if err := processor.run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		slog.Error("historic verification sidecar exited", "error", err)
		os.Exit(1)
	}
}

func (s *sidecar) run(ctx context.Context) error {
	for {
		processed := 0
		for _, source := range providers {
			for {
				if err := ctx.Err(); err != nil {
					return err
				}
				last, err := s.cursor(ctx, source)
				if err != nil {
					return err
				}
				entries, err := s.next(ctx, source, last)
				if err != nil {
					return err
				}
				if len(entries) == 0 {
					break
				}
				for _, entry := range entries {
					if err := ctx.Err(); err != nil {
						return err
					}
					if err := s.handle(ctx, entry); err != nil {
						slog.Warn("sidecar failed to process task", "source", source, "task", entry.id, "error", err)
					}
					if err := s.saveCursor(ctx, source, entry.id); err != nil {
						return err
					}
					processed++
				}
			}
			if err := s.saveCursor(ctx, source, 0); err != nil {
				return err
			}
		}
		slog.Info("historic audit sweep complete", "inspected", processed)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Minute):
		}
	}
}

func (s *sidecar) cursor(ctx context.Context, source string) (int64, error) {
	var id int64
	err := s.state.QueryRowContext(ctx, "SELECT last_id FROM cursors WHERE source_id=?", source).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, nil
	}
	return id, err
}

func (s *sidecar) saveCursor(ctx context.Context, source string, id int64) error {
	_, err := s.state.ExecContext(ctx, `INSERT INTO cursors(source_id,last_id) VALUES(?,?)
		ON CONFLICT(source_id) DO UPDATE SET last_id=excluded.last_id`, source, id)
	return err
}

func (s *sidecar) next(ctx context.Context, source string, after int64) ([]candidate, error) {
	rows, err := s.queue.QueryContext(ctx, `SELECT id,source_id,collection_id,granule_id,filename,url,archive_path,sha256,observed_size
		FROM tasks WHERE state='done' AND official_checksum_status='' AND source_id=?
		AND id>? ORDER BY id LIMIT 32`, source, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []candidate
	for rows.Next() {
		var v candidate
		if err := rows.Scan(&v.id, &v.source, &v.collection, &v.granule, &v.name, &v.url, &v.archive, &v.sha256, &v.size); err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, rows.Err()
}

func (s *sidecar) handle(ctx context.Context, c candidate) error {
	var oldStatus string
	var attempts int
	var due int64
	err := s.state.QueryRowContext(ctx, "SELECT status,attempts,next_retry_ns FROM results WHERE task_id=?", c.id).Scan(&oldStatus, &attempts, &due)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if oldStatus == "mismatch" || oldStatus == "missing" || oldStatus == "recorded" {
		return nil
	}
	if oldStatus == "retry" && time.Now().UnixNano() < due {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if c.archive == "" || len(c.sha256) != 64 {
		return s.record(ctx, c, "missing", "", "", "", "missing archived path or SHA-256", attempts+1, 0)
	}
	info, err := os.Stat(c.archive)
	if err != nil {
		return s.record(ctx, c, "missing", "", "", "", err.Error(), attempts+1, 0)
	}
	if !info.Mode().IsRegular() || (c.size > 0 && c.size != info.Size()) {
		return s.record(ctx, c, "mismatch", "", "", "", "archived file size differs from completed task", attempts+1, 0)
	}
	// Throttle every metadata attempt, including errors, so a remote outage
	// cannot turn this background scanner into an upstream request storm.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.metaDelay):
	}
	// Metadata calls use a separate HTTP client, never the live download
	// transport pools.
	checks, err := s.resolver.Resolve(ctx, model.Task{TaskSpec: model.TaskSpec{
		SourceID: c.source, CollectionID: c.collection, GranuleID: c.granule,
		Name: c.name, URL: c.url}})
	if err != nil {
		return s.deferRetry(ctx, c, attempts+1, err)
	}
	sha, err := transfer.VerifyArchivedFile(c.archive, checks, s.rate)
	if err != nil {
		var integrity *transfer.IntegrityError
		if errors.As(err, &integrity) {
			return s.record(ctx, c, "mismatch", "", "", "", integrity.Error(), attempts+1, 0)
		}
		return s.deferRetry(ctx, c, attempts+1, err)
	}
	if !strings.EqualFold(c.sha256, sha) {
		return s.record(ctx, c, "mismatch", "", "", sha, "archived file no longer matches stored SHA-256", attempts+1, 0)
	}
	status := "local_only"
	var algorithm, value string
	if len(checks) > 0 {
		status = "verified"
		for i, item := range checks {
			if i > 0 {
				algorithm += ","
				value += ","
			}
			algorithm += item.Algorithm
			value += item.Value
		}
	}
	if err := s.report(ctx, c, status, algorithm, value, sha); err != nil {
		return s.deferRetry(ctx, c, attempts+1, err)
	}
	if err := s.record(ctx, c, "recorded", algorithm, value, sha, "", attempts+1, 0); err != nil {
		return err
	}
	slog.Info("historic checksum audit complete", "source", c.source, "task", c.id, "status", status, "algorithm", algorithm, "size", info.Size())
	return nil
}

func (s *sidecar) report(ctx context.Context, c candidate, status, algorithm, value, sha string) error {
	payload := map[string]string{"archive_path": c.archive, "sha256": sha, "status": status, "algorithm": algorithm, "value": value}
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		fmt.Sprintf("http://unix/v1/tasks/%d/archive-audit", c.id), bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 300))
		return fmt.Errorf("audit status update HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (s *sidecar) deferRetry(ctx context.Context, c candidate, attempts int, cause error) error {
	// Independent bounded exponential backoff; metadata outages never stop
	// ordinary downloads and never result in terminal archive mutations.
	stage := min(attempts-1, 8)
	delay := 5 * time.Second * time.Duration(1<<stage)
	if delay > 30*time.Minute {
		delay = 30 * time.Minute
	}
	delay = delay/2 + time.Duration(rand.Int64N(int64(delay/2)+1))
	slog.Warn("historic audit will retry", "source", c.source, "task", c.id, "delay", delay, "error", cause)
	return s.record(ctx, c, "retry", "", "", "", cause.Error(), attempts, time.Now().Add(delay).UnixNano())
}

func (s *sidecar) record(ctx context.Context, c candidate, status, algorithm, value, sha, detail string, attempts int, due int64) error {
	if len(detail) > 500 {
		detail = detail[:500]
	}
	_, err := s.state.ExecContext(ctx, `INSERT INTO results(task_id,source_id,status,algorithm,official_value,local_sha256,attempts,next_retry_ns,detail,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(task_id) DO UPDATE SET status=excluded.status,algorithm=excluded.algorithm,
		official_value=excluded.official_value,local_sha256=excluded.local_sha256,attempts=excluded.attempts,
		next_retry_ns=excluded.next_retry_ns,detail=excluded.detail,updated_at=excluded.updated_at`,
		c.id, c.source, status, algorithm, value, sha, attempts, due, detail, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}
