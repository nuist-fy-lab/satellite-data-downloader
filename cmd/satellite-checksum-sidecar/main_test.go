package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/server"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/store"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/transfer"
)

func historicalFixture(t *testing.T) (*sidecar, *store.Store, candidate, func()) {
	t.Helper()
	root := t.TempDir()
	path := filepath.Join(root, "queue.sqlite3")
	queue, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task, _, err := queue.Enqueue(ctx, model.TaskSpec{
		Key: "historic", SourceID: "earthcare", CollectionID: "earthcare:archive",
		GranuleID: "earthcare:historic", AssetID: "archive", Name: "old.nc",
		URL: "https://example.invalid/old.nc", RelativeDir: "earthcare/2026",
	})
	if err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "archive.nc")
	data := []byte("an archived satellite product")
	if err := os.WriteFile(archive, data, 0600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	sha := hex.EncodeToString(digest[:])
	if err := queue.Complete(ctx, task.ID, archive, sha, int64(len(data))); err != nil {
		t.Fatal(err)
	}
	readOnly, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	readOnly.SetMaxOpenConns(1)
	state, err := sql.Open("sqlite", filepath.Join(root, "audit.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := state.Exec(schema); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(root, "downloader.sock")
	listener, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	httpServer := &http.Server{Handler: server.New(queue, nil)}
	go func() { _ = httpServer.Serve(listener) }()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}, Timeout: 3 * time.Second}
	s := &sidecar{queue: readOnly, state: state, resolver: transfer.NewOfficialResolver(), client: client,
		socket: sock, rate: 64 << 20, metaDelay: time.Millisecond}
	entry := candidate{id: task.ID, source: "earthcare", collection: "earthcare:archive", granule: "earthcare:historic",
		name: "old.nc", url: "https://example.invalid/old.nc", archive: archive, sha256: sha, size: int64(len(data))}
	cleanup := func() {
		_ = httpServer.Close()
		_ = listener.Close()
		_ = readOnly.Close()
		_ = state.Close()
		_ = queue.Close()
	}
	return s, queue, entry, cleanup
}

func TestSidecarAuditsLegacyArchiveWithoutMutatingDownloadState(t *testing.T) {
	s, queue, entry, cleanup := historicalFixture(t)
	defer cleanup()
	ctx := context.Background()
	entries, err := s.next(ctx, "earthcare", 0)
	if err != nil || len(entries) != 1 {
		t.Fatalf("legacy task was not discoverable: %+v %v", entries, err)
	}
	if err := s.handle(ctx, entry); err != nil {
		t.Fatal(err)
	}
	updated, err := queue.Task(ctx, entry.id)
	if err != nil {
		t.Fatal(err)
	}
	if updated.State != "done" || updated.OfficialChecksumStatus != "local_only" ||
		updated.SHA256 != entry.sha256 || updated.ArchivePath != entry.archive {
		t.Fatalf("sidecar modified live download state: %+v", updated)
	}
	entries, err = s.next(ctx, "earthcare", 0)
	if err != nil || len(entries) != 0 {
		t.Fatalf("sidecar repeated a completed audit: %+v %v", entries, err)
	}
	var status string
	if err := s.state.QueryRow("SELECT status FROM results WHERE task_id=?", entry.id).Scan(&status); err != nil || status != "recorded" {
		t.Fatalf("sidecar checkpoint not persisted: %s %v", status, err)
	}
	if err := s.saveCursor(ctx, entry.source, entry.id); err != nil {
		t.Fatal(err)
	}
	if cursor, err := s.cursor(ctx, entry.source); err != nil || cursor != entry.id {
		t.Fatalf("cursor not persisted: %d %v", cursor, err)
	}
}

func TestSidecarReportsMismatchWithoutDestroyingArchive(t *testing.T) {
	s, queue, entry, cleanup := historicalFixture(t)
	defer cleanup()
	if err := os.WriteFile(entry.archive, []byte("tampered archive of same size"), 0600); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := s.handle(ctx, entry); err != nil {
		t.Fatal(err)
	}
	unchanged, err := queue.Task(ctx, entry.id)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.State != "done" || unchanged.OfficialChecksumStatus != "legacy_local_only" {
		t.Fatalf("sidecar corrupted original task state: %+v", unchanged)
	}
	if _, err := os.Stat(entry.archive); err != nil {
		t.Fatalf("mismatching archive was removed: %v", err)
	}
	var status string
	if err := s.state.QueryRow("SELECT status FROM results WHERE task_id=?", entry.id).Scan(&status); err != nil || status != "mismatch" {
		t.Fatalf("mismatch not checkpointed: %s %v", status, err)
	}
}

func TestSidecarRetryCheckpointSkipsNotYetDueMetadata(t *testing.T) {
	s, _, entry, cleanup := historicalFixture(t)
	defer cleanup()
	ctx := context.Background()
	if err := s.record(ctx, entry, "retry", "", "", "", "transient metadata failure", 2, time.Now().Add(time.Hour).UnixNano()); err != nil {
		t.Fatal(err)
	}
	if err := s.handle(ctx, entry); err != nil {
		t.Fatal(err)
	}
	var attempts int
	if err := s.state.QueryRow("SELECT attempts FROM results WHERE task_id=?", entry.id).Scan(&attempts); err != nil || attempts != 2 {
		t.Fatalf("sidecar ignored backoff checkpoint: %d %v", attempts, err)
	}
}
