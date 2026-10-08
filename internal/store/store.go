package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
	_ "modernc.org/sqlite"
)

const schema = `
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_key TEXT NOT NULL UNIQUE,
  source_id TEXT NOT NULL,
  collection_id TEXT NOT NULL,
  granule_id TEXT NOT NULL,
  asset_id TEXT NOT NULL,
  filename TEXT NOT NULL,
  url TEXT NOT NULL,
  auth_ref TEXT NOT NULL DEFAULT '',
  relative_dir TEXT NOT NULL,
  expected_size INTEGER NOT NULL DEFAULT 0,
  etag TEXT NOT NULL DEFAULT '',
  media_type TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL DEFAULT 'queued',
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_ns INTEGER NOT NULL DEFAULT 0,
  observed_size INTEGER NOT NULL DEFAULT 0,
  sha256 TEXT NOT NULL DEFAULT '',
	 official_checksum_status TEXT NOT NULL DEFAULT '',
	 official_checksum_algorithm TEXT NOT NULL DEFAULT '',
	 official_checksum_value TEXT NOT NULL DEFAULT '',
  staging_path TEXT NOT NULL DEFAULT '',
  archive_path TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  requested_at TEXT NOT NULL,
  started_at TEXT NOT NULL DEFAULT '',
  completed_at TEXT NOT NULL DEFAULT '',
  updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tasks_provider_ready_idx
ON tasks(source_id,state,next_attempt_ns,id);
CREATE INDEX IF NOT EXISTS tasks_state_idx
ON tasks(state,updated_at,id);

CREATE TABLE IF NOT EXISTS archive_event_outbox (
  task_id INTEGER PRIMARY KEY REFERENCES tasks(id) ON DELETE CASCADE,
  next_attempt_ns INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0
);
`

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// Queue transitions are tiny. Serializing SQLite writes avoids SQLITE_BUSY
	// storms while provider network workers remain fully concurrent.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`PRAGMA busy_timeout=30000`); err != nil {
		_ = db.Close()
		return nil, err
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("initialize downloader state: %w", err)
	}
	// Online schema upgrade for existing 11k+ queued jobs; metadata is added
	// without rebuilding the database or changing existing transfer states.
	for _, column := range []string{"official_checksum_status", "official_checksum_algorithm", "official_checksum_value"} {
		var found bool
		rows, err := db.Query("PRAGMA table_info(tasks)")
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		for rows.Next() {
			var id, notnull, pk int
			var name, kind string
			var defaultValue any
			if err := rows.Scan(&id, &name, &kind, &notnull, &defaultValue, &pk); err != nil {
				rows.Close()
				_ = db.Close()
				return nil, err
			}
			if name == column {
				found = true
			}
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			_ = db.Close()
			return nil, err
		}
		rows.Close()
		if !found {
			if _, err := db.Exec("ALTER TABLE tasks ADD COLUMN " + column + " TEXT NOT NULL DEFAULT ''"); err != nil {
				_ = db.Close()
				return nil, err
			}
		}
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("secure downloader state: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) Recover(ctx context.Context) error {
	now := stamp(time.Now())
	if _, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='queued',started_at='',last_error='recovered after downloader restart',updated_at=?
		WHERE state='downloading'`, now); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='verifying',last_error='recovered interrupted promotion',updated_at=?
		WHERE state='promoting'`, now)
	return err
}

func (s *Store) Enqueue(ctx context.Context, spec model.TaskSpec) (model.Task, bool, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO tasks(
		 task_key,source_id,collection_id,granule_id,asset_id,filename,url,auth_ref,relative_dir,
		 expected_size,etag,media_type,state,requested_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,'queued',?,?)
		ON CONFLICT(task_key) DO NOTHING`,
		spec.Key, spec.SourceID, spec.CollectionID, spec.GranuleID, spec.AssetID, spec.Name, spec.URL,
		spec.AuthRef, spec.RelativeDir, spec.Size, spec.ETag, spec.MediaType, stamp(now), stamp(now))
	if err != nil {
		return model.Task{}, false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return model.Task{}, false, err
	}
	var task model.Task
	if changed == 1 {
		id, err := result.LastInsertId()
		if err != nil {
			return model.Task{}, false, err
		}
		task, err = s.Task(ctx, id)
		return task, true, err
	}
	task, err = s.TaskByKey(ctx, spec.Key)
	if err != nil {
		return model.Task{}, false, err
	}
	if task.State != "done" && (task.URL != spec.URL || task.AuthRef != spec.AuthRef || task.Size != spec.Size || task.ETag != spec.ETag || task.MediaType != spec.MediaType || task.RelativeDir != spec.RelativeDir) {
		state := task.State
		lastError := task.LastError
		nextAttempt := int64(0)
		if task.State == "waiting_auth" || task.State == "failed" || task.State == "retry_wait" {
			state = "queued"
			lastError = ""
		}
		_, updateErr := s.db.ExecContext(ctx, `
			UPDATE tasks
			SET url=?,auth_ref=?,relative_dir=?,expected_size=?,etag=?,media_type=?,state=?,next_attempt_ns=?,last_error=?,updated_at=?
			WHERE id=?`,
			spec.URL, spec.AuthRef, spec.RelativeDir, spec.Size, spec.ETag, spec.MediaType,
			state, nextAttempt, lastError, stamp(now), task.ID)
		if updateErr != nil {
			return model.Task{}, false, updateErr
		}
		task, err = s.Task(ctx, task.ID)
	}
	return task, false, err
}

func (s *Store) ClaimTransfer(ctx context.Context, sourceID string) (model.Task, bool, error) {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Task{}, false, err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM tasks
		WHERE source_id=?
		  AND state IN ('queued','retry_wait')
		  AND next_attempt_ns<=?
		ORDER BY CASE state WHEN 'retry_wait' THEN 0 ELSE 1 END, next_attempt_ns, id
		LIMIT 1`, sourceID, now.UnixNano()).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Task{}, false, nil
	}
	if err != nil {
		return model.Task{}, false, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET state='downloading',attempts=attempts+1,started_at=?,last_error='',updated_at=?
		WHERE id=? AND state IN ('queued','retry_wait')`,
		stamp(now), stamp(now), id)
	if err != nil {
		return model.Task{}, false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return model.Task{}, false, err
	}
	if changed != 1 {
		return model.Task{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return model.Task{}, false, err
	}
	task, err := s.Task(ctx, id)
	return task, err == nil, err
}

func (s *Store) ClaimVerify(ctx context.Context, sourceID string) (model.Task, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return model.Task{}, false, err
	}
	defer tx.Rollback()
	var id int64
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM tasks
		WHERE source_id=? AND state IN ('verifying','verify_wait') AND next_attempt_ns<=?
		ORDER BY CASE state WHEN 'verify_wait' THEN 0 ELSE 1 END, next_attempt_ns, id
		LIMIT 1`, sourceID, time.Now().UnixNano()).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return model.Task{}, false, nil
	}
	if err != nil {
		return model.Task{}, false, err
	}
	now := stamp(time.Now())
	result, err := tx.ExecContext(ctx, `
		UPDATE tasks SET state='promoting',next_attempt_ns=0,updated_at=?
		WHERE id=? AND state IN ('verifying','verify_wait')`, now, id)
	if err != nil {
		return model.Task{}, false, err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return model.Task{}, false, err
	}
	if changed != 1 {
		return model.Task{}, false, nil
	}
	if err := tx.Commit(); err != nil {
		return model.Task{}, false, err
	}
	task, err := s.Task(ctx, id)
	return task, err == nil, err
}

func (s *Store) MarkDownloaded(ctx context.Context, id int64, stagingPath string, observedSize int64) error {
	now := stamp(time.Now())
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='verifying',staging_path=?,observed_size=?,next_attempt_ns=0,last_error='',updated_at=?
		WHERE id=?`, stagingPath, observedSize, now, id)
	return err
}

func (s *Store) RetryTransfer(ctx context.Context, id int64, cause error, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='retry_wait',next_attempt_ns=?,last_error=?,updated_at=?
		WHERE id=?`, now.Add(delay).UnixNano(), safeError(cause), stamp(now), id)
	return err
}

func (s *Store) WaitForAuth(ctx context.Context, id int64, cause error) error {
	now := stamp(time.Now())
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='waiting_auth',next_attempt_ns=0,last_error=?,updated_at=?
		WHERE id=?`, safeError(cause), now, id)
	return err
}

func (s *Store) ReleaseWaitingAuth(ctx context.Context, sourceID string) error {
	now := stamp(time.Now())
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='queued',last_error='',updated_at=?
		WHERE source_id=? AND state='waiting_auth'`, now, sourceID)
	return err
}

func (s *Store) RetryVerification(ctx context.Context, id int64, cause error, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='verify_wait',next_attempt_ns=?,last_error=?,updated_at=?
		WHERE id=?`, now.Add(delay).UnixNano(), safeError(cause), stamp(now), id)
	return err
}

func (s *Store) Redownload(ctx context.Context, id int64, cause error, delay time.Duration) error {
	if delay < time.Second {
		delay = time.Second
	}
	now := time.Now().UTC()
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='retry_wait',staging_path='',observed_size=0,sha256='',next_attempt_ns=?,last_error=?,updated_at=?
		WHERE id=?`, now.Add(delay).UnixNano(), safeError(cause), stamp(now), id)
	return err
}

func (s *Store) Complete(ctx context.Context, id int64, archivePath, digest string, size int64) error {
	return s.CompleteWithOfficial(ctx, id, archivePath, digest, size, "", "", "")
}

func (s *Store) CompleteWithOfficial(ctx context.Context, id int64, archivePath, digest string, size int64, status, algorithm, value string) error {
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `
		UPDATE tasks
		SET state='done',archive_path=?,sha256=?,observed_size=?,official_checksum_status=?,official_checksum_algorithm=?,official_checksum_value=?,staging_path='',completed_at=?,last_error='',updated_at=?
		WHERE id=?`, archivePath, digest, size, status, algorithm, value, stamp(now), stamp(now), id)
	if err != nil {
		return err
	}
	updated, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if updated != 1 {
		return errors.New("completed task not found")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO archive_event_outbox(task_id) VALUES(?)
		ON CONFLICT(task_id) DO UPDATE SET next_attempt_ns=0,attempts=0`, id); err != nil {
		return err
	}
	return tx.Commit()
}

// RecordArchiveAudit updates checksum evidence only. The sidecar is never
// allowed to change the download state, SHA-256, archived file path or
// completion event. The expected local SHA-256 and archive path act as an
// optimistic concurrency guard against races with ordinary downloads.
func (s *Store) RecordArchiveAudit(ctx context.Context, id int64, archivePath, digest, status, algorithm, value string) error {
	if id <= 0 || archivePath == "" || len(digest) != 64 ||
		(status != "verified" && status != "local_only") ||
		(status == "verified" && (algorithm == "" || value == "")) ||
		(status == "local_only" && (algorithm != "" || value != "")) {
		return errors.New("invalid archived checksum audit")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET official_checksum_status=?,
			official_checksum_algorithm=?,official_checksum_value=?
		WHERE id=? AND state='done' AND official_checksum_status=''
		AND archive_path=? AND sha256=?`,
		status, algorithm, value, id, archivePath, digest)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		// A successful sidecar write may be acknowledged by the downloader
		// just before the sidecar itself crashes. Accept an exact replay so
		// retrying a result never marks valid archived files as stale.
		var existingStatus, existingAlgorithm, existingValue string
		err := s.db.QueryRowContext(ctx, `SELECT official_checksum_status,official_checksum_algorithm,official_checksum_value
			FROM tasks WHERE id=? AND state='done' AND archive_path=? AND sha256=?`,
			id, archivePath, digest).Scan(&existingStatus, &existingAlgorithm, &existingValue)
		if err == nil && existingStatus == status && existingAlgorithm == algorithm && existingValue == value {
			return nil
		}
		return errors.New("archive audit rejected: stale task or different verification evidence")
	}
	return nil
}

func (s *Store) PendingArchiveEvents(ctx context.Context, limit int) ([]model.Task, error) {
	rows, err := s.db.QueryContext(ctx, taskSelect+`
		WHERE state='done' AND id IN (SELECT task_id FROM archive_event_outbox WHERE next_attempt_ns<=?)
		ORDER BY id LIMIT ?`, time.Now().UnixNano(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var pending []model.Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		pending = append(pending, task)
	}
	return pending, rows.Err()
}

func (s *Store) RetryArchiveEvent(ctx context.Context, taskID int64, delay time.Duration) error {
	_, err := s.db.ExecContext(ctx, `UPDATE archive_event_outbox
		SET next_attempt_ns=?,attempts=attempts+1 WHERE task_id=?`, time.Now().Add(delay).UnixNano(), taskID)
	return err
}

func (s *Store) AcknowledgeArchiveEvent(ctx context.Context, taskID int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM archive_event_outbox WHERE task_id=?`, taskID)
	return err
}

func (s *Store) Fail(ctx context.Context, id int64, cause error) error {
	now := stamp(time.Now())
	_, err := s.db.ExecContext(ctx, `
		UPDATE tasks SET state='failed',last_error=?,updated_at=? WHERE id=?`,
		safeError(cause), now, id)
	return err
}

func (s *Store) RetryTask(ctx context.Context, id int64) error {
	now := stamp(time.Now())
	result, err := s.db.ExecContext(ctx, `
		UPDATE tasks
		SET state='queued',next_attempt_ns=0,last_error='',updated_at=?
		WHERE id=? AND state IN ('failed','retry_wait','verify_wait','waiting_auth')`, now, id)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed != 1 {
		return errors.New("task is not retryable")
	}
	return nil
}

func (s *Store) Task(ctx context.Context, id int64) (model.Task, error) {
	row := s.db.QueryRowContext(ctx, taskSelect+` WHERE id=?`, id)
	return scanTask(row)
}

func (s *Store) TaskByKey(ctx context.Context, key string) (model.Task, error) {
	row := s.db.QueryRowContext(ctx, taskSelect+` WHERE task_key=?`, key)
	return scanTask(row)
}

func (s *Store) Tasks(ctx context.Context, sourceID, state string, limit int) ([]model.Task, error) {
	if limit < 1 {
		limit = 50
	}
	if limit > 500 {
		limit = 500
	}
	query := taskSelect + ` WHERE 1=1`
	args := make([]any, 0, 3)
	if sourceID = strings.TrimSpace(sourceID); sourceID != "" {
		query += ` AND source_id=?`
		args = append(args, sourceID)
	}
	if state = strings.TrimSpace(state); state != "" {
		query += ` AND state=?`
		args = append(args, state)
	}
	query += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]model.Task, 0, limit)
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *Store) TasksByIDs(ctx context.Context, ids []int64) ([]model.Task, error) {
	if len(ids) == 0 {
		return []model.Task{}, nil
	}
	if len(ids) > 500 {
		return nil, errors.New("at most 500 task ids may be queried at once")
	}
	placeholders := make([]string, len(ids))
	args := make([]any, len(ids))
	for index, id := range ids {
		if id <= 0 {
			return nil, errors.New("task ids must be positive")
		}
		placeholders[index] = "?"
		args[index] = id
	}
	rows, err := s.db.QueryContext(ctx, taskSelect+` WHERE id IN (`+strings.Join(placeholders, ",")+`) ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]model.Task, 0, len(ids))
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, task)
	}
	return out, rows.Err()
}

func (s *Store) StateCounts(ctx context.Context, sourceID string) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT state,count(*) FROM tasks WHERE source_id=? GROUP BY state`, sourceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var state string
		var count int
		if err := rows.Scan(&state, &count); err != nil {
			return nil, err
		}
		out[state] = count
	}
	return out, rows.Err()
}

const taskSelect = `
SELECT id,task_key,source_id,collection_id,granule_id,asset_id,filename,url,auth_ref,relative_dir,
       expected_size,etag,media_type,state,attempts,next_attempt_ns,observed_size,sha256,
       official_checksum_status,official_checksum_algorithm,official_checksum_value,staging_path,
       archive_path,last_error,requested_at,started_at,completed_at,updated_at
FROM tasks`

type scanner interface {
	Scan(...any) error
}

func scanTask(row scanner) (model.Task, error) {
	var task model.Task
	var nextAttemptNS int64
	var requested, started, completed, updated string
	err := row.Scan(
		&task.ID, &task.Key, &task.SourceID, &task.CollectionID, &task.GranuleID, &task.AssetID,
		&task.Name, &task.URL, &task.AuthRef, &task.RelativeDir, &task.Size, &task.ETag, &task.MediaType,
		&task.State, &task.Attempts, &nextAttemptNS, &task.ObservedSize, &task.SHA256,
		&task.OfficialChecksumStatus, &task.OfficialChecksumAlgorithm, &task.OfficialChecksumValue, &task.StagingPath,
		&task.ArchivePath, &task.LastError, &requested, &started, &completed, &updated,
	)
	if err != nil {
		return model.Task{}, err
	}
	task.RequestedAt = parseStamp(requested)
	task.StartedAt = parseStamp(started)
	task.CompletedAt = parseStamp(completed)
	task.UpdatedAt = parseStamp(updated)
	if task.State == "done" && task.OfficialChecksumStatus == "" {
		// Do not present pre-upgrade archives as officially verified.
		task.OfficialChecksumStatus = "legacy_local_only"
	}
	if nextAttemptNS > 0 {
		task.NextAttemptAt = time.Unix(0, nextAttemptNS).UTC()
	}
	return task, nil
}

func stamp(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

func parseStamp(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed.UTC()
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	value := strings.TrimSpace(err.Error())
	if len(value) > 2000 {
		value = value[:2000]
	}
	return value
}
