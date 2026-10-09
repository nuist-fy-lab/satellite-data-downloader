package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

func TestAdoptVerifiedSharesExistingAcrossOrders(t *testing.T) {
	ctx := context.Background()
	s, err := Open(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	spec := model.TaskSpec{
		Key: "global-unique-satellite-asset", SourceID: "modis",
		CollectionID: "modis:MYD06", GranuleID: "modis:granule-1", AssetID: "asset",
		Name: "cloud.hdf", URL: "https://example.invalid/archive/cloud.hdf",
		RelativeDir: "modis/2026"}
	digest := strings.Repeat("a", 64)
	first, created, err := s.AdoptVerified(ctx, spec, "/archive/cloud.hdf", digest, 1024)
	if err != nil || !created || first.State != "done" || first.SHA256 != digest {
		t.Fatalf("adopt=%+v created=%v err=%v", first, created, err)
	}
	second, created, err := s.AdoptVerified(ctx, spec, "/other/cloud.hdf", strings.Repeat("b", 64), 1024)
	if err != nil || created || second.ID != first.ID || second.ArchivePath != first.ArchivePath {
		t.Fatalf("cross-order archive silently replaced: %+v created=%v err=%v", second, created, err)
	}
	shared, created, err := s.Enqueue(ctx, spec)
	if err != nil || created || shared.ID != first.ID {
		t.Fatalf("second order did not reuse completed download: %+v %v %v", shared, created, err)
	}
	newTask := spec
	newTask.Key = "already-downloading"
	queued, created, err := s.Enqueue(ctx, newTask)
	if err != nil || !created {
		t.Fatal(err)
	}
	nowAdopted, created, err := s.AdoptVerified(ctx, newTask, "/archive/cloud.hdf", digest, 1024)
	if err != nil || created || nowAdopted.ID != queued.ID || nowAdopted.State != "queued" {
		t.Fatalf("active queue replaced with adopted archive: %+v created=%v err=%v", nowAdopted, created, err)
	}
}

func TestOfficialChecksumMigrationPreservesOldQueueAndVerificationStatus(t *testing.T) {
	name := filepath.Join(t.TempDir(), "existing.sqlite3")
	legacy, err := sql.Open("sqlite", name)
	if err != nil {
		t.Fatal(err)
	}
	// Recreate the legacy queue schema without the new verification fields.
	legacySchema := `CREATE TABLE tasks(
	  id INTEGER PRIMARY KEY AUTOINCREMENT,task_key TEXT NOT NULL UNIQUE,
	  source_id TEXT NOT NULL,collection_id TEXT NOT NULL,granule_id TEXT NOT NULL,
	  asset_id TEXT NOT NULL,filename TEXT NOT NULL,url TEXT NOT NULL,
	  auth_ref TEXT NOT NULL DEFAULT '',relative_dir TEXT NOT NULL,
	  expected_size INTEGER NOT NULL DEFAULT 0,etag TEXT NOT NULL DEFAULT '',
	  media_type TEXT NOT NULL DEFAULT '',state TEXT NOT NULL DEFAULT 'queued',
	  attempts INTEGER NOT NULL DEFAULT 0,next_attempt_ns INTEGER NOT NULL DEFAULT 0,
	  observed_size INTEGER NOT NULL DEFAULT 0,sha256 TEXT NOT NULL DEFAULT '',
	  staging_path TEXT NOT NULL DEFAULT '',archive_path TEXT NOT NULL DEFAULT '',
	  last_error TEXT NOT NULL DEFAULT '',requested_at TEXT NOT NULL,
	  started_at TEXT NOT NULL DEFAULT '',completed_at TEXT NOT NULL DEFAULT '',
	  updated_at TEXT NOT NULL);
	INSERT INTO tasks(task_key,source_id,collection_id,granule_id,asset_id,filename,url,relative_dir,requested_at,updated_at)
	VALUES('legacy-modis','modis','modis:collection','modis:G123-LAADS','123','cloud.hdf','https://example.invalid/cloud.hdf','modis/2023','2026-10-08T12:00:00Z','2026-10-08T12:00:00Z');`
	if _, err := legacy.Exec(legacySchema); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	state, err := Open(name)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	queued, err := state.TaskByKey(ctx, "legacy-modis")
	if err != nil || queued.State != "queued" || queued.ID != 1 {
		t.Fatalf("legacy task missing after migration: %+v %v", queued, err)
	}
	if err := state.CompleteWithOfficial(ctx, queued.ID, "/archive/cloud.hdf", "0123456789", 123, "verified", "MD5", "aaaa"); err != nil {
		t.Fatal(err)
	}
	verified, err := state.Task(ctx, queued.ID)
	if err != nil || verified.OfficialChecksumStatus != "verified" || verified.OfficialChecksumAlgorithm != "MD5" || verified.OfficialChecksumValue != "aaaa" {
		t.Fatalf("official verification status was not stored: %+v %v", verified, err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	verified, err = state.Task(ctx, queued.ID)
	if err != nil || verified.OfficialChecksumStatus != "verified" {
		t.Fatalf("checksum evidence lost across restart: %+v %v", verified, err)
	}
}

func TestNewDownloaderStoreDoesNotCreateAccessTables(t *testing.T) {
	state, err := Open(filepath.Join(t.TempDir(), "queue.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	var tables int
	err = state.db.QueryRow(`SELECT count(*) FROM sqlite_master WHERE type='table' AND name IN ('credentials','proxies','accounts')`).Scan(&tables)
	if err != nil || tables != 0 {
		t.Fatalf("downloader still initializes credential storage: tables=%d err=%v", tables, err)
	}
}

func TestProviderClaimsAreIsolated(t *testing.T) {
	state, err := Open(filepath.Join(t.TempDir(), "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
	for _, source := range []string{"goes", "modis"} {
		_, _, err := state.Enqueue(ctx, model.TaskSpec{
			Key: source + "-1", SourceID: source, CollectionID: source + ":product",
			GranuleID: "g", AssetID: "a", Name: source + ".dat",
			URL: "https://example.invalid/" + source, RelativeDir: source + "/2026",
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	task, ok, err := state.ClaimTransfer(ctx, "modis")
	if err != nil || !ok {
		t.Fatalf("claim modis ok=%v err=%v", ok, err)
	}
	if task.SourceID != "modis" {
		t.Fatalf("claimed %q from modis queue", task.SourceID)
	}
	goes, ok, err := state.ClaimTransfer(ctx, "goes")
	if err != nil || !ok {
		t.Fatalf("claim goes ok=%v err=%v", ok, err)
	}
	if goes.SourceID != "goes" {
		t.Fatalf("claimed %q from goes queue", goes.SourceID)
	}
}

func TestClaimTransferPrioritizesReadyRetry(t *testing.T) {
	state, err := Open(filepath.Join(t.TempDir(), "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
	queued, _, err := state.Enqueue(ctx, model.TaskSpec{
		Key: "goes-queued", SourceID: "goes", CollectionID: "goes:product",
		GranuleID: "queued", AssetID: "a", Name: "queued.nc",
		URL: "https://example.invalid/queued", RelativeDir: "goes/2026",
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, _, err := state.Enqueue(ctx, model.TaskSpec{
		Key: "goes-retry", SourceID: "goes", CollectionID: "goes:product",
		GranuleID: "retry", AssetID: "b", Name: "retry.nc",
		URL: "https://example.invalid/retry", RelativeDir: "goes/2026",
	})
	if err != nil {
		t.Fatal(err)
	}
	if queued.ID >= retry.ID {
		t.Fatalf("test requires queued id < retry id: %d >= %d", queued.ID, retry.ID)
	}
	if err := state.RetryTransfer(ctx, retry.ID, errors.New("temporary timeout"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := state.db.ExecContext(ctx, "UPDATE tasks SET next_attempt_ns=? WHERE id=?", time.Now().Add(-time.Second).UnixNano(), retry.ID); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := state.ClaimTransfer(ctx, "goes")
	if err != nil || !ok {
		t.Fatalf("claim retry ok=%v err=%v", ok, err)
	}
	if claimed.ID != retry.ID {
		t.Fatalf("claimed task=%d want ready retry=%d", claimed.ID, retry.ID)
	}
}

func TestClaimVerifyPrioritizesReadyRetry(t *testing.T) {
	state, err := Open(filepath.Join(t.TempDir(), "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
	verifying, _, err := state.Enqueue(ctx, model.TaskSpec{
		Key: "goes-verify", SourceID: "goes", CollectionID: "goes:product",
		GranuleID: "verify", AssetID: "a", Name: "verify.nc",
		URL: "https://example.invalid/verify", RelativeDir: "goes/2026",
	})
	if err != nil {
		t.Fatal(err)
	}
	retry, _, err := state.Enqueue(ctx, model.TaskSpec{
		Key: "goes-verify-retry", SourceID: "goes", CollectionID: "goes:product",
		GranuleID: "verify-retry", AssetID: "b", Name: "verify-retry.nc",
		URL: "https://example.invalid/verify-retry", RelativeDir: "goes/2026",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.MarkDownloaded(ctx, verifying.ID, "/tmp/verify", 1); err != nil {
		t.Fatal(err)
	}
	if err := state.MarkDownloaded(ctx, retry.ID, "/tmp/verify-retry", 1); err != nil {
		t.Fatal(err)
	}
	if err := state.RetryVerification(ctx, retry.ID, errors.New("temporary verify error"), time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := state.db.ExecContext(ctx, "UPDATE tasks SET next_attempt_ns=? WHERE id=?", time.Now().Add(-time.Second).UnixNano(), retry.ID); err != nil {
		t.Fatal(err)
	}
	claimed, ok, err := state.ClaimVerify(ctx, "goes")
	if err != nil || !ok {
		t.Fatalf("claim verify retry ok=%v err=%v", ok, err)
	}
	if claimed.ID != retry.ID {
		t.Fatalf("claimed verify task=%d want ready retry=%d", claimed.ID, retry.ID)
	}
}

func TestEnqueueRefreshesStaleAssetURL(t *testing.T) {
	state, err := Open(filepath.Join(t.TempDir(), "state.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	ctx := context.Background()
	original := model.TaskSpec{Key: "same", SourceID: "modis", CollectionID: "c", GranuleID: "g", AssetID: "a", Name: "x.nc", URL: "https://old.invalid/x.nc", AuthRef: "laads", RelativeDir: "modis/x", Size: 1}
	task, created, err := state.Enqueue(ctx, original)
	if err != nil || !created {
		t.Fatalf("enqueue created=%v err=%v", created, err)
	}
	if err := state.WaitForAuth(ctx, task.ID, errors.New("old auth failure")); err != nil {
		t.Fatal(err)
	}
	updated := original
	updated.URL = "https://ladsweb.modaps.eosdis.nasa.gov/archive/allData/x.nc"
	updated.Size = 2
	refreshed, created, err := state.Enqueue(ctx, updated)
	if err != nil || created {
		t.Fatalf("refresh created=%v err=%v", created, err)
	}
	if refreshed.URL != updated.URL || refreshed.Size != 2 || refreshed.State != "queued" || refreshed.LastError != "" {
		t.Fatalf("unexpected refreshed task: %#v", refreshed)
	}
}

func TestArchiveNotificationOutboxSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.sqlite3")
	state, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	task, _, err := state.Enqueue(ctx, model.TaskSpec{
		Key: "earthcare-test", SourceID: "earthcare", CollectionID: "collection",
		GranuleID: "item", AssetID: "asset", Name: "file.h5",
		URL: "https://example.invalid/file.h5", RelativeDir: "PRODUCT/20260928",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := state.Complete(ctx, task.ID, "/archive/PRODUCT/20260928/file.h5", "digest", 42); err != nil {
		t.Fatal(err)
	}
	if err := state.Close(); err != nil {
		t.Fatal(err)
	}
	state, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer state.Close()
	pending, err := state.PendingArchiveEvents(ctx, 16)
	if err != nil || len(pending) != 1 || pending[0].ID != task.ID || pending[0].ArchivePath != "/archive/PRODUCT/20260928/file.h5" {
		t.Fatalf("outbox after restart: %+v (%v)", pending, err)
	}
	if err := state.RetryArchiveEvent(ctx, task.ID, time.Hour); err != nil {
		t.Fatal(err)
	}
	pending, err = state.PendingArchiveEvents(ctx, 16)
	if err != nil || len(pending) != 0 {
		t.Fatalf("retry should defer event: %+v (%v)", pending, err)
	}
	if err := state.AcknowledgeArchiveEvent(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := state.Complete(ctx, task.ID, "/archive/PRODUCT/20260928/file.h5", "digest", 42); err != nil {
		t.Fatal(err)
	}
	pending, err = state.PendingArchiveEvents(ctx, 16)
	if err != nil || len(pending) != 1 {
		t.Fatalf("recompleted task should requeue event: %+v (%v)", pending, err)
	}
}
