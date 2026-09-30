package transfer

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

func TestSafeRelativeDir(t *testing.T) {
	for _, value := range []string{"goes/2026/09/24", "modis/Terra"} {
		if _, err := safeRelativeDir(value); err != nil {
			t.Fatalf("safe path %q rejected: %v", value, err)
		}
	}
	for _, value := range []string{"", ".", "..", "../escape", "/absolute"} {
		if _, err := safeRelativeDir(value); err == nil {
			t.Fatalf("unsafe path %q accepted", value)
		}
	}
}

func TestParseContentRange(t *testing.T) {
	start, end, total, ok := parseContentRange("bytes 100-199/1000")
	if !ok || start != 100 || end != 199 || total != 1000 {
		t.Fatalf("unexpected range %d-%d/%d ok=%v", start, end, total, ok)
	}
}

func TestPromoteDoesNotReplaceDifferentArchiveContent(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging")
	if err := os.WriteFile(staging, []byte("new content"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "archive")
	target := filepath.Join(archive, "AUX_JSG_1D", "20260924", "file.h5")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("old content"), 0o600); err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: 1, TaskSpec: model.TaskSpec{Name: "file.h5", RelativeDir: "AUX_JSG_1D/20260924"}, StagingPath: staging}
	_, err := Promote(task, archive)
	var collision *CollisionError
	if !errors.As(err, &collision) {
		t.Fatalf("expected archive collision, got %v", err)
	}
	content, err := os.ReadFile(target)
	if err != nil || string(content) != "old content" {
		t.Fatalf("existing archive changed: %q (%v)", content, err)
	}
	if _, err := os.Stat(staging); err != nil {
		t.Fatalf("staging file lost on collision: %v", err)
	}
}

func TestPromoteReusesMatchingArchiveContent(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, "staging")
	if err := os.WriteFile(staging, []byte("same content"), 0o600); err != nil {
		t.Fatal(err)
	}
	archive := filepath.Join(root, "archive")
	target := filepath.Join(archive, "CDS", "dataset", "job-1", "file.nc")
	if err := os.MkdirAll(filepath.Dir(target), 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, []byte("same content"), 0o600); err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: 2, TaskSpec: model.TaskSpec{Name: "file.nc", RelativeDir: "CDS/dataset/job-1"}, StagingPath: staging}
	result, err := Promote(task, archive)
	if err != nil || result.Path != target {
		t.Fatalf("matching archive not reused: %+v (%v)", result, err)
	}
	if _, err := os.Stat(staging); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("staging file not removed: %v", err)
	}
}

func TestPromoteRejectsArchiveDirectorySymlink(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "archive")
	outside := filepath.Join(root, "outside")
	for _, dir := range []string{archive, outside} {
		if err := os.Mkdir(dir, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink(outside, filepath.Join(archive, "AUX_JSG_1D")); err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(root, "staging")
	if err := os.WriteFile(staging, []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: 3, TaskSpec: model.TaskSpec{Name: "file.h5", RelativeDir: "AUX_JSG_1D/20260924"}, StagingPath: staging}
	if _, err := Promote(task, archive); err == nil {
		t.Fatal("archive path traversed directory symlink")
	}
	if _, err := os.Stat(filepath.Join(outside, "20260924")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("wrote outside archive root: %v", err)
	}
}

func TestPromoteSharedArchiveCreatesGroupWritableFiles(t *testing.T) {
	root := t.TempDir()
	archive := filepath.Join(root, "archive")
	if err := os.Mkdir(archive, 0o770); err != nil {
		t.Fatal(err)
	}
	staging := filepath.Join(root, "staging")
	if err := os.WriteFile(staging, []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: 4, TaskSpec: model.TaskSpec{Name: "file.nc", RelativeDir: "CDS/dataset/2026/09/28/job-1"}, StagingPath: staging}
	result, err := Promote(task, archive, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, part := range []string{"CDS", "CDS/dataset", "CDS/dataset/2026/09/28/job-1"} {
		info, err := os.Stat(filepath.Join(archive, part))
		if err != nil || info.Mode().Perm() != 0o775 || info.Mode()&os.ModeSetgid == 0 {
			t.Fatalf("shared directory %s mode: %v (%v)", part, info, err)
		}
	}
	info, err := os.Stat(result.Path)
	if err != nil || info.Mode().Perm() != 0o664 {
		t.Fatalf("shared file mode: %v (%v)", info, err)
	}
}

func TestDownloadResumesOnlyMatchingObject(t *testing.T) {
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("ETag", `"original"`)
		w.Header().Set("Content-Length", "10")
		if requests == 1 {
			_, _ = w.Write([]byte("12345"))
			return
		}
		if r.Header.Get("Range") != "bytes=5-" || r.Header.Get("If-Range") != `"original"` {
			t.Errorf("unexpected resume headers: %v", r.Header)
		}
		w.Header().Set("Content-Range", "bytes 5-9/10")
		w.Header().Set("Content-Length", "5")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("67890"))
	}))
	defer server.Close()
	task := model.Task{ID: 42, TaskSpec: model.TaskSpec{SourceID: "goes", Name: "file.nc", URL: server.URL, Size: 10}}
	root := t.TempDir()
	client := NewHTTP(server.Client())
	if _, err := client.Download(context.Background(), task, root, nil); err == nil {
		t.Fatal("expected interrupted first download")
	}
	result, err := client.Download(context.Background(), task, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(result.Path)
	if err != nil || string(data) != "1234567890" || requests != 2 {
		t.Fatalf("resumed content=%q requests=%d err=%v", data, requests, err)
	}
}

func TestDownloadRejectsUnidentifiedPart(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			t.Error("unidentified part must not be resumed")
		}
		w.Header().Set("ETag", `"current"`)
		_, _ = w.Write([]byte("new content"))
	}))
	defer server.Close()
	root := t.TempDir()
	dir := filepath.Join(root, "staging", "goes", "7")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "file.nc.part"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	task := model.Task{ID: 7, TaskSpec: model.TaskSpec{SourceID: "goes", Name: "file.nc", URL: server.URL}}
	result, err := NewHTTP(server.Client()).Download(context.Background(), task, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(result.Path)
	if err != nil || string(data) != "new content" {
		t.Fatalf("fresh content=%q err=%v", data, err)
	}
}

func TestDownloadRejectsShortPartialRange(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Range", "bytes 0-4/10")
		w.Header().Set("ETag", `"current"`)
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("12345"))
	}))
	defer server.Close()
	task := model.Task{ID: 3, TaskSpec: model.TaskSpec{SourceID: "goes", Name: "file.nc", URL: server.URL, Size: 10}}
	root := t.TempDir()
	_, err := NewHTTP(server.Client()).Download(context.Background(), task, root, nil)
	var integrity *IntegrityError
	if !errors.As(err, &integrity) {
		t.Fatalf("expected integrity error, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging", "goes", "3", "file.nc.downloaded")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("partial file unexpectedly ready: %v", err)
	}
}
