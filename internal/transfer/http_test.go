package transfer

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestRoundedCMRGranuleSizeUsesOriginLength(t *testing.T) {
	const body = "downloaded MODIS cloud granule"
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"modis-object"`)
		_, _ = w.Write([]byte(body))
	}))
	defer server.Close()

	for _, source := range []string{"modis", "viirs"} {
		t.Run(source, func(t *testing.T) {
			root := t.TempDir()
			task := model.Task{ID: 1, TaskSpec: model.TaskSpec{
				SourceID: source, Name: "cloud.hdf", URL: server.URL,
				RelativeDir: "Aqua/MODIS/L2/2023/01/01", Size: int64(len(body) + 1024),
			}}
			result, err := NewHTTP(server.Client()).Download(context.Background(), task, root, nil)
			if err != nil || result.Size != int64(len(body)) {
				t.Fatalf("transfer result=%+v err=%v", result, err)
			}
			task.StagingPath = result.Path
			promoted, err := Promote(task, filepath.Join(root, "archive"))
			if err != nil || promoted.Size != int64(len(body)) {
				t.Fatalf("archive result=%+v err=%v", promoted, err)
			}
		})
	}
}

func TestExactCatalogSizeStillEnforcedForOtherProviders(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("content"))
	}))
	defer server.Close()
	task := model.Task{ID: 2, TaskSpec: model.TaskSpec{
		SourceID: "goes", Name: "file.nc", URL: server.URL, Size: 900,
	}}
	_, err := NewHTTP(server.Client()).Download(context.Background(), task, t.TempDir(), nil)
	var integrity *IntegrityError
	if !errors.As(err, &integrity) || integrity.Detail != "origin Content-Length does not match catalog" {
		t.Fatalf("expected strict size validation, got %v", err)
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

func TestDownloadIdleTimeoutTracksProgressNotTotalDuration(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"slow"`)
		w.Header().Set("Content-Length", "6")
		for _, b := range []byte("123456") {
			_, _ = w.Write([]byte{b})
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(10 * time.Millisecond)
		}
	}))
	defer server.Close()

	task := model.Task{ID: 43, TaskSpec: model.TaskSpec{SourceID: "goes", Name: "slow.nc", URL: server.URL, Size: 6}}
	result, err := NewHTTP(server.Client(), 25*time.Millisecond).Download(context.Background(), task, t.TempDir(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if result.Size != 6 {
		t.Fatalf("downloaded size=%d want 6", result.Size)
	}
}

func TestDownloadIdleTimeoutStopsStalledBody(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"stalled"`)
		w.Header().Set("Content-Length", "2")
		_, _ = w.Write([]byte("1"))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(80 * time.Millisecond)
		_, _ = w.Write([]byte("2"))
	}))
	defer server.Close()

	task := model.Task{ID: 44, TaskSpec: model.TaskSpec{SourceID: "goes", Name: "stalled.nc", URL: server.URL, Size: 2}}
	_, err := NewHTTP(server.Client(), 25*time.Millisecond).Download(context.Background(), task, t.TempDir(), nil)
	var idle *IdleTimeoutError
	if !errors.As(err, &idle) {
		t.Fatalf("error=%v want IdleTimeoutError", err)
	}
}

func TestDownloadLowThroughputRecyclesAndResumesPart(t *testing.T) {
	const total = 256 << 10
	payload := make([]byte, total)
	for i := range payload {
		payload[i] = byte(i)
	}
	requests := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("ETag", `"slow-flow"`)
		if requests == 1 {
			w.Header().Set("Content-Length", fmt.Sprint(total))
			for offset := 0; offset < total; offset += 512 {
				end := min(offset+512, total)
				if _, err := w.Write(payload[offset:end]); err != nil {
					return
				}
				if f, ok := w.(http.Flusher); ok {
					f.Flush()
				}
				select {
				case <-r.Context().Done():
					return
				case <-time.After(20 * time.Millisecond):
				}
			}
			return
		}

		var start int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-", &start); err != nil || start <= 0 || start >= total {
			t.Errorf("invalid resume range %q", r.Header.Get("Range"))
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if r.Header.Get("If-Range") != `"slow-flow"` {
			t.Errorf("unexpected If-Range %q", r.Header.Get("If-Range"))
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, total-1, total))
		w.Header().Set("Content-Length", fmt.Sprint(total-start))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(payload[start:])
	}))
	defer server.Close()

	task := model.Task{ID: 45, TaskSpec: model.TaskSpec{
		SourceID: "goes",
		Name:     "slow-flow.nc",
		URL:      server.URL,
		Size:     total,
	}}
	root := t.TempDir()
	base := NewHTTP(server.Client())
	guarded := base.WithLowThroughput(LowThroughputPolicy{
		StartupGrace: 40 * time.Millisecond,
		Window:       40 * time.Millisecond,
		Sustain:      40 * time.Millisecond,
		Threshold:    func() int64 { return 128 << 10 },
	})
	_, err := guarded.Download(context.Background(), task, root, nil)
	var slow *LowThroughputError
	if !errors.As(err, &slow) {
		t.Fatalf("error=%v want LowThroughputError", err)
	}
	partPath := filepath.Join(root, "staging", "goes", "45", "slow-flow.nc.part")
	info, err := os.Stat(partPath)
	if err != nil || info.Size() <= 0 || info.Size() >= total {
		t.Fatalf("partial download size=%v err=%v", info, err)
	}

	result, err := base.Download(context.Background(), task, root, nil)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(result.Path)
	if err != nil || len(data) != total || requests != 2 {
		t.Fatalf("resumed bytes=%d requests=%d err=%v", len(data), requests, err)
	}
	for i := range data {
		if data[i] != payload[i] {
			t.Fatalf("resumed content differs at byte %d", i)
		}
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
