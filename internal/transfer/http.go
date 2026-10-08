package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/auth"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
)

var contentRangePattern = regexp.MustCompile(`(?i)^bytes\s+(\d+)-(\d+)/(\d+|\*)$`)

type HTTPError struct {
	Status     int
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string { return fmt.Sprintf("download returned HTTP %d", e.Status) }

func parseRetryAfter(value string) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds > 0 {
		return min(time.Duration(seconds)*time.Second, time.Hour)
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return min(delay, time.Hour)
		}
	}
	return 0
}

type IntegrityError struct {
	Detail string
}

func (e *IntegrityError) Error() string { return e.Detail }

type CollisionError struct {
	Path string
}

func (e *CollisionError) Error() string {
	return "archive target already exists with different content: " + e.Path
}

type DownloadResult struct {
	Path string
	Size int64
}

type PromoteResult struct {
	Path              string
	Size              int64
	SHA256            string
	OfficialAlgorithm string
	OfficialValue     string
	OfficialStatus    string
}

type HTTP struct {
	client        *http.Client
	idleTimeout   time.Duration
	lowThroughput LowThroughputPolicy
}

func NewHTTP(timeoutClient *http.Client, idleTimeout ...time.Duration) *HTTP {
	if timeoutClient == nil {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.MaxIdleConns = 256
		transport.MaxIdleConnsPerHost = 32
		transport.IdleConnTimeout = 90 * time.Second
		timeoutClient = &http.Client{Transport: transport}
	}
	var idle time.Duration
	if len(idleTimeout) > 0 {
		idle = idleTimeout[0]
	}
	return &HTTP{client: timeoutClient, idleTimeout: idle}
}

type LowThroughputPolicy struct {
	StartupGrace time.Duration
	Window       time.Duration
	Sustain      time.Duration
	Threshold    func() int64
}

func (p LowThroughputPolicy) enabled() bool {
	return p.StartupGrace > 0 && p.Window > 0 && p.Sustain > 0 && p.Threshold != nil
}

func (h *HTTP) WithLowThroughput(policy LowThroughputPolicy) *HTTP {
	clone := *h
	clone.lowThroughput = policy
	return &clone
}

type IdleTimeoutError struct {
	Duration time.Duration
}

func (e *IdleTimeoutError) Error() string {
	return fmt.Sprintf("download made no progress for %s", e.Duration)
}

func (e *IdleTimeoutError) Timeout() bool   { return true }
func (e *IdleTimeoutError) Temporary() bool { return true }

type LowThroughputError struct {
	BytesPerSecond int64
	Threshold      int64
	Window         time.Duration
	Sustained      time.Duration
}

func (e *LowThroughputError) Error() string {
	return fmt.Sprintf(
		"download remained below throughput threshold for %s: %d B/s over %s (threshold %d B/s)",
		e.Sustained, e.BytesPerSecond, e.Window, e.Threshold,
	)
}

func (e *LowThroughputError) Temporary() bool { return true }

type idleWatchdog struct {
	pulse chan struct{}
	stop  chan struct{}
	done  chan struct{}
}

func newIdleWatchdog(timeout time.Duration, cancel context.CancelCauseFunc) *idleWatchdog {
	if timeout <= 0 {
		return nil
	}
	w := &idleWatchdog{
		pulse: make(chan struct{}, 1),
		stop:  make(chan struct{}),
		done:  make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		for {
			select {
			case <-w.pulse:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(timeout)
			case <-timer.C:
				cancel(&IdleTimeoutError{Duration: timeout})
				return
			case <-w.stop:
				return
			}
		}
	}()
	return w
}

func (w *idleWatchdog) Pulse() {
	if w == nil {
		return
	}
	select {
	case w.pulse <- struct{}{}:
	default:
	}
}

func (w *idleWatchdog) Stop() {
	if w == nil {
		return
	}
	close(w.stop)
	<-w.done
}

type throughputSample struct {
	at    time.Time
	bytes int64
}

type throughputWatchdog struct {
	bytes atomic.Int64
	stop  chan struct{}
	done  chan struct{}
}

func newThroughputWatchdog(policy LowThroughputPolicy, cancel context.CancelCauseFunc) *throughputWatchdog {
	if !policy.enabled() {
		return nil
	}
	w := &throughputWatchdog{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		started := time.Now()
		samples := []throughputSample{{at: started}}
		var belowSince time.Time
		interval := policy.Window / 6
		if interval > 5*time.Second {
			interval = 5 * time.Second
		}
		if interval < 10*time.Millisecond {
			interval = 10 * time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case now := <-ticker.C:
				total := w.bytes.Load()
				samples = append(samples, throughputSample{at: now, bytes: total})
				cutoff := now.Add(-policy.Window)
				for len(samples) > 2 && samples[1].at.Before(cutoff) {
					samples = samples[1:]
				}
				if now.Sub(started) < policy.StartupGrace || now.Sub(started) < policy.Window {
					continue
				}
				oldest := samples[0]
				elapsed := now.Sub(oldest.at)
				if elapsed <= 0 {
					continue
				}
				rate := int64(float64(total-oldest.bytes) / elapsed.Seconds())
				threshold := policy.Threshold()
				if threshold <= 0 || rate >= threshold {
					belowSince = time.Time{}
					continue
				}
				if belowSince.IsZero() {
					belowSince = now
					continue
				}
				if now.Sub(belowSince) >= policy.Sustain {
					cancel(&LowThroughputError{
						BytesPerSecond: rate,
						Threshold:      threshold,
						Window:         elapsed,
						Sustained:      now.Sub(belowSince),
					})
					return
				}
			case <-w.stop:
				return
			}
		}
	}()
	return w
}

func (w *throughputWatchdog) Add(n int64) {
	if w != nil && n > 0 {
		w.bytes.Add(n)
	}
}

func (w *throughputWatchdog) Stop() {
	if w == nil {
		return
	}
	close(w.stop)
	<-w.done
}

type partIdentity struct {
	URLHash string `json:"url_hash"`
	ETag    string `json:"etag"`
	Size    int64  `json:"size"`
}

// A completed staging file is reusable only if its content still matches the
// checksum recorded after an HTTP transfer. Catalogue byte counts, regardless
// of provider, cannot establish that content is correct.
type readyIdentity struct {
	URLHash string `json:"url_hash"`
	ETag    string `json:"etag"`
	SHA256  string `json:"sha256"`
}

func strongETag(value string) bool {
	return strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) && len(value) >= 2
}

func urlHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func (h *HTTP) Download(ctx context.Context, task model.Task, downloadRoot string, authorizer auth.Authorizer, onProgress ...func(int64)) (DownloadResult, error) {
	requestCtx := ctx
	var cancel context.CancelCauseFunc
	var watchdog *idleWatchdog
	if h.idleTimeout > 0 || h.lowThroughput.enabled() {
		requestCtx, cancel = context.WithCancelCause(ctx)
		if h.idleTimeout > 0 {
			watchdog = newIdleWatchdog(h.idleTimeout, cancel)
		}
		defer func() {
			watchdog.Stop()
			cancel(nil)
		}()
	}
	name, err := safeName(task.Name)
	if err != nil {
		return DownloadResult{}, err
	}
	if _, err := httpsURL(task.URL); err != nil {
		return DownloadResult{}, err
	}
	dir := filepath.Join(downloadRoot, "staging", task.SourceID, strconv.FormatInt(task.ID, 10))
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return DownloadResult{}, err
	}
	partPath := filepath.Join(dir, name+".part")
	metaPath := partPath + ".json"
	readyPath := filepath.Join(dir, name+".downloaded")
	readyMetaPath := readyPath + ".json"

	if info, err := os.Stat(readyPath); err == nil {
		data, metaErr := os.ReadFile(readyMetaPath)
		var receipt readyIdentity
		if metaErr == nil && json.Unmarshal(data, &receipt) == nil && receipt.URLHash == urlHash(task.URL) &&
			(!strongETag(task.ETag) || receipt.ETag == task.ETag) && len(receipt.SHA256) == 64 {
			if digest, hashErr := fileSHA256(readyPath); hashErr == nil && digest == receipt.SHA256 {
				return DownloadResult{Path: readyPath, Size: info.Size()}, nil
			}
		}
		if err := os.Remove(readyPath); err != nil {
			return DownloadResult{}, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return DownloadResult{}, err
	}
	_ = os.Remove(readyMetaPath)

	resumeFrom := int64(0)
	if info, err := os.Stat(partPath); err == nil {
		resumeFrom = info.Size()
	} else if !errors.Is(err, os.ErrNotExist) {
		return DownloadResult{}, err
	}
	var identity partIdentity
	if resumeFrom > 0 {
		data, err := os.ReadFile(metaPath)
		if err != nil || json.Unmarshal(data, &identity) != nil || !strongETag(identity.ETag) ||
			identity.URLHash != urlHash(task.URL) ||
			(strongETag(task.ETag) && identity.ETag != task.ETag) {
			if err := discardPart(partPath, metaPath); err != nil {
				return DownloadResult{}, err
			}
			resumeFrom = 0
		}
	}

	resp, err := h.request(requestCtx, task.URL, resumeFrom, identity.ETag, authorizer)
	if err != nil {
		if cause := context.Cause(requestCtx); cause != nil && cause != context.Canceled && cause != context.DeadlineExceeded {
			return DownloadResult{}, cause
		}
		return DownloadResult{}, err
	}
	watchdog.Pulse()
	if resumeFrom > 0 && resp.StatusCode == http.StatusRequestedRangeNotSatisfiable {
		_ = resp.Body.Close()
		if err := discardPart(partPath, metaPath); err != nil {
			return DownloadResult{}, err
		}
		resumeFrom = 0
		resp, err = h.request(requestCtx, task.URL, 0, "", authorizer)
		if err != nil {
			if cause := context.Cause(requestCtx); cause != nil && cause != context.Canceled && cause != context.DeadlineExceeded {
				return DownloadResult{}, cause
			}
			return DownloadResult{}, err
		}
		watchdog.Pulse()
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return DownloadResult{}, &HTTPError{Status: resp.StatusCode, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
	}
	if encoding := resp.Header.Get("Content-Encoding"); encoding != "" && !strings.EqualFold(encoding, "identity") {
		return DownloadResult{}, &IntegrityError{Detail: "origin returned encoded content for a byte-range download"}
	}
	if strongETag(task.ETag) && resp.Header.Get("ETag") != "" && resp.Header.Get("ETag") != task.ETag {
		return DownloadResult{}, &IntegrityError{Detail: "origin ETag does not match catalog"}
	}

	// These counts are HTTP range/framing coordinates, never product-integrity
	// assertions against a catalogue's approximate size metadata.
	remoteSize := identity.Size
	appendMode := resumeFrom > 0 && resp.StatusCode == http.StatusPartialContent
	var rangeEnd int64
	if resp.StatusCode == http.StatusPartialContent {
		start, end, total, ok := parseContentRange(resp.Header.Get("Content-Range"))
		if !ok || start != resumeFrom || (resp.ContentLength >= 0 && resp.ContentLength != end-start+1) {
			return DownloadResult{}, &IntegrityError{Detail: "origin returned a mismatched Content-Range"}
		}
		if appendMode && resp.Header.Get("ETag") != identity.ETag {
			if err := discardPart(partPath, metaPath); err != nil {
				return DownloadResult{}, err
			}
			return DownloadResult{}, &IntegrityError{Detail: "origin changed ETag while resuming"}
		}
		rangeEnd = end
		if total == 0 && remoteSize == 0 {
			return DownloadResult{}, &IntegrityError{Detail: "origin did not specify complete object length"}
		}
		if total > 0 {
			if remoteSize > 0 && total != remoteSize {
				return DownloadResult{}, &IntegrityError{Detail: fmt.Sprintf("remote object changed size from %d to %d", remoteSize, total)}
			}
			remoteSize = total
		}
	} else if resp.ContentLength >= 0 {
		remoteSize = resp.ContentLength
	}
	if !appendMode {
		if err := discardPart(partPath, metaPath); err != nil {
			return DownloadResult{}, err
		}
		identity = partIdentity{URLHash: urlHash(task.URL), ETag: resp.Header.Get("ETag"), Size: remoteSize}
		data, err := json.Marshal(identity)
		if err != nil {
			return DownloadResult{}, err
		}
		if err := os.WriteFile(metaPath+".tmp", data, 0o600); err != nil {
			return DownloadResult{}, err
		}
		if err := os.Rename(metaPath+".tmp", metaPath); err != nil {
			return DownloadResult{}, err
		}
	}

	flags := os.O_CREATE | os.O_WRONLY
	if appendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	handle, err := os.OpenFile(partPath, flags, 0o640)
	if err != nil {
		return DownloadResult{}, err
	}
	throughputWatchdog := newThroughputWatchdog(h.lowThroughput, cancel)
	defer throughputWatchdog.Stop()
	var destination io.Writer = handle
	if watchdog != nil || throughputWatchdog != nil || len(onProgress) > 0 && onProgress[0] != nil {
		destination = progressWriter{Writer: handle, report: func(n int64) {
			watchdog.Pulse()
			throughputWatchdog.Add(n)
			if len(onProgress) > 0 && onProgress[0] != nil {
				onProgress[0](n)
			}
		}}
	}
	_, copyErr := io.CopyBuffer(destination, resp.Body, make([]byte, 1<<20))
	syncErr := handle.Sync()
	closeErr := handle.Close()
	if copyErr != nil {
		if cause := context.Cause(requestCtx); cause != nil && cause != context.Canceled && cause != context.DeadlineExceeded {
			return DownloadResult{}, fmt.Errorf("download body: %w", cause)
		}
		return DownloadResult{}, fmt.Errorf("download body: %w", copyErr)
	}
	if syncErr != nil {
		return DownloadResult{}, syncErr
	}
	if closeErr != nil {
		return DownloadResult{}, closeErr
	}
	info, err := os.Stat(partPath)
	if err != nil {
		return DownloadResult{}, err
	}
	if remoteSize > 0 && info.Size() != remoteSize {
		return DownloadResult{}, &IntegrityError{Detail: fmt.Sprintf("downloaded size %d does not match expected size %d", info.Size(), remoteSize)}
	}
	if resp.StatusCode == http.StatusPartialContent && info.Size() != rangeEnd+1 {
		return DownloadResult{}, &IntegrityError{Detail: "partial response did not cover the complete requested range"}
	}
	digest, err := fileSHA256(partPath)
	if err != nil {
		return DownloadResult{}, err
	}
	receipt, err := json.Marshal(readyIdentity{URLHash: urlHash(task.URL), ETag: resp.Header.Get("ETag"), SHA256: digest})
	if err != nil {
		return DownloadResult{}, err
	}
	if err := os.WriteFile(readyMetaPath+".tmp", receipt, 0o600); err != nil {
		return DownloadResult{}, err
	}
	if err := os.Rename(readyMetaPath+".tmp", readyMetaPath); err != nil {
		return DownloadResult{}, err
	}
	if err := os.Rename(partPath, readyPath); err != nil {
		return DownloadResult{}, err
	}
	_ = os.Remove(metaPath)
	_ = syncDir(dir)
	return DownloadResult{Path: readyPath, Size: info.Size()}, nil
}

type progressWriter struct {
	io.Writer
	report func(int64)
}

func (w progressWriter) Write(data []byte) (int, error) {
	n, err := w.Writer.Write(data)
	w.report(int64(n))
	return n, err
}

func discardPart(partPath, metaPath string) error {
	if err := os.Remove(partPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if err := os.Remove(metaPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (h *HTTP) request(ctx context.Context, locator string, resumeFrom int64, etag string, authorizer auth.Authorizer) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, locator, nil)
	if err != nil {
		return nil, err
	}
	if resumeFrom > 0 {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeFrom))
		req.Header.Set("If-Range", etag)
	}
	req.Header.Set("Accept-Encoding", "identity")
	if authorizer != nil {
		if err := authorizer.Authorize(ctx, req); err != nil {
			return nil, err
		}
	}
	return h.client.Do(req)
}

func Promote(task model.Task, archiveRoot string, groupWritable ...bool) (PromoteResult, error) {
	return PromoteWithOfficial(task, archiveRoot, nil, groupWritable...)
}

// PromoteWithOfficial verifies authoritative provider digests *before* the
// first archive write; local SHA-256 remains mandatory for every source.
func PromoteWithOfficial(task model.Task, archiveRoot string, official []OfficialChecksum, groupWritable ...bool) (PromoteResult, error) {
	name, err := safeName(task.Name)
	if err != nil {
		return PromoteResult{}, err
	}
	relative, err := safeRelativeDir(task.RelativeDir)
	if err != nil {
		return PromoteResult{}, err
	}
	if strings.TrimSpace(task.StagingPath) == "" {
		return PromoteResult{}, &IntegrityError{Detail: "staging path is empty"}
	}
	info, err := os.Stat(task.StagingPath)
	if err != nil {
		return PromoteResult{}, err
	}
	digest, err := sha256AndVerifyOfficial(task.StagingPath, official)
	if err != nil {
		return PromoteResult{}, err
	}
	verification := PromoteResult{OfficialStatus: "local_only"}
	if len(official) > 0 {
		verification.OfficialStatus = "verified"
		for i, c := range official {
			if i > 0 {
				verification.OfficialAlgorithm += ","
				verification.OfficialValue += ","
			}
			verification.OfficialAlgorithm += c.Algorithm
			verification.OfficialValue += c.Value
		}
	}

	targetDir := filepath.Join(archiveRoot, relative)
	shared := len(groupWritable) > 0 && groupWritable[0]
	if err := ensureArchiveDirectory(archiveRoot, relative, shared); err != nil {
		return PromoteResult{}, err
	}
	target := filepath.Join(targetDir, name)
	if existing, err := os.Lstat(target); err == nil {
		if !existing.Mode().IsRegular() {
			return PromoteResult{}, &CollisionError{Path: target}
		}
		existingDigest, digestErr := fileSHA256(target)
		if digestErr != nil {
			return PromoteResult{}, digestErr
		}
		if existingDigest != digest {
			return PromoteResult{}, &CollisionError{Path: target}
		}
		_ = os.Remove(task.StagingPath)
		verification.Path, verification.Size, verification.SHA256 = target, existing.Size(), digest
		return verification, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return PromoteResult{}, err
	}

	temp := filepath.Join(targetDir, fmt.Sprintf(".%s.task-%d.tmp", name, task.ID))
	_ = os.Remove(temp)
	if err := copyFile(task.StagingPath, temp, shared); err != nil {
		_ = os.Remove(temp)
		return PromoteResult{}, err
	}
	tempDigest, err := fileSHA256(temp)
	if err != nil {
		_ = os.Remove(temp)
		return PromoteResult{}, err
	}
	if tempDigest != digest {
		_ = os.Remove(temp)
		return PromoteResult{}, &IntegrityError{Detail: "archive copy SHA-256 mismatch"}
	}
	if err := os.Link(temp, target); err != nil {
		_ = os.Remove(temp)
		if errors.Is(err, os.ErrExist) {
			return PromoteResult{}, &CollisionError{Path: target}
		}
		return PromoteResult{}, err
	}
	if err := os.Remove(temp); err != nil {
		return PromoteResult{}, err
	}
	if err := syncDir(targetDir); err != nil {
		return PromoteResult{}, err
	}
	if err := os.Remove(task.StagingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return PromoteResult{}, err
	}
	_ = syncDir(filepath.Dir(task.StagingPath))
	verification.Path, verification.Size, verification.SHA256 = target, info.Size(), digest
	return verification, nil
}

func ensureArchiveDirectory(root, relative string, groupWritable bool) error {
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		return errors.New("archive root must be a real directory")
	}
	current := root
	for _, component := range strings.Split(relative, string(filepath.Separator)) {
		current = filepath.Join(current, component)
		mode := os.FileMode(0o750)
		if groupWritable {
			// Allow the parent ACL to propagate group write and public read.
			mode = os.ModeSetgid | 0o777
		}
		created := false
		if err := os.Mkdir(current, mode); err == nil {
			created = true
		} else if !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return &CollisionError{Path: current}
		}
		if created && groupWritable && (info.Mode().Perm() != 0o775 || info.Mode()&os.ModeSetgid == 0) {
			if err := os.Chmod(current, os.ModeSetgid|0o775); err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(source, destination string, shared bool) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	mode := os.FileMode(0o640)
	if shared {
		mode = 0o666
	}
	out, err := os.OpenFile(destination, os.O_CREATE|os.O_WRONLY|os.O_TRUNC|os.O_EXCL, mode)
	if err != nil {
		return err
	}
	if shared {
		info, err := out.Stat()
		if err == nil && info.Mode().Perm() != 0o664 {
			err = out.Chmod(0o664)
		}
		if err != nil {
			_ = out.Close()
			return err
		}
	}
	_, copyErr := io.CopyBuffer(out, in, make([]byte, 1<<20))
	syncErr := out.Sync()
	closeErr := out.Close()
	if copyErr != nil {
		return copyErr
	}
	if syncErr != nil {
		return syncErr
	}
	return closeErr
}

func fileSHA256(path string) (string, error) {
	handle, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer handle.Close()
	hash := sha256.New()
	if _, err := io.CopyBuffer(hash, handle, make([]byte, 1<<20)); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func safeName(name string) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" || name == "." || name == ".." || filepath.Base(name) != name || strings.ContainsAny(name, `/\`) {
		return "", errors.New("asset name must be a plain filename")
	}
	return name, nil
}

func safeRelativeDir(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", errors.New("relative archive directory is required")
	}
	if filepath.IsAbs(value) {
		return "", errors.New("relative archive directory must not be absolute")
	}
	clean := filepath.Clean(value)
	if clean == "." || clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("relative archive directory escapes archive root")
	}
	return clean, nil
}

func httpsURL(value string) (*url.URL, error) {
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, err
	}
	if parsed.Scheme != "https" || parsed.Hostname() == "" {
		return nil, errors.New("asset URL must use HTTPS")
	}
	return parsed, nil
}

func parseContentRange(value string) (start, end, total int64, ok bool) {
	match := contentRangePattern.FindStringSubmatch(strings.TrimSpace(value))
	if match == nil {
		return 0, 0, 0, false
	}
	start, err1 := strconv.ParseInt(match[1], 10, 64)
	end, err2 := strconv.ParseInt(match[2], 10, 64)
	if err1 != nil || err2 != nil || end < start {
		return 0, 0, 0, false
	}
	if match[3] != "*" {
		total, err1 = strconv.ParseInt(match[3], 10, 64)
		if err1 != nil || total <= end {
			return 0, 0, 0, false
		}
	}
	return start, end, total, true
}

func syncDir(path string) error {
	handle, err := os.Open(path)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
