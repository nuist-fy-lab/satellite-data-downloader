package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/access"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/archiveevent"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/config"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/model"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/store"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/transfer"
)

type Runtime struct {
	cfg      config.Config
	store    *store.Store
	access   *access.Manager
	provider map[string]config.Provider
	flows    map[string]*providerFlow
	events   *archiveevent.Notifier
	official *transfer.OfficialResolver
	wg       sync.WaitGroup
}

func (r *Runtime) ArchiveRootFor(source string) (string, error) {
	p, ok := r.provider[source]
	if !ok || !p.Enabled {
		return "", fmt.Errorf("archive provider %s unavailable", source)
	}
	if p.ArchiveRoot != "" {
		return p.ArchiveRoot, nil
	}
	return r.cfg.ArchiveRoot, nil
}

type providerFlow struct {
	sync.Mutex
	limit, active, successes int
	bytes, lastBytes         int64
	lastSample               time.Time
	rate                     float64
	gap                      time.Duration
	nextStart                time.Time
	errorRate                float64
	normalTransferRates      []float64
	probeAt                  time.Time
	probeBytes               int64
	probeBlockedUntil        time.Time
}

const (
	// MODIS granules can take minutes under a slow upstream route. Scaling
	// solely after eight *completed files* deadlocks concurrency at four.
	progressProbeInterval = 45 * time.Second
	progressProbeMinBytes = 512 << 10
)

func (f *providerFlow) start() bool {
	f.Lock()
	defer f.Unlock()
	if f.active >= f.limit {
		return false
	}
	f.active++
	return true
}

func (f *providerFlow) pace(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		f.Lock()
		wait := time.Until(f.nextStart)
		if wait <= 0 {
			f.nextStart = time.Now().Add(f.gap)
			f.Unlock()
			return nil
		}
		f.Unlock()
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (f *providerFlow) cooldown(delay time.Duration) {
	if delay <= 0 {
		return
	}
	f.Lock()
	defer f.Unlock()
	if until := time.Now().Add(min(delay, time.Hour)); until.After(f.nextStart) {
		f.nextStart = until
	}
}

func (f *providerFlow) finish(success, congested bool, ceiling int) {
	f.Lock()
	defer f.Unlock()
	f.active--
	if congested {
		f.errorRate = f.errorRate*0.8 + 0.2
		if f.errorRate >= 0.2 {
			f.limit = max(1, f.limit/2)
			f.gap = min(5*time.Second, max(100*time.Millisecond, f.gap*2))
			if next := time.Now().Add(f.gap); next.After(f.nextStart) {
				f.nextStart = next
			}
		}
		f.successes = 0
		// True provider congestion (429/5xx or timeout), unlike a slow but
		// progressing stream, prevents immediate upward exploration.
		f.probeBlockedUntil = time.Now().Add(3 * progressProbeInterval)
		f.probeAt, f.probeBytes = time.Now(), f.bytes
	} else if success {
		f.errorRate *= 0.8
		f.successes++
		if f.successes >= 8 && f.errorRate < 0.1 {
			f.limit = min(ceiling, f.limit+1)
			f.gap = max(100*time.Millisecond, f.gap*4/5)
			f.successes = 0
		}
	}
}

func (f *providerFlow) addBytes(n int64) {
	f.Lock()
	f.bytes += n
	f.Unlock()
}

// probeProgress performs additive concurrency exploration without waiting for
// whole-file completion. It observes *aggregate useful bytes*, is capped by
// configured concurrency and backs off after real upstream congestion.
// No active transfer is ever cancelled to adjust the limit.
func (f *providerFlow) probeProgress(now time.Time, ceiling int) (oldLimit, newLimit int, rate float64) {
	f.Lock()
	defer f.Unlock()
	if f.probeAt.IsZero() {
		f.probeAt, f.probeBytes = now, f.bytes
		return f.limit, f.limit, 0
	}
	elapsed := now.Sub(f.probeAt)
	if elapsed < progressProbeInterval {
		return f.limit, f.limit, 0
	}
	progress := max(int64(0), f.bytes-f.probeBytes)
	f.probeBytes, f.probeAt = f.bytes, now
	oldLimit, newLimit = f.limit, f.limit
	if elapsed > 0 {
		rate = float64(progress) / elapsed.Seconds()
	}
	if now.Before(f.probeBlockedUntil) || f.errorRate >= 0.2 ||
		f.limit >= ceiling || f.active < f.limit || progress < progressProbeMinBytes {
		return oldLimit, newLimit, rate
	}
	f.limit++
	f.gap = max(100*time.Millisecond, f.gap*4/5)
	f.successes = 0
	return oldLimit, f.limit, rate
}

func (f *providerFlow) observeTransferRate(bytes int64, elapsed time.Duration, floor int64) {
	if bytes <= 0 || elapsed < 5*time.Second {
		return
	}
	rate := float64(bytes) / elapsed.Seconds()
	if rate < float64(floor) {
		return
	}
	f.Lock()
	defer f.Unlock()
	f.normalTransferRates = append(f.normalTransferRates, rate)
	if len(f.normalTransferRates) > 16 {
		f.normalTransferRates = append([]float64(nil), f.normalTransferRates[len(f.normalTransferRates)-16:]...)
	}
}

func (f *providerFlow) lowThroughputThreshold(policy config.LowThroughputPolicy) int64 {
	threshold := policy.FloorBytesPerSecond
	f.Lock()
	rates := append([]float64(nil), f.normalTransferRates...)
	f.Unlock()
	if len(rates) == 0 {
		return threshold
	}
	sort.Float64s(rates)
	var baseline float64
	middle := len(rates) / 2
	if len(rates)%2 == 0 {
		baseline = (rates[middle-1] + rates[middle]) / 2
	} else {
		baseline = rates[middle]
	}
	adaptive := int64(baseline * policy.BaselineFraction)
	if adaptive > threshold {
		threshold = adaptive
	}
	return threshold
}

func (f *providerFlow) snapshot() (limit, active int, rate float64) {
	f.Lock()
	defer f.Unlock()
	now := time.Now()
	if !f.lastSample.IsZero() && now.Sub(f.lastSample) >= time.Second {
		f.rate = float64(f.bytes-f.lastBytes) / now.Sub(f.lastSample).Seconds()
		f.lastSample, f.lastBytes = now, f.bytes
	}
	return f.limit, f.active, f.rate
}

func New(cfg config.Config, state *store.Store, accessManager *access.Manager) (*Runtime, error) {
	if state == nil || accessManager == nil {
		return nil, errors.New("pipeline requires store and access manager")
	}
	providers := make(map[string]config.Provider, len(cfg.Providers))
	flows := make(map[string]*providerFlow, len(cfg.Providers))
	for _, item := range cfg.Providers {
		if strings.TrimSpace(item.SourceID) == "" {
			return nil, errors.New("provider source id is empty")
		}
		if _, exists := providers[item.SourceID]; exists {
			return nil, fmt.Errorf("duplicate provider %q", item.SourceID)
		}
		providers[item.SourceID] = item
		flows[item.SourceID] = &providerFlow{limit: max(1, min(item.InitialTransferConcurrency, item.TransferConcurrency)),
			gap: 200 * time.Millisecond, lastSample: time.Now(), probeAt: time.Now()}
	}
	return &Runtime{
		cfg: cfg, store: state, access: accessManager, provider: providers, flows: flows,
		events:   archiveevent.New(cfg.ArchiveEventSocket, state),
		official: transfer.NewOfficialResolver(),
	}, nil
}

func (r *Runtime) Run(ctx context.Context) {
	go r.events.Run(ctx)
	for _, item := range r.cfg.Providers {
		if !item.Enabled {
			continue
		}
		item := item
		if item.SourceID == "modis" {
			go func() {
				ticker := time.NewTicker(5 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case now := <-ticker.C:
						before, after, rate := r.flows[item.SourceID].probeProgress(now, item.TransferConcurrency)
						if after > before {
							slog.Info("adaptive MODIS concurrency grows on useful in-flight progress",
								"from", before, "to", after, "ceiling", item.TransferConcurrency,
								"aggregate_bytes_per_second", rate)
						}
					}
				}
			}()
		}
		for worker := 0; worker < item.TransferConcurrency; worker++ {
			r.wg.Add(1)
			go func(worker int) {
				defer r.wg.Done()
				r.transferLoop(ctx, item, worker)
			}(worker)
		}
		for worker := 0; worker < item.VerifyConcurrency; worker++ {
			r.wg.Add(1)
			go func(worker int) {
				defer r.wg.Done()
				r.verifyLoop(ctx, item, worker)
			}(worker)
		}
	}
	go func() {
		<-ctx.Done()
		r.wg.Wait()
	}()
}

func (r *Runtime) Provider(sourceID string) (config.Provider, bool) {
	item, ok := r.provider[sourceID]
	return item, ok
}

func (r *Runtime) ProviderStatuses(ctx context.Context) ([]model.ProviderStatus, error) {
	out := make([]model.ProviderStatus, 0, len(r.cfg.Providers))
	for _, item := range r.cfg.Providers {
		counts, err := r.store.StateCounts(ctx, item.SourceID)
		if err != nil {
			return nil, err
		}
		limit, _, rate := r.flows[item.SourceID].snapshot()
		active := providerActiveTransfers(counts)
		out = append(out, model.ProviderStatus{
			SourceID: item.SourceID, Enabled: item.Enabled,
			TransferConcurrency: item.TransferConcurrency, VerifyConcurrency: item.VerifyConcurrency,
			AdaptiveConcurrency: limit, ActiveTransfers: active, BytesPerSecond: rate,
			AuthRef: item.AuthRef, AuthConfigured: r.access.Configured(ctx, item.SourceID, item.AuthRef), States: counts,
		})
	}
	return out, nil
}

func providerActiveTransfers(counts map[string]int) int {
	return max(0, counts["downloading"])
}

func (r *Runtime) ValidateTask(spec model.TaskSpec) error {
	provider, ok := r.provider[spec.SourceID]
	if !ok {
		return fmt.Errorf("unknown provider %q", spec.SourceID)
	}
	if !provider.Enabled {
		return fmt.Errorf("provider %q is disabled", spec.SourceID)
	}
	if strings.TrimSpace(spec.AuthRef) != strings.TrimSpace(provider.AuthRef) {
		return fmt.Errorf("provider %q requires auth_ref %q", spec.SourceID, provider.AuthRef)
	}
	parsed, err := url.Parse(spec.URL)
	if err != nil || parsed.Scheme != "https" {
		return errors.New("asset URL must use HTTPS")
	}
	if !allowedHost(spec.SourceID, parsed.Hostname()) {
		return fmt.Errorf("asset host %q is not allowed for provider %q", parsed.Hostname(), spec.SourceID)
	}
	if strings.TrimSpace(spec.Name) == "" || strings.TrimSpace(spec.CollectionID) == "" || strings.TrimSpace(spec.GranuleID) == "" {
		return errors.New("name, collection_id and granule_id are required")
	}
	if spec.Size < 0 {
		return errors.New("asset size must not be negative")
	}
	return nil
}

func (r *Runtime) transferLoop(ctx context.Context, provider config.Provider, worker int) {
	poll := time.NewTimer(0)
	defer poll.Stop()
	flow := r.flows[provider.SourceID]
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		}
		// A task can enter waiting_auth whenever the centre reports no usable
		// route. Anonymous providers such as GOES have an empty AuthRef, but
		// still depend on the centre for direct/proxy routing. Release their
		// waiting tasks as soon as the centre reports a usable route again.
		if worker == 0 && r.access.Configured(ctx, provider.SourceID, provider.AuthRef) {
			if err := r.store.ReleaseWaitingAuth(ctx, provider.SourceID); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("release provider auth wait", "provider", provider.SourceID, "worker", worker, "error", err)
			}
		}
		if !flow.start() {
			poll.Reset(150 * time.Millisecond)
			continue
		}
		task, ok, err := r.store.ClaimTransfer(ctx, provider.SourceID)
		if err != nil {
			flow.finish(false, false, provider.TransferConcurrency)
			slog.Warn("claim provider transfer", "provider", provider.SourceID, "worker", worker, "error", err)
			poll.Reset(500 * time.Millisecond)
			continue
		}
		if !ok {
			flow.finish(false, false, provider.TransferConcurrency)
			poll.Reset(150 * time.Millisecond)
			continue
		}
		route, err := r.access.Resolve(ctx, provider.SourceID, provider.AuthRef, provider.RequestTimeout)
		if err != nil {
			flow.finish(false, false, provider.TransferConcurrency)
			if errors.Is(err, access.ErrNoRoute) {
				_ = r.store.WaitForAuth(context.WithoutCancel(ctx), task.ID, fmt.Errorf("%s access route is not configured", provider.SourceID))
			} else {
				r.scheduleTransferRetry(ctx, provider.SourceID, task, err)
			}
			poll.Reset(25 * time.Millisecond)
			continue
		}
		if err := flow.pace(ctx); err != nil {
			flow.finish(false, false, provider.TransferConcurrency)
			r.scheduleTransferRetry(ctx, provider.SourceID, task, err)
			return
		}
		transferClient := route.Transfer
		if provider.LowThroughput.Enabled {
			transferClient = transferClient.WithLowThroughput(transfer.LowThroughputPolicy{
				StartupGrace: provider.LowThroughput.StartupGrace,
				Window:       provider.LowThroughput.Window,
				Sustain:      provider.LowThroughput.Sustain,
				Threshold: func() int64 {
					return flow.lowThroughputThreshold(provider.LowThroughput)
				},
			})
		}
		started := time.Now()
		var transferred int64
		result, err := transferClient.Download(ctx, task, r.cfg.DownloadRoot, route.Authorizer, func(n int64) {
			transferred += n
			flow.addBytes(n)
		})
		if err != nil {
			flow.finish(false, isCongestion(err), provider.TransferConcurrency)
			var lowThroughput *transfer.LowThroughputError
			if errors.As(err, &lowThroughput) {
				slog.Warn("recycle low-throughput transfer",
					"provider", provider.SourceID,
					"profile", provider.LowThroughput.Profile,
					"task", task.ID,
					"bytes_per_second", lowThroughput.BytesPerSecond,
					"threshold_bytes_per_second", lowThroughput.Threshold,
					"window", lowThroughput.Window,
					"sustained", lowThroughput.Sustained,
				)
			}
			var httpErr *transfer.HTTPError
			if errors.As(err, &httpErr) && isCongestion(err) {
				flow.cooldown(httpErr.RetryAfter)
			}
			r.handleTransferError(ctx, provider, task, route, err)
			poll.Reset(25 * time.Millisecond)
			continue
		}
		flow.observeTransferRate(transferred, time.Since(started), provider.LowThroughput.FloorBytesPerSecond)
		flow.finish(true, false, provider.TransferConcurrency)
		r.access.MarkSuccess(context.WithoutCancel(ctx), route)
		if err := r.store.MarkDownloaded(context.WithoutCancel(ctx), task.ID, result.Path, result.Size); err != nil {
			slog.Warn("checkpoint provider transfer", "provider", provider.SourceID, "task", task.ID, "error", err)
		}
		poll.Reset(10 * time.Millisecond)
	}
}

func (r *Runtime) handleTransferError(ctx context.Context, provider config.Provider, task model.Task, route access.Route, cause error) {
	if isCredentialFailure(cause) {
		r.access.MarkAuthFailure(context.WithoutCancel(ctx), route, cause.Error())
		if r.access.HasUsableRoute(ctx, provider.SourceID, provider.AuthRef, provider.RequestTimeout) {
			r.scheduleTransferRetry(ctx, provider.SourceID, task, cause)
		} else {
			_ = r.store.WaitForAuth(context.WithoutCancel(ctx), task.ID, cause)
		}
		return
	}
	var httpErr *transfer.HTTPError
	if errors.As(cause, &httpErr) {
		if provider.AuthRef != "" && (httpErr.Status == http.StatusUnauthorized || httpErr.Status == http.StatusForbidden) {
			r.access.MarkAuthFailure(context.WithoutCancel(ctx), route, cause.Error())
			if r.access.HasUsableRoute(ctx, provider.SourceID, provider.AuthRef, provider.RequestTimeout) {
				r.scheduleTransferRetry(ctx, provider.SourceID, task, cause)
			} else {
				_ = r.store.WaitForAuth(context.WithoutCancel(ctx), task.ID, cause)
			}
			return
		}
	}
	if route.ProxyID != nil {
		r.access.MarkProxyFailure(context.WithoutCancel(ctx), route, cause.Error())
	}
	r.scheduleTransferRetry(ctx, provider.SourceID, task, cause)
}

func (r *Runtime) scheduleTransferRetry(ctx context.Context, source string, task model.Task, cause error) {
	decision := transferRetryDecision(task.Attempts, cause)
	if decision.terminal {
		slog.Warn("stop retrying unavailable or invalid download", "provider", source,
			"task", task.ID, "attempts", task.Attempts, "reason", decision.reason, "error", cause)
		if err := r.store.Fail(context.WithoutCancel(ctx), task.ID, cause); err != nil {
			slog.Error("persist terminal download failure", "provider", source, "task", task.ID, "error", err)
		}
		return
	}
	if err := r.store.RetryTransfer(context.WithoutCancel(ctx), task.ID, cause, decision.delay); err != nil {
		slog.Error("persist scheduled download retry", "provider", source, "task", task.ID, "error", err)
		return
	}
	slog.Info("schedule download retry", "provider", source, "task", task.ID,
		"attempt", task.Attempts, "delay", decision.delay, "error", cause)
}

func isCredentialFailure(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "credential") ||
		strings.Contains(strings.ToLower(err.Error()), "token") && strings.Contains(strings.ToLower(err.Error()), "not configured")
}

func (r *Runtime) verifyLoop(ctx context.Context, provider config.Provider, worker int) {
	poll := time.NewTimer(0)
	defer poll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-poll.C:
		}
		task, ok, err := r.store.ClaimVerify(ctx, provider.SourceID)
		if err != nil {
			slog.Warn("claim provider verification", "provider", provider.SourceID, "worker", worker, "error", err)
			poll.Reset(500 * time.Millisecond)
			continue
		}
		if !ok {
			poll.Reset(150 * time.Millisecond)
			continue
		}
		archiveRoot := provider.ArchiveRoot
		if archiveRoot == "" {
			archiveRoot = r.cfg.ArchiveRoot
		}
		// Metadata requests run only in verification workers, not in the
		// concurrent download hot path. A metadata outage keeps the staging
		// file intact for later verification rather than declaring success.
		var official []transfer.OfficialChecksum
		if r.official != nil {
			official, err = r.official.Resolve(ctx, task)
			if err != nil {
				slog.Warn("official checksum lookup delayed", "provider", provider.SourceID, "task", task.ID, "error", err)
				_ = r.store.RetryVerification(context.WithoutCancel(ctx), task.ID, err, retryDelay(max(task.Attempts, 1)))
				poll.Reset(25 * time.Millisecond)
				continue
			}
		}
		result, err := transfer.PromoteWithOfficial(task, archiveRoot, official, provider.ArchiveRoot != "" || r.cfg.ArchiveGroupWritable)
		if err != nil {
			var collision *transfer.CollisionError
			if errors.As(err, &collision) {
				_ = r.store.RetryVerification(context.WithoutCancel(ctx), task.ID, err, time.Hour)
				poll.Reset(25 * time.Millisecond)
				continue
			}
			var integrity *transfer.IntegrityError
			if errors.As(err, &integrity) || errors.Is(err, os.ErrNotExist) {
				_ = os.Remove(task.StagingPath)
				_ = r.store.Redownload(context.WithoutCancel(ctx), task.ID, err, retryDelay(task.Attempts))
			} else {
				_ = r.store.RetryVerification(context.WithoutCancel(ctx), task.ID, err, retryDelay(task.Attempts))
			}
			poll.Reset(25 * time.Millisecond)
			continue
		}
		if err := r.store.CompleteWithOfficial(context.WithoutCancel(ctx), task.ID, result.Path, result.SHA256, result.Size, result.OfficialStatus, result.OfficialAlgorithm, result.OfficialValue); err != nil {
			slog.Warn("checkpoint provider promotion", "provider", provider.SourceID, "task", task.ID, "error", err)
		} else {
			r.events.Notify()
		}
		poll.Reset(10 * time.Millisecond)
	}
}

func isCongestion(err error) bool {
	var httpErr *transfer.HTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.Status {
		case http.StatusRequestTimeout, http.StatusTooManyRequests,
			http.StatusInternalServerError, http.StatusBadGateway,
			http.StatusServiceUnavailable, http.StatusGatewayTimeout:
			return true
		}
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}

func allowedHost(sourceID, host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	switch sourceID {
	case "goes":
		return host == "storage.googleapis.com"
	case "modis", "viirs":
		return host == "data.laadsdaac.earthdatacloud.nasa.gov" || host == "ladsweb.modaps.eosdis.nasa.gov"
	case "mtg":
		return host == "api.eumetsat.int"
	case "earthcare":
		return host == "catalog.maap.eo.esa.int"
	case "calipso":
		return host == "data.asdc.earthdata.nasa.gov"
	case "cloudsat":
		return host == "dpc-thredds.cira.colostate.edu"
	case "era5":
		return host == "cds.climate.copernicus.eu"
	case "himawari8":
		return host == "noaa-himawari8.s3.amazonaws.com"
	case "himawari9":
		return host == "noaa-himawari9.s3.amazonaws.com"
	default:
		return false
	}
}
