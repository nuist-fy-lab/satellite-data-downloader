package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/nuist-fy-lab/satellite-data-downloader/internal/config"
	"github.com/nuist-fy-lab/satellite-data-downloader/internal/transfer"
)

func TestAllowedHostAcceptsNativeLAADS(t *testing.T) {
	for _, source := range []string{"modis", "viirs"} {
		if !allowedHost(source, "ladsweb.modaps.eosdis.nasa.gov") {
			t.Fatalf("%s should allow native LAADS archive", source)
		}
		if !allowedHost(source, "data.laadsdaac.earthdatacloud.nasa.gov") {
			t.Fatalf("%s should retain Earthdata Cloud compatibility", source)
		}
	}
	if allowedHost("modis", "example.invalid") {
		t.Fatal("unexpected host accepted")
	}
}

func TestERA5TasksUseOnlyTheStableCentreReference(t *testing.T) {
	if !allowedHost("era5", "cds.climate.copernicus.eu") {
		t.Fatal("stable CDS job reference was rejected")
	}
	for _, host := range []string{"object-store.os-api.cci2.ecmwf.int", "127.0.0.1", "example.invalid"} {
		if allowedHost("era5", host) {
			t.Fatalf("downloader accepted direct ERA5 asset host %s", host)
		}
	}
}

func TestProviderFlowAdaptsWithinCeiling(t *testing.T) {
	flow := &providerFlow{limit: 4, gap: 200 * time.Millisecond, lastSample: time.Now().Add(-2 * time.Second)}
	for range 4 {
		if !flow.start() {
			t.Fatal("capacity lost before limit")
		}
	}
	if flow.start() {
		t.Fatal("flow exceeded concurrency limit")
	}
	flow.finish(false, true, 4)
	limit, active, _ := flow.snapshot()
	if limit != 2 || active != 3 || flow.gap != 400*time.Millisecond {
		t.Fatalf("after throttle: limit=%d active=%d", limit, active)
	}
	if time.Until(flow.nextStart) < 300*time.Millisecond {
		t.Fatalf("failure did not slow next network start: %s", time.Until(flow.nextStart))
	}
	for range 3 {
		flow.finish(true, false, 4)
	}
	for range 8 {
		if !flow.start() {
			t.Fatal("expected one active slot")
		}
		flow.finish(true, false, 4)
	}
	limit, _, _ = flow.snapshot()
	if limit != 3 {
		t.Fatalf("expected additive recovery, got %d", limit)
	}
	for range 12 {
		flow.active++
		flow.finish(false, true, 4)
	}
	if flow.limit != 1 || flow.gap != 5*time.Second {
		t.Fatalf("congestion not bounded: limit=%d gap=%s", flow.limit, flow.gap)
	}
	for range 160 {
		flow.active++
		flow.finish(true, false, 4)
	}
	if flow.limit != 4 || flow.gap != 100*time.Millisecond {
		t.Fatalf("success recovery not bounded: limit=%d gap=%s", flow.limit, flow.gap)
	}
}

func TestProviderFlowCanGrowAboveInitialLimit(t *testing.T) {
	flow := &providerFlow{limit: 4, gap: 200 * time.Millisecond}
	for range 32 {
		flow.active++
		flow.finish(true, false, 8)
	}
	if flow.limit != 8 {
		t.Fatalf("adaptive concurrency=%d want ceiling 8", flow.limit)
	}
	for range 20 {
		flow.active++
		flow.finish(false, true, 8)
	}
	if flow.limit != 1 {
		t.Fatalf("adaptive concurrency=%d want floor 1", flow.limit)
	}
}

func TestMODISExploresDuringLongSlowTransfersWithoutCompletedFiles(t *testing.T) {
	now := time.Now()
	flow := &providerFlow{
		limit: 4, active: 4, gap: 200 * time.Millisecond,
		probeAt: now.Add(-progressProbeInterval - time.Second),
		bytes:   2 << 20,
	}
	previous, next, rate := flow.probeProgress(now, 8)
	if previous != 4 || next != 5 || rate <= 0 {
		t.Fatalf("slow but progressing requests should grow 4->5 without a full-file completion: %d %d %.2f", previous, next, rate)
	}
	if _, next, _ := flow.probeProgress(now.Add(10*time.Second), 8); next != 5 {
		t.Fatal("concurrency increased faster than the exploration interval")
	}
	for limit := 6; limit <= 8; limit++ {
		flow.active = flow.limit
		flow.addBytes(2 << 20)
		_, next, _ = flow.probeProgress(now.Add(time.Duration(limit-4)*progressProbeInterval+10*time.Second), 8)
		if next != limit {
			t.Fatalf("progressive exploration stopped at %d, want %d", next, limit)
		}
	}
	flow.active = 8
	flow.addBytes(2 << 20)
	if _, next, _ := flow.probeProgress(now.Add(8*progressProbeInterval), 8); next != 8 {
		t.Fatal("concurrency exceeded configured ceiling")
	}
}

func TestMODISProgressProbeNeedsTrafficAndFullSlots(t *testing.T) {
	now := time.Now()
	for _, tc := range []struct {
		name   string
		active int
		delta  int64
	}{
		{"idle", 0, 2 << 20},
		{"spare_slot", 3, 2 << 20},
		{"no_progress", 4, 0},
		{"insufficient_progress", 4, 200 << 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flow := &providerFlow{limit: 4, active: tc.active, probeAt: now.Add(-time.Minute), bytes: tc.delta}
			_, next, _ := flow.probeProgress(now, 8)
			if next != 4 {
				t.Fatalf("%s grew to %d", tc.name, next)
			}
		})
	}
}

func TestMODISCongestionSuppressesExplorationUntilCooldown(t *testing.T) {
	now := time.Now()
	flow := &providerFlow{limit: 4, active: 4, probeAt: now.Add(-time.Minute), bytes: 2 << 20, gap: 200 * time.Millisecond}
	flow.finish(false, true, 8)
	if flow.limit != 2 {
		t.Fatalf("true upstream congestion must still reduce concurrency, got %d", flow.limit)
	}
	flow.active = flow.limit
	flow.errorRate = 0
	flow.addBytes(4 << 20)
	if _, next, _ := flow.probeProgress(now.Add(time.Minute), 8); next != 2 {
		t.Fatalf("probe ignored upstream congestion cooldown: %d", next)
	}
	flow.addBytes(4 << 20)
	if _, next, _ := flow.probeProgress(now.Add(4*progressProbeInterval), 8); next != 3 {
		t.Fatalf("probe never recovered after cooldown: %d", next)
	}
}

func TestProviderLowThroughputThresholdUsesRecentMedian(t *testing.T) {
	flow := &providerFlow{}
	policy := config.LowThroughputPolicy{
		FloorBytesPerSecond: 64 << 10,
		BaselineFraction:    0.15,
	}
	if got := flow.lowThroughputThreshold(policy); got != 64<<10 {
		t.Fatalf("empty baseline threshold=%d want %d", got, 64<<10)
	}
	for _, rate := range []int64{4 << 20, 6 << 20, 5 << 20} {
		flow.observeTransferRate(rate*10, 10*time.Second, policy.FloorBytesPerSecond)
	}
	want := int64(float64(5<<20) * 0.15)
	if got := flow.lowThroughputThreshold(policy); got != want {
		t.Fatalf("adaptive threshold=%d want %d", got, want)
	}
}

func TestProviderPacingAndCooldown(t *testing.T) {
	flow := &providerFlow{limit: 2, gap: time.Hour}
	if err := flow.pace(t.Context()); err != nil {
		t.Fatal(err)
	}
	flow.cooldown(2 * time.Hour)
	if time.Until(flow.nextStart) > time.Hour || time.Until(flow.nextStart) < 59*time.Minute {
		t.Fatalf("Retry-After cap not honored: %s", time.Until(flow.nextStart))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := flow.pace(ctx); err == nil {
		t.Fatal("pace should honor cancellation")
	}
}

func TestProviderActiveTransfersUsesDownloadingTasks(t *testing.T) {
	if got := providerActiveTransfers(map[string]int{"done": 66}); got != 0 {
		t.Fatalf("idle provider active transfers=%d want 0", got)
	}
	if got := providerActiveTransfers(map[string]int{"downloading": 3, "queued": 20}); got != 3 {
		t.Fatalf("active transfers=%d want 3", got)
	}
}

func TestCongestionClassification(t *testing.T) {
	if !isCongestion(&transfer.HTTPError{Status: 429}) || !isCongestion(&transfer.HTTPError{Status: 503}) {
		t.Fatal("rate limiting and server errors should reduce concurrency")
	}
	if isCongestion(&transfer.HTTPError{Status: 404}) {
		t.Fatal("missing objects should not reduce concurrency")
	}
	if isCongestion(&transfer.LowThroughputError{}) {
		t.Fatal("one recycled slow connection should not reduce provider concurrency")
	}
}
