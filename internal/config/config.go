package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Provider struct {
	SourceID                   string
	ArchiveRoot                string
	Enabled                    bool
	InitialTransferConcurrency int
	TransferConcurrency        int
	VerifyConcurrency          int
	RequestTimeout             time.Duration
	LowThroughput              LowThroughputPolicy
	AuthRef                    string
}

type LowThroughputPolicy struct {
	Profile             string
	Enabled             bool
	StartupGrace        time.Duration
	Window              time.Duration
	Sustain             time.Duration
	FloorBytesPerSecond int64
	BaselineFraction    float64
}

type Config struct {
	SocketPath           string
	DBPath               string
	CentralSocket        string
	DownloadRoot         string
	ArchiveRoot          string
	ArchiveGroupWritable bool
	ArchiveEventSocket   string
	Providers            []Provider
}

func Load() Config {
	home, _ := os.UserHomeDir()
	state := filepath.Join(home, ".local", "state", "satellite-data-downloader")
	runtime := runtimeRoot(home)
	providers := []Provider{
		provider("goes", true, 4, 8, 2, 3*time.Minute, ""),
		provider("himawari8", true, 1, 1, 1, 3*time.Minute, ""),
		provider("himawari9", true, 1, 1, 1, 3*time.Minute, ""),
		// Begin with 4 and let AIMD expand to 8 when LAADS stays healthy.
		provider("modis", true, 4, 8, 2, 5*time.Minute, "laads"),
		provider("viirs", true, 4, 4, 2, 5*time.Minute, "laads"),
		provider("mtg", true, 2, 2, 1, 10*time.Minute, "eumetsat"),
		provider("earthcare", true, 2, 2, 1, 10*time.Minute, "earthcare"),
		provider("calipso", true, 3, 3, 2, 10*time.Minute, "earthdata"),
		provider("cloudsat", true, 2, 2, 2, 10*time.Minute, "cloudsat"),
		provider("era5", true, 2, 2, 2, 10*time.Minute, "cds"),
	}
	return Config{
		SocketPath:           env("SATDOWN_SOCKET", filepath.Join(runtime, "satellite-data-downloader", "downloader.sock")),
		DBPath:               env("SATDOWN_DB", filepath.Join(state, "downloader.sqlite3")),
		CentralSocket:        env("SATDOWN_CENTRAL_SOCKET", filepath.Join(runtime, "satellite-data", "central.sock")),
		DownloadRoot:         env("SATDOWN_DOWNLOAD_ROOT", filepath.Join(state, "delivery")),
		ArchiveRoot:          env("SATDOWN_ARCHIVE_ROOT", filepath.Join(home, "satellite-downloads")),
		ArchiveGroupWritable: envBool("SATDOWN_ARCHIVE_GROUP_WRITABLE", true),
		ArchiveEventSocket:   env("SATDOWN_ARCHIVE_EVENT_SOCKET", filepath.Join(runtime, "satellite-data", "archive-events.sock")),
		Providers:            providers,
	}
}

func runtimeRoot(home string) string {
	if value := strings.TrimSpace(os.Getenv("XDG_RUNTIME_DIR")); value != "" {
		return value
	}
	return filepath.Join(home, ".local", "state")
}

func provider(source string, enabled bool, initialTransfers, maxTransfers, verifiers int, timeout time.Duration, authRef string) Provider {
	prefix := "SATDOWN_" + strings.ToUpper(strings.ReplaceAll(source, "-", "_"))
	transferConcurrency := envInt(prefix+"_TRANSFER_CONCURRENCY", maxTransfers, 1, 32)
	lowThroughputDefault := source == "goes" || source == "modis"
	profile := "default"
	grace, window, sustain := 30*time.Second, 30*time.Second, 45*time.Second
	floor, fraction := int64(64<<10), 0.15
	if source == "modis" {
		// Both arms use the same adaptive concurrency and transfer pipeline.
		profile = strings.ToLower(env(prefix+"_LOW_THROUGHPUT_PROFILE", "conservative"))
		if profile != "normal" {
			profile = "conservative"
		}
		if profile == "normal" {
			grace, window, sustain = 45*time.Second, 30*time.Second, 45*time.Second
			floor, fraction = 128<<10, 0.25
		} else {
			grace, window, sustain = 90*time.Second, 60*time.Second, 120*time.Second
			floor, fraction = 64<<10, 0.12
		}
	}
	return Provider{
		SourceID:                   source,
		ArchiveRoot:                strings.TrimSpace(os.Getenv(prefix + "_ARCHIVE_ROOT")),
		Enabled:                    envBool(prefix+"_ENABLED", enabled),
		InitialTransferConcurrency: envInt(prefix+"_INITIAL_TRANSFER_CONCURRENCY", min(initialTransfers, transferConcurrency), 1, transferConcurrency),
		TransferConcurrency:        transferConcurrency,
		VerifyConcurrency:          envInt(prefix+"_VERIFY_CONCURRENCY", verifiers, 1, 32),
		RequestTimeout:             envDuration(prefix+"_REQUEST_TIMEOUT", timeout),
		LowThroughput: LowThroughputPolicy{
			Profile:             profile,
			Enabled:             envBool(prefix+"_LOW_THROUGHPUT_ENABLED", lowThroughputDefault),
			StartupGrace:        envDuration(prefix+"_LOW_THROUGHPUT_STARTUP_GRACE", grace),
			Window:              envDuration(prefix+"_LOW_THROUGHPUT_WINDOW", window),
			Sustain:             envDuration(prefix+"_LOW_THROUGHPUT_SUSTAIN", sustain),
			FloorBytesPerSecond: envInt64(prefix+"_LOW_THROUGHPUT_FLOOR_BPS", floor, 1, 1<<40),
			BaselineFraction:    envFloat(prefix+"_LOW_THROUGHPUT_BASELINE_FRACTION", fraction, 0.01, 1),
		},
		AuthRef: authRef,
	}
}

func env(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envBool(name string, fallback bool) bool {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return fallback
	}
	return parsed
}
func envInt(name string, fallback, minimum, maximum int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed < minimum || parsed > maximum {
		return fallback
	}
	return parsed
}
func envInt64(name string, fallback, minimum, maximum int64) int64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return fallback
	}
	return parsed
}
func envFloat(name string, fallback, minimum, maximum float64) float64 {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil || parsed < minimum || parsed > maximum {
		return fallback
	}
	return parsed
}
func envDuration(name string, fallback time.Duration) time.Duration {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}
