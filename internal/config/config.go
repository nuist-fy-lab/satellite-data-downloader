package config

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type Provider struct {
	SourceID            string
	ArchiveRoot         string
	Enabled             bool
	TransferConcurrency int
	VerifyConcurrency   int
	RequestTimeout      time.Duration
	AuthRef             string
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
	providers := []Provider{
		provider("goes", true, 4, 2, 3*time.Minute, ""),
		provider("himawari8", true, 1, 1, 3*time.Minute, ""),
		provider("himawari9", true, 1, 1, 3*time.Minute, ""),
		provider("modis", true, 4, 2, 5*time.Minute, "laads"),
		provider("viirs", true, 4, 2, 5*time.Minute, "laads"),
		provider("mtg", true, 2, 1, 10*time.Minute, "eumetsat"),
		provider("earthcare", true, 2, 1, 10*time.Minute, "earthcare"),
		provider("calipso", true, 3, 2, 10*time.Minute, "earthdata"),
		provider("cloudsat", true, 2, 2, 10*time.Minute, "cloudsat"),
		provider("era5", true, 2, 2, 10*time.Minute, "cds"),
	}
	return Config{
		SocketPath:           env("SATDOWN_SOCKET", filepath.Join(state, "downloader.sock")),
		DBPath:               env("SATDOWN_DB", filepath.Join(state, "downloader.sqlite3")),
		CentralSocket:        env("SATDOWN_CENTRAL_SOCKET", filepath.Join(home, ".local", "state", "satellite-data", "central.sock")),
		DownloadRoot:         env("SATDOWN_DOWNLOAD_ROOT", filepath.Join(state, "delivery")),
		ArchiveRoot:          env("SATDOWN_ARCHIVE_ROOT", filepath.Join(home, "satellite-downloads")),
		ArchiveGroupWritable: envBool("SATDOWN_ARCHIVE_GROUP_WRITABLE", true),
		ArchiveEventSocket:   env("SATDOWN_ARCHIVE_EVENT_SOCKET", filepath.Join(home, ".local", "state", "satellite-data", "archive-events.sock")),
		Providers:            providers,
	}
}

func provider(source string, enabled bool, transfers, verifiers int, timeout time.Duration, authRef string) Provider {
	prefix := "SATDOWN_" + strings.ToUpper(strings.ReplaceAll(source, "-", "_"))
	return Provider{
		SourceID:            source,
		ArchiveRoot:         strings.TrimSpace(os.Getenv(prefix + "_ARCHIVE_ROOT")),
		Enabled:             envBool(prefix+"_ENABLED", enabled),
		TransferConcurrency: envInt(prefix+"_TRANSFER_CONCURRENCY", transfers, 1, 32),
		VerifyConcurrency:   envInt(prefix+"_VERIFY_CONCURRENCY", verifiers, 1, 32),
		RequestTimeout:      envDuration(prefix+"_REQUEST_TIMEOUT", timeout),
		AuthRef:             authRef,
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
