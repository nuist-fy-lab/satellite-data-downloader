package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestGOESAdaptiveTransferDefaults(t *testing.T) {
	t.Setenv("SATDOWN_GOES_INITIAL_TRANSFER_CONCURRENCY", "")
	t.Setenv("SATDOWN_GOES_TRANSFER_CONCURRENCY", "")
	cfg := Load()
	for _, p := range cfg.Providers {
		if p.SourceID == "goes" {
			if p.InitialTransferConcurrency != 4 || p.TransferConcurrency != 8 || p.VerifyConcurrency != 2 {
				t.Fatalf("invalid GOES defaults: %+v", p)
			}
			if !p.LowThroughput.Enabled ||
				p.LowThroughput.StartupGrace != 30*time.Second ||
				p.LowThroughput.Window != 30*time.Second ||
				p.LowThroughput.Sustain != 45*time.Second ||
				p.LowThroughput.FloorBytesPerSecond != 64<<10 ||
				p.LowThroughput.BaselineFraction != 0.15 {
				t.Fatalf("invalid GOES low-throughput defaults: %+v", p.LowThroughput)
			}
			return
		}
	}
	t.Fatal("GOES transfer worker missing")
}

func TestLowThroughputPolicyCanBeOverriddenPerProvider(t *testing.T) {
	t.Setenv("SATDOWN_GOES_LOW_THROUGHPUT_ENABLED", "false")
	t.Setenv("SATDOWN_GOES_LOW_THROUGHPUT_STARTUP_GRACE", "45s")
	t.Setenv("SATDOWN_GOES_LOW_THROUGHPUT_WINDOW", "20s")
	t.Setenv("SATDOWN_GOES_LOW_THROUGHPUT_SUSTAIN", "1m")
	t.Setenv("SATDOWN_GOES_LOW_THROUGHPUT_FLOOR_BPS", "131072")
	t.Setenv("SATDOWN_GOES_LOW_THROUGHPUT_BASELINE_FRACTION", "0.25")
	for _, p := range Load().Providers {
		if p.SourceID == "goes" {
			policy := p.LowThroughput
			if policy.Enabled ||
				policy.StartupGrace != 45*time.Second ||
				policy.Window != 20*time.Second ||
				policy.Sustain != time.Minute ||
				policy.FloorBytesPerSecond != 131072 ||
				policy.BaselineFraction != 0.25 {
				t.Fatalf("GOES low-throughput overrides not applied: %+v", policy)
			}
			return
		}
	}
	t.Fatal("GOES transfer worker missing")
}

func TestInitialTransferConcurrencyNeverExceedsCeiling(t *testing.T) {
	t.Setenv("SATDOWN_GOES_TRANSFER_CONCURRENCY", "3")
	t.Setenv("SATDOWN_GOES_INITIAL_TRANSFER_CONCURRENCY", "8")
	for _, p := range Load().Providers {
		if p.SourceID == "goes" {
			if p.InitialTransferConcurrency != 3 || p.TransferConcurrency != 3 {
				t.Fatalf("GOES concurrency was not clamped to ceiling: %+v", p)
			}
			return
		}
	}
	t.Fatal("GOES transfer worker missing")
}

func TestMODISAdaptiveFourToEightAndComparedRecyclingPolicies(t *testing.T) {
	for _, tc := range []struct {
		profile                string
		grace, window, sustain time.Duration
		floor                  int64
		fraction               float64
	}{
		{"conservative", 90 * time.Second, 60 * time.Second, 120 * time.Second, 64 << 10, 0.12},
		{"normal", 45 * time.Second, 30 * time.Second, 45 * time.Second, 128 << 10, 0.25},
	} {
		t.Run(tc.profile, func(t *testing.T) {
			t.Setenv("SATDOWN_MODIS_LOW_THROUGHPUT_PROFILE", tc.profile)
			t.Setenv("SATDOWN_MODIS_LOW_THROUGHPUT_ENABLED", "")
			t.Setenv("SATDOWN_MODIS_TRANSFER_CONCURRENCY", "")
			for _, p := range Load().Providers {
				if p.SourceID != "modis" {
					continue
				}
				if p.InitialTransferConcurrency != 4 || p.TransferConcurrency != 8 || !p.LowThroughput.Enabled ||
					p.LowThroughput.Profile != tc.profile || p.LowThroughput.StartupGrace != tc.grace ||
					p.LowThroughput.Window != tc.window || p.LowThroughput.Sustain != tc.sustain ||
					p.LowThroughput.FloorBytesPerSecond != tc.floor || p.LowThroughput.BaselineFraction != tc.fraction {
					t.Fatalf("wrong MODIS adaptive/profile config: %+v", p)
				}
				return
			}
			t.Fatal("MODIS missing")
		})
	}
}

func TestERA5WorkerUsesCentreIPC(t *testing.T) {
	cfg := Load()
	for _, p := range cfg.Providers {
		if p.SourceID == "era5" {
			if !p.Enabled || p.InitialTransferConcurrency != 2 || p.TransferConcurrency != 2 || p.VerifyConcurrency != 2 || p.AuthRef != "cds" {
				t.Fatalf("invalid ERA5 defaults: %+v", p)
			}
			return
		}
	}
	t.Fatal("ERA5 transfer worker missing")
}

func TestArchiveSharingDefaultAndOptOut(t *testing.T) {
	t.Setenv("SATDOWN_ARCHIVE_GROUP_WRITABLE", "")
	if !Load().ArchiveGroupWritable {
		t.Fatal("default archive must allow group write and public read")
	}
	t.Setenv("SATDOWN_ARCHIVE_GROUP_WRITABLE", "false")
	if Load().ArchiveGroupWritable {
		t.Fatal("explicit private archive policy must be respected")
	}
}

func TestLoadUsesXDGRunTimeForIPC(t *testing.T) {
	home := t.TempDir()
	runtime := filepath.Join(t.TempDir(), "run")
	t.Setenv("HOME", home)
	t.Setenv("XDG_RUNTIME_DIR", runtime)
	t.Setenv("SATDOWN_SOCKET", "")
	t.Setenv("SATDOWN_CENTRAL_SOCKET", "")
	t.Setenv("SATDOWN_ARCHIVE_EVENT_SOCKET", "")

	cfg := Load()
	if want := filepath.Join(runtime, "satellite-data-downloader", "downloader.sock"); cfg.SocketPath != want {
		t.Fatalf("SocketPath=%q want %q", cfg.SocketPath, want)
	}
	if want := filepath.Join(runtime, "satellite-data", "central.sock"); cfg.CentralSocket != want {
		t.Fatalf("CentralSocket=%q want %q", cfg.CentralSocket, want)
	}
	if want := filepath.Join(runtime, "satellite-data", "archive-events.sock"); cfg.ArchiveEventSocket != want {
		t.Fatalf("ArchiveEventSocket=%q want %q", cfg.ArchiveEventSocket, want)
	}
	if want := filepath.Join(home, ".local", "state", "satellite-data-downloader", "downloader.sqlite3"); cfg.DBPath != want {
		t.Fatalf("DBPath=%q want %q", cfg.DBPath, want)
	}
}

func TestLoadFallsBackToHomeStateForIPC(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("XDG_RUNTIME_DIR", "")
	t.Setenv("SATDOWN_SOCKET", "")
	t.Setenv("SATDOWN_CENTRAL_SOCKET", "")
	t.Setenv("SATDOWN_ARCHIVE_EVENT_SOCKET", "")

	cfg := Load()
	root := filepath.Join(home, ".local", "state")
	if want := filepath.Join(root, "satellite-data-downloader", "downloader.sock"); cfg.SocketPath != want {
		t.Fatalf("SocketPath=%q want %q", cfg.SocketPath, want)
	}
	if want := filepath.Join(root, "satellite-data", "central.sock"); cfg.CentralSocket != want {
		t.Fatalf("CentralSocket=%q want %q", cfg.CentralSocket, want)
	}
	if want := filepath.Join(root, "satellite-data", "archive-events.sock"); cfg.ArchiveEventSocket != want {
		t.Fatalf("ArchiveEventSocket=%q want %q", cfg.ArchiveEventSocket, want)
	}
}
