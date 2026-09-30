package config

import "testing"

func TestERA5WorkerUsesCentreIPC(t *testing.T) {
	cfg := Load()
	for _, p := range cfg.Providers {
		if p.SourceID == "era5" {
			if !p.Enabled || p.TransferConcurrency != 2 || p.VerifyConcurrency != 2 || p.AuthRef != "cds" {
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
