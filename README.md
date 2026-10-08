# Satellite Data Downloader

[English](README.md) | [简体中文](README.zh-CN.md)

Durable, provider-isolated transfer, verification and archive-promotion service
for [`nuist-fy-lab/satellite-data`](https://github.com/nuist-fy-lab/satellite-data).

This daemon is intentionally narrow: `satellite-data` owns catalogues, discovery,
orders, credentials and user-facing APIs; the downloader turns approved asset tasks
into verified archive files.

## What it does

- persists download tasks in SQLite before work starts;
- keeps independent transfer and verification pools per provider;
- resumes interrupted HTTP transfers;
- adapts provider concurrency and request pacing under congestion;
- separates network transfer from integrity verification;
- atomically promotes verified files into provider archives;
- refuses to overwrite a different file at an existing archive path;
- reports durable archive-completion events back to the centre.

It does **not** own catalogue discovery, portal authentication, provider accounts,
credentials, proxy configuration or the public web UI.

## Architecture

```text
 satellite-data
 discovery + orders + credentials
          │
          │ private Unix sockets
          ▼
 ┌─────────────────────────────┐
 │ satellite-data-downloader   │
 │                             │
 │ durable SQLite task queue   │
 │          │                  │
 │          ▼                  │
 │ provider transfer workers   │
 │          │                  │
 │          ▼                  │
 │ provider verify workers     │
 │          │                  │
 │          ▼                  │
 │ atomic archive promotion    │
 └──────────┬──────────────────┘
            │
            ├── archive file
            └── durable completion event ──► satellite-data
```

Transfer and verification are separate stages. Once a network transfer is
checkpointed as `verifying`, that transfer worker can immediately accept another
task while a provider-local verification worker hashes and promotes the completed
payload.

## Provider isolation

Each source has its own transfer pool, verification pool, adaptive concurrency and
retry clock. A slow or failing provider therefore cannot consume another
provider's worker slots.

| Provider | Initial / max transfers | Verify workers | Centre auth reference |
| --- | ---: | ---: | --- |
| GOES | 4 / 8 | 2 | anonymous / centre route |
| Himawari-8 | 1 / 1 | 1 | anonymous / centre route |
| Himawari-9 | 1 / 1 | 1 | anonymous / centre route |
| MODIS | 4 / 4 | 2 | `laads` |
| VIIRS | 4 / 4 | 2 | `laads` |
| MTG | 2 / 2 | 1 | `eumetsat` |
| EarthCARE | 2 / 2 | 1 | `earthcare` |
| CALIPSO | 3 / 3 | 2 | `earthdata` |
| CloudSat | 2 / 2 | 2 | `cloudsat` |
| ERA5 | 2 / 2 | 2 | `cds` |

Himawari defaults to one transfer and one verifier per satellite because its
segmented AHI full-disk objects can otherwise dominate shared bandwidth and I/O.

### Adaptive pacing

Each provider starts with a 200 ms network-start gap. Repeated timeouts or
HTTP 408/429/5xx responses reduce adaptive concurrency and can extend the gap up
to 5 seconds. Sustained success gradually restores concurrency and can reduce the
gap to 100 ms.

`Retry-After` is respected for provider-wide starts, capped at one hour. All
satellite products use the same durable, equal-jitter exponential backoff for
download retries: 5s, 10s, 20s, 40s, etc. with a 10-minute capped baseline
and a randomized delay in [50%, 100%] of that baseline. A server-provided
`Retry-After` is treated as a minimum, with small extra jitter.

HTTP 404/410 results are rechecked for up to **six actual download attempts**
before becoming terminal failures. Temporary network errors, 429, 5xx and
central gateway outages continue retrying with capped backoff. Invalid
requests (e.g. 400/422) fail immediately; exhausted or unavailable credentials
use the separate waiting-for-auth flow.

GOES and MODIS start at four transfers and can adapt up to their ceiling of eight.
Other providers normally start at their configured ceiling. The initial value can
be overridden with `SATDOWN_<PROVIDER>_INITIAL_TRANSFER_CONCURRENCY`.

### Slow-transfer recycling

GOES enables a low-throughput guard by default. After a 30-second startup grace,
the downloader evaluates a 30-second moving window. A connection that remains
abnormally slow for 45 seconds is recycled while preserving its validated
`.part` file and ETag, so the durable retry resumes with `Range`/`If-Range`
instead of restarting the object.

The threshold is the greater of 64 KiB/s and 15% of the median rate of the last
16 normally completed transfers for that provider. Recycling one slow connection
does not by itself reduce provider-wide adaptive concurrency; congestion handling
remains a separate signal.

The policy is configurable through:

```text
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_ENABLED
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_STARTUP_GRACE
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_WINDOW
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_SUSTAIN
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_FLOOR_BPS
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_BASELINE_FRACTION
```

## Access model

All provider credentials, accounts, proxy routes and authentication logic live in
the `satellite-data` centre.

The downloader has no credential Vault, access database or Secret environment
fallback. For every transfer it asks the centre for an allowed route over the
private central Unix socket. The centre-mediated transfer path preserves resume
headers such as `Range`, `If-Range` and `ETag`.

Even nominally anonymous providers still depend on the centre for route policy.
If no usable route is available, tasks enter `waiting_auth` and are released once
the centre reports a usable route again.

`SATDOWN_CENTRAL_SOCKET` must point to the same socket as
`SATELLITE_CENTRAL_SOCKET` in the centre service.

For the coordinated credential migration and rollback procedure, see the centre's
[`docs/central-access.md`](https://github.com/nuist-fy-lab/satellite-data/blob/main/docs/central-access.md).

## Task lifecycle

The durable queue is designed to survive process restarts.

```text
queued
  │
  ▼
downloading ── temporary failure ──► retry
  │
  ├── no usable centre route ─────► waiting_auth
  │
  ▼
verifying
  │
  ├── integrity failure ──────────► redownload
  ├── archive collision ──────────► verification retry
  │
  ▼
complete
  │
  ▼
archive-event outbox
  │
  └── retained until satellite-data acknowledges it
```

HTTP 404/410 responses are retried with jittered exponential backoff up to six
times before the asset is marked failed. Credential
and route failures are handled separately from ordinary transient network errors.

## Archive safety

Archive promotion is conservative:

- downloads land in the staging/download root first;
- verification completes before archive promotion;
- promotion is atomic where the filesystem permits it;
- an existing different file at the target path is never silently replaced;
- integrity failures remove the bad staged payload and schedule a redownload;
- completed archive events remain in a SQLite outbox until acknowledged by the
  centre, so a temporary centre outage does not lose completion state.

### Official checksums and local SHA-256

Local SHA-256 always protects staged caches and archived copies. When the
provider has a verified public file-level checksum API, independent verification
workers query official metadata before promoting the file:

| Provider | Authoritative file check | Metadata API |
| --- | --- | --- |
| MODIS / VIIRS / CALIPSO | MD5 | NASA CMR UMM-G, matching granule and filename |
| GOES | MD5 and CRC32C (both checked) | Google Cloud Storage object JSON |
| MTG | MD5 | EUMETSAT Browse product detail |
| Himawari-9 | Full-object CRC32 / CRC32C / SHA-1 / SHA-256 as actually returned | S3 HEAD with checksum mode |
| Himawari-8 / EarthCARE / CloudSat / ERA5 | No confirmed full-file checksum exposed by current data interfaces | Local SHA-256 only, never treating ETags as MD5 |

Metadata lookups use public direct routes and run **only in verification
workers**, without consuming download connections. Already-queued tasks benefit
without re-enqueueing. Completion records distinguish `verified` (official
checksum matched), `local_only` (no available official checksum), and
`legacy_local_only` (older completed records not retrospectively checked).
Temporary checksum API outages retry verification without redownloading;
checksum mismatches block promotion and schedule a clean redownload. SHA-256 and
all authoritative digests are computed in a single pass over the staged file.

Shared archive roots create group-writable files/directories when configured for
shared operation. Provider-specific archive roots override the general archive
root.

## Configuration

The complete non-secret example is in
[`deploy/systemd/satellite-data-downloader.env.example`](deploy/systemd/satellite-data-downloader.env.example).

### Core paths

| Variable | Purpose | Default / production example |
| --- | --- | --- |
| `SATDOWN_SOCKET` | Downloader task API Unix socket | `$XDG_RUNTIME_DIR/satellite-data-downloader/downloader.sock` |
| `SATDOWN_DB` | Durable queue database | `~/.local/state/satellite-data-downloader/downloader.sqlite3` |
| `SATDOWN_DOWNLOAD_ROOT` | Staging/download root | `~/.local/state/satellite-data-downloader/delivery` |
| `SATDOWN_ARCHIVE_ROOT` | General satellite archive root | production: `/misc/nuist_data1/public_sharing/satellite` |
| `SATDOWN_CENTRAL_SOCKET` | Centre authorization/fetch socket | `$XDG_RUNTIME_DIR/satellite-data/central.sock` |
| `SATDOWN_ARCHIVE_EVENT_SOCKET` | Completion-event socket | `$XDG_RUNTIME_DIR/satellite-data/archive-events.sock` |

Production also uses provider-specific shared roots where appropriate:

```text
SATDOWN_EARTHCARE_ARCHIVE_ROOT=/misc/nuist_data1/public_sharing/EarthCare
SATDOWN_ERA5_ARCHIVE_ROOT=/misc/nuist_data1/public_sharing/ERA5
```

### Provider worker settings

Every provider supports:

```text
SATDOWN_<PROVIDER>_ENABLED
SATDOWN_<PROVIDER>_INITIAL_TRANSFER_CONCURRENCY
SATDOWN_<PROVIDER>_TRANSFER_CONCURRENCY
SATDOWN_<PROVIDER>_VERIFY_CONCURRENCY
SATDOWN_<PROVIDER>_REQUEST_TIMEOUT
SATDOWN_<PROVIDER>_ARCHIVE_ROOT
```

Concurrency values are bounded to `1..32` by configuration loading.

Runtime configuration is intentionally non-secret. Do not add provider Tokens,
passwords or proxy passwords to the downloader environment.

## Runtime paths and sockets

When `XDG_RUNTIME_DIR` is available, Unix sockets default there so a shared HOME
filesystem cannot expose one node's live listener path to another node.

Avoid placing live Unix sockets on GPFS, NFS or another shared filesystem. State
databases may be durable, but listener paths must remain node-local.

## systemd service

The included user service is:

```text
deploy/systemd/satellite-data-downloader.service
```

It loads optional runtime settings from:

```text
~/.config/satellite-data-downloader/runtime.env
```

Typical status and log checks:

```bash
systemctl --user status satellite-data-downloader.service
journalctl --user -u satellite-data-downloader.service -n 100 --no-pager
```

The service uses `Restart=on-failure`, `NoNewPrivileges=true` and a private
temporary directory.

## Repository layout

```text
cmd/satellite-data-downloader/   daemon entry point

internal/
  access/                       centre route/auth client
  archiveevent/                 durable completion notifications
  config/                       non-secret runtime configuration
  model/                        task/status types
  pipeline/                     provider workers, pacing and retries
  server/                       private downloader task API
  store/                        SQLite queue and recovery
  transfer/                     resumable HTTP + verification/promotion

deploy/systemd/                 user unit + environment example
```

## Relationship with `satellite-data`

Keep the two repositories conceptually separate:

| `satellite-data` | `satellite-data-downloader` |
| --- | --- |
| discovers official assets | transfers approved assets |
| owns users and API Keys | has no public user authentication |
| owns provider credentials and proxy routes | consumes centre-authorized routes |
| schedules persistent acquisition orders | persists individual transfer tasks |
| indexes archive coverage | verifies and promotes files |
| exposes portal/API/CLI state | exposes private task/progress state |

The centre may be temporarily unavailable without losing already completed
downloads: completion notifications remain durable until acknowledged.

## Development and verification

Project builds and tests run in GitHub Actions. Development and production hosts
are used for source editing, configuration, deployment and runtime health checks,
not as substitute build/test runners.
