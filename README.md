# Satellite Data Downloader

Shared archive roots configured per provider create group-writable setgid
subdirectories and group-writable files. Archive promotion never replaces a
different existing file with the same path. Completed tasks retain their
archive notification in the SQLite outbox until the centre verifies it; a
temporary centre failure is retried after restart.

Durable, provider-isolated transfer pipeline for the `nuist-fy-lab/satellite-data`
service.

The downloader deliberately does **not** own the public web UI or official
catalogue indexing. `satellite-data` discovers official granules and submits
idempotent asset tasks over a local Unix socket. This daemon persists those
tasks, downloads them, verifies them, atomically promotes them into the archive,
and exposes progress back to `satellite-data`.

## Pipeline

```text
satellite-data discovery
        |
        v
  Unix-socket API
        |
        v
 durable SQLite queue
        |
        +--> GOES transfer workers ------> GOES verify/promote workers
        +--> MODIS transfer workers -----> MODIS verify/promote workers
        +--> VIIRS transfer workers -----> VIIRS verify/promote workers
        +--> MTG transfer workers -------> MTG verify/promote workers
        +--> EarthCARE workers ----------> EarthCARE verify/promote workers
        +--> CALIPSO workers ------------> CALIPSO verify/promote workers
        +--> CloudSat workers -----------> CloudSat verify/promote workers
```

Every provider has its own worker pools and retry clock. A slow or broken
provider cannot consume another provider's transfer or verification slots.
Network starts are paced separately per provider: the interval starts at
200 ms, rises on repeated 408/429/5xx or timeouts (up to 5 seconds), and
falls after sustained success (no less than 100 ms). The existing adaptive
transfer concurrency stays between 1 and the configured per-provider maximum.
When supplied, `Retry-After` also delays new transfers for that provider,
up to one hour; individual task retries remain durable and jittered.
Himawari-8/9 use independent single-transfer/single-verifier pools by default
so their segmented AHI full-disk objects cannot starve other providers.

Network transfer and integrity verification are separate stages. A completed
network transfer is durably checkpointed as `verifying`, then the network worker
immediately takes another task while provider-local verification workers hash
and promote files.

## Authentication

Discovery may be public even when science-file transfer is not.

| Provider | Transfer authentication |
| --- | --- |
| GOES | anonymous |
| MODIS | LAADS download token |
| VIIRS | LAADS download token |
| MTG | EUMETSAT Consumer Key + Consumer Secret |
| EarthCARE | ESA MAAP bearer access token |
| CALIPSO | NASA Earthdata bearer token |
| CloudSat | CloudSat DPC THREDDS username + password |
| Himawari-8/9 | anonymous |

Credentials, accounts, proxies and provider authentication are owned exclusively
by the `satellite-data` centre. This downloader has no Vault, access database or
credential environment fallback. Its transfer client streams data through the
private centre Unix socket, preserving Range, If-Range and ETag.

Configure `SATDOWN_CENTRAL_SOCKET` to match `SATELLITE_CENTRAL_SOCKET` in the centre.
The socket is private (`0600`); both processes must use the same trusted service
user. Queue tables and task APIs are unchanged. Legacy access endpoints return
HTTP 410 rather than accepting Secrets.

Migration is coordinated with the matching centre release. Stop both processes,
retain old releases/database/key for rollback, and run the centre's
`satellited -migrate-access` against the existing local files. The centre must
have an empty access database. See the centre's `docs/central-access.md`.

## Configuration

General, non-secret runtime configuration and provider concurrency are separate
from credentials. See `deploy/systemd/satellite-data-downloader.env.example`.

The default Unix socket and state paths live under
`~/.local/state/satellite-data-downloader/`; the archive path must be configured
for the production data node.
