# Satellite Data Downloader（卫星数据下载器）

[English](README.md) | **简体中文**

为
[`nuist-fy-lab/satellite-data`](https://github.com/nuist-fy-lab/satellite-data)
提供持久化、按数据源隔离的传输、校验与归档服务。

这个守护进程的职责刻意保持很窄：
`satellite-data` 负责官方目录、数据发现、持久订单、凭据和用户 API；
下载器只负责把已经批准的资产任务可靠地变成经过校验的归档文件。

## 核心职责

- 在开始工作前把下载任务持久化到 SQLite（嵌入式数据库）；
- 为每个 Provider（数据提供方）维护独立的传输与校验 Worker（工作进程）池；
- 支持 HTTP（超文本传输协议）断点续传；
- 在上游拥塞时动态调整提供方并发和请求节奏；
- 将网络传输与完整性校验拆成两个独立阶段；
- 将校验完成的文件原子提升到正式归档；
- 目标路径已有不同文件时拒绝静默覆盖；
- 使用持久事件把归档完成状态可靠地回报中心服务。

下载器**不负责**官方目录发现、门户登录、提供方账号、凭据、代理配置和公共 Web UI（网页界面）。

## 架构

```text
 satellite-data
 数据发现 + 订单 + 凭据
          │
          │ 私有 Unix Socket（套接字）
          ▼
 ┌─────────────────────────────┐
 │ satellite-data-downloader   │
 │                             │
 │ 持久 SQLite 任务队列        │
 │          │                  │
 │          ▼                  │
 │ 数据源传输 Worker           │
 │          │                  │
 │          ▼                  │
 │ 数据源校验 Worker           │
 │          │                  │
 │          ▼                  │
 │ 原子归档提升                │
 └──────────┬──────────────────┘
            │
            ├── 正式归档文件
            └── 持久完成事件 ─────────► satellite-data
```

传输与校验是两个独立阶段。
网络下载一旦持久化进入 `verifying`（校验中）状态，传输 Worker 就可以立即领取下一个任务；
另一个同数据源校验 Worker 负责哈希和归档提升。

## 数据源隔离

每个数据源都有自己的传输池、校验池、自适应并发和重试时钟。
因此一个变慢或故障的数据源不会占用其他数据源的工作槽位。

| 数据源 | 初始 / 最大传输 Worker | 校验 Worker | 中心认证引用 |
| --- | ---: | ---: | --- |
| GOES | 4 / 8 | 2 | 匿名 / 中心路由 |
| Himawari-8 | 1 / 1 | 1 | 匿名 / 中心路由 |
| Himawari-9 | 1 / 1 | 1 | 匿名 / 中心路由 |
| MODIS | 4 / 8 | 2 | `laads` |
| VIIRS | 4 / 4 | 2 | `laads` |
| MTG | 2 / 2 | 1 | `eumetsat` |
| EarthCARE | 2 / 2 | 1 | `earthcare` |
| CALIPSO | 3 / 3 | 2 | `earthdata` |
| CloudSat | 2 / 2 | 2 | `cloudsat` |
| ERA5 | 2 / 2 | 2 | `cds` |

Himawari 默认每颗卫星只使用 1 个传输 Worker 和 1 个校验 Worker，
避免大量分片的 AHI（先进向日葵成像仪）全圆盘数据独占共享带宽和 I/O（输入输出）。

### 自适应请求节奏

每个提供方初始网络启动间隔为 200 ms（毫秒）。
连续出现超时或 HTTP 408 / 429 / 5xx 时，系统会降低自适应并发，
并把启动间隔逐步增加到最多 5 秒。

持续成功后，并发会逐步恢复，启动间隔最低可回落到 100 ms。

如果上游返回 `Retry-After`（建议重试时间），系统会暂停该提供方的新传输，
最长尊重 1 小时。所有卫星产品的单任务下载重试使用相同的持久化指数退避
和等幅随机抖动：基准间隔按 5、10、20、40 秒等倍增，最多为 10 分钟，
每次实际间隔随机落在基准的 50%–100% 范围。若服务端提供
`Retry-After`，以该时间为最短等待时间，并额外加入少量随机抖动，
避免任务在同一时刻集中重试。

HTTP 404 / 410 也会重试，最多确认 **6 次下载尝试**，之后才标记永久失败；
超时、429、5xx 和临时中央访问故障则持续按上限退避，等待服务恢复。
非法请求（如 HTTP 400 / 422）直接失败；认证失效继续交给凭据与路由池处理，
无可用凭据时进入等待认证，而不频繁请求失效令牌。

GOES 和 MODIS 默认从 4 个传输 Worker 开始，并可自适应提升到 8 个；
其他数据源通常从配置的最大并发开始。
可通过 `SATDOWN_<PROVIDER>_INITIAL_TRANSFER_CONCURRENCY` 单独覆盖初始值。

### 慢连接回收

GOES 默认开启低吞吐保护。连接启动后先给予 30 秒宽限期，随后按 30 秒移动窗口评估速度；
如果连续 45 秒保持异常低速，则主动回收该连接。

回收不会丢弃已经验证的 `.part`（部分下载）文件和 ETag（实体标签），
因此持久重试可以继续使用 `Range` / `If-Range` 从已有位置续传，而不是重新下载整个对象。

慢速阈值取以下两者较大值：

- 64 KiB/s；
- 最近 16 个正常完成传输速度中位数的 15%。

单个慢连接被回收不会直接降低整个 Provider 的自适应并发；
提供方级降并发仍由独立的拥塞判断负责。

该策略可通过以下变量调整：

```text
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_ENABLED
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_STARTUP_GRACE
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_WINDOW
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_SUSTAIN
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_FLOOR_BPS
SATDOWN_<PROVIDER>_LOW_THROUGHPUT_BASELINE_FRACTION
```

## 访问与凭据模型

所有提供方凭据、账号、代理路由和认证逻辑都属于 `satellite-data` 中心服务。

下载器没有自己的 Vault（凭据保险库）、访问数据库，也不支持从环境变量回退读取 Secret（敏感信息）。
每次传输前，它都会通过私有中心 Unix Socket 请求一条允许使用的访问路由。

中心代理的传输路径会保留断点续传需要的：

- `Range`；
- `If-Range`；
- `ETag`。

即使 GOES、Himawari 这类数据源本身支持匿名访问，下载器仍然依赖中心的路由策略。
如果当前没有可用路由，任务会进入 `waiting_auth`（等待访问条件）；
中心恢复可用路由后，任务会自动重新释放到传输队列。

`SATDOWN_CENTRAL_SOCKET` 必须与中心服务的
`SATELLITE_CENTRAL_SOCKET` 指向同一个 Socket。

凭据迁移和回滚流程见中心项目：
[`docs/central-access.md`](https://github.com/nuist-fy-lab/satellite-data/blob/main/docs/central-access.md)。

## 任务生命周期

持久任务队列能够跨进程重启恢复：

```text
queued（排队）
  │
  ▼
downloading（下载中） ── 临时错误 ──► retry（重试）
  │
  ├── 中心无可用路由 ─────────────► waiting_auth（等待访问条件）
  │
  ▼
verifying（校验中）
  │
  ├── 完整性失败 ─────────────────► redownload（重新下载）
  ├── 归档路径冲突 ───────────────► verification retry（校验重试）
  │
  ▼
complete（完成）
  │
  ▼
archive-event outbox（归档事件发件箱）
  │
  └── 一直保留，直到 satellite-data 确认收到
```

HTTP 404 / 410 会进行最多 6 次带随机抖动的指数退避确认；持续不存在才将资产标记为失败。
凭据/路由失败也会与普通临时网络故障分开处理。

## 归档安全

归档提升采取保守策略：

- 文件先下载到 staging（暂存）/下载目录；
- 完整性校验完成后才允许进入正式归档；
- 文件系统允许时使用原子提升；
- 目标路径已经存在不同文件时绝不静默覆盖；
- 完整性失败会删除损坏的暂存文件并安排重新下载；
- 完成事件持久保存在 SQLite outbox（发件箱）中，直到中心明确确认。

### 历史文件旁路复核（Sidecar）

历史归档由独立 `satellite-checksum-sidecar` 进程负责，不使用下载器的传输、
校验 Worker，也不把下载与历史扫描排成一条串行队列。Sidecar 只读访问现有
`downloader.sqlite3`，它自己的断点、异常和重试状态保存在
`~/.local/state/satellite-data-checksum-sidecar/audit.sqlite3`。
校验成功后，通过本地 Unix socket 仅补充已经完成的记录的官方校验字段，
**不改下载状态、路径、文件 SHA-256 或正式文件内容**。

默认单线程、最多 8 MiB/s 归档读取速率、官方元数据间隔至少 500 ms，
并设置低 CPU／磁盘 IO 优先级。中断后从检查点续跑；远端元数据故障单独退避，
已归档文件丢失、大小不一致或 SHA-256／官方校验不匹配记录为
`missing`／`mismatch`，由人工排查，不自动删除、覆盖或重新下载历史文件。
旧版记录即使没有官方校验码，也重新核算本地 SHA-256，随后记录
`local_only`。可以使用 `deploy/systemd/satellite-checksum-sidecar.service`
作为独立用户服务安装运行，完全不依赖 ChatGPT 云端任务。

因此，即使中心服务临时不可用，也不会丢失已经完成的归档通知。

### 官方校验码与本地 SHA-256

下载器始终计算本地 SHA-256，以验证暂存缓存和归档副本的一致性。
对已确认提供官方校验码的源，在独立的校验 Worker 中直接查询公开的
**文件级官方元数据**，校验通过后才允许归档：

| 来源 | 官方文件校验 | 元数据接口 |
|---|---|---|
| MODIS、VIIRS、CALIPSO | MD5 | NASA CMR UMM-G；根据颗粒 ID 和文件名严格匹配 |
| GOES | MD5 + CRC32C（同时比对） | Google Cloud Storage 对象元数据 |
| MTG | MD5 | EUMETSAT Browse 产品详情 |
| Himawari-9 | CRC32／CRC32C／SHA-1／SHA-256 中该对象实际返回的完整文件校验 | S3 对象 HEAD，且只采用 FULL_OBJECT |
| Himawari-8、EarthCARE、CloudSat、ERA5 | 无已确认的可用官方文件校验值 | 仅本地 SHA-256；不会把 ETag 误认成 MD5 |

这些公开元数据查询不使用下载代理，也不会占用并发传输 Worker；
现有排队文件无需重新生成订单。校验值和状态保存在任务数据库：
`official_checksum_status=verified` 表示通过官方校验，
`local_only` 表示没有官方校验值，
`legacy_local_only` 表示旧版已经完成、尚未追溯官方校验的文件。
如果官方元数据暂时无法访问，任务保留暂存内容并进入校验重试；
如果官方校验与文件内容不一致，绝不归档，记录错误并安排重新下载。
本地 SHA-256 与所有官方散列在同一轮文件读取中计算，避免重复扫描文件。

共享归档模式下，新目录和文件可配置为组可写。
如果为某个 Provider 设置了专用归档根目录，则该目录优先于通用归档根目录。

## 配置

完整的非敏感配置示例位于：

[`deploy/systemd/satellite-data-downloader.env.example`](deploy/systemd/satellite-data-downloader.env.example)

### 核心路径

| 变量 | 用途 | 默认值 / 生产示例 |
| --- | --- | --- |
| `SATDOWN_SOCKET` | 下载器任务 API Unix Socket | `$XDG_RUNTIME_DIR/satellite-data-downloader/downloader.sock` |
| `SATDOWN_DB` | 持久任务队列数据库 | `~/.local/state/satellite-data-downloader/downloader.sqlite3` |
| `SATDOWN_DOWNLOAD_ROOT` | 暂存 / 下载根目录 | `~/.local/state/satellite-data-downloader/delivery` |
| `SATDOWN_ARCHIVE_ROOT` | 通用卫星归档根目录 | 生产环境：`/misc/nuist_data1/public_sharing/satellite` |
| `SATDOWN_CENTRAL_SOCKET` | 中心认证 / 抓取 Socket | `$XDG_RUNTIME_DIR/satellite-data/central.sock` |
| `SATDOWN_ARCHIVE_EVENT_SOCKET` | 归档完成事件 Socket | `$XDG_RUNTIME_DIR/satellite-data/archive-events.sock` |

生产环境还使用专用共享归档：

```text
SATDOWN_EARTHCARE_ARCHIVE_ROOT=/misc/nuist_data1/public_sharing/EarthCare
SATDOWN_ERA5_ARCHIVE_ROOT=/misc/nuist_data1/public_sharing/ERA5
```

### 数据源 Worker 配置

每个 Provider 都支持：

```text
SATDOWN_<PROVIDER>_ENABLED
SATDOWN_<PROVIDER>_INITIAL_TRANSFER_CONCURRENCY
SATDOWN_<PROVIDER>_TRANSFER_CONCURRENCY
SATDOWN_<PROVIDER>_VERIFY_CONCURRENCY
SATDOWN_<PROVIDER>_REQUEST_TIMEOUT
SATDOWN_<PROVIDER>_ARCHIVE_ROOT
```

并发配置会被限制在 `1..32`。

下载器运行配置应保持**无 Secret**。
不要把提供方 Token、密码或代理密码加入下载器环境变量。

## 运行时目录与 Socket

当存在 `XDG_RUNTIME_DIR` 时，Unix Socket 默认放在该节点本地运行时目录，
避免共享 HOME（主目录）文件系统让其他节点看到同一路径但实际上连接到错误的 Listener（监听器）。

不要把实时 Unix Socket 放到 GPFS、NFS 或其他共享文件系统。
数据库需要持久化，但 Listener 路径必须保持节点本地。

## systemd 服务

项目提供用户级 systemd（Linux 服务管理器）单元：

```text
deploy/systemd/satellite-data-downloader.service
```

它会读取可选运行配置：

```text
~/.config/satellite-data-downloader/runtime.env
```

常用检查命令：

```bash
systemctl --user status satellite-data-downloader.service
journalctl --user -u satellite-data-downloader.service -n 100 --no-pager
```

服务使用 `Restart=on-failure`（失败后自动重启）、
`NoNewPrivileges=true`（禁止获得新权限）以及私有临时目录。

## 仓库结构

```text
cmd/satellite-data-downloader/   守护进程入口

internal/
  access/                       中心路由 / 认证客户端
  archiveevent/                 持久化归档完成通知
  config/                       非敏感运行配置
  model/                        任务 / 状态模型
  pipeline/                     Provider Worker、限速与重试
  server/                       私有下载任务 API
  store/                        SQLite 队列与恢复
  transfer/                     断点 HTTP 传输 + 校验 / 归档提升

deploy/systemd/                 用户级服务与环境变量示例
```

## 与 `satellite-data` 的职责关系

两个仓库应保持明确的概念边界：

| `satellite-data` | `satellite-data-downloader` |
| --- | --- |
| 发现官方资产 | 传输已批准资产 |
| 管理用户和 API Key | 不提供公共用户认证 |
| 管理提供方凭据和代理路由 | 使用中心授权的路由 |
| 调度持久化数据获取订单 | 持久化单个传输任务 |
| 索引归档覆盖率 | 校验文件并提升到归档 |
| 提供门户 / API / CLI 状态 | 提供私有任务 / 进度状态 |

中心服务临时不可用不会使已经完成的下载丢失：
归档完成事件会一直保留到中心确认。

## 开发与验证

项目构建和测试只通过 GitHub Actions（云端自动化工作流）执行。
开发机和生产节点用于源码编辑、配置、部署和运行状态检查，
不作为替代的本地构建或测试环境。
