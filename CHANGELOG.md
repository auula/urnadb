# Changelog


## 1.5.1 - 2026-04-27

### Fixed

- 🐛 Fixed resolve mvcc conflict bug in transactions.

---

## 1.5.0 - 2026-04-21

### Added

- 🎉 Added support for full transactional integrity and improved data reading using mmap.

### Fixed

- 🔍 Fixed atomic export issue with `index.db`.
- ⏰ Fixed anomalies in `ttl` calculations.
- ♻️ Fixed interruption issues during `checkpoint` generation.
- 💬 Improved CLI messages and enhanced readability of `json body` output.

---


## 1.1.2 - 2025-04-05

### Added

- 🎉 针对 checkpoint 功能做出改进，以加速崩溃恢复过程中的启动速度，解决 checkpoint 生成过程中突然中断后恢复数据一致性问题。

### Fixed
- 🚄 修复 `checkpoint` 并发状态同步问题。
- 🗑️ 清理 `checkpoint` 中断过程中生成临时文件。
- 🧲 优化内存分配策略，使用内存池复用高频使用的组建。
- ♻️ 修复 `checkpoint` 生成过程中突然中断数据一致性问题。
- ⛰️ 缩小默认单个 `region` 区域大小为 2GB，减少垃圾回收和 `checkpoint` 扫描所需的时间。

---


## 1.1.1 - 2025-03-25

### Added
- 🎉 添加 Checkpoint 功能，以加速崩溃恢复过程中的启动速度，并确保恢复后的数据一致性。

### Fixed
- ♻️ 更换垃圾回收器状态同步机制。
- 📄 更换 `IP Whitelist` 开启方式。
- 🗜️ 更换数据编码解码为 `Msgpack` 格式。
- 🫙 更改原有 `List` 类型为 `Collection` 类型。

---

## 1.0.1 - 2025-03-08

### Added
- 🎉 添加新的 `query/{key}` 查询接口，能直接返回 `{key}` 数据类型、存储的 BSON 数据和剩下的存活时间。

### Fixed
- 🐛 修复索引恢复时排除已经过期的索引 `Inode`。
- 🏗️ 更换容器启动方式为 `ENTRYPOINT` 方式。
- ♻️ 更换垃圾回收器调度周期设置方式为 `cron`。
- 📜 使用类 CURL 请求的返回 JSON 结构格式更为美观。

---

## 1.0.0 - 2025-02-26

🎉 **首个 Beta 版本发布**，支持 `Linux`、`macOS`、`Windows` 平台，并提供容器镜像。

### Added
- 支持多种内置的数据结构。
- 支持数据安全 IP 白名单访问功能。
- 提供高吞吐量、低延迟、高效批量数据写入。
- 支持磁盘数据存储和磁盘垃圾数据回收。
- 支持磁盘数据静态加密和静态数据压缩。
- 通过基于 RESTful API 协议操作数据。
