# 可配置项

配置结构：`runtime.Config`（`runtime/config.go`）。零值字段在 `NewHost` 时由 `DefaultConfig()` 填充。

| 字段 | 默认值 | 作用 |
| --- | --- | --- |
| `CheckpointDir` | `./.host/checkpoints` | 每连接器检查点目录的根 |
| `SchemaDir` | `./.host/schemas` | 已接受 schema 的持久化目录（跨重启兼容性校验） |
| `ReadTimeout` | `5s` | 单次 `Source.Read` 看门狗上限 |
| `CommitTimeout` | `5s` | `Downstream.Deliver`、sink `Write`/`Commit`、`Close` 的看门狗上限 |
| `OpenTimeout` | `3s` | `Open` / `Resume` 等生命周期钩子上限 |
| `BufferCapacity` | `4` | 连接器→下游的在途批次上界，也是背压阈值 |
| `BatchSize` | `32` | 每次 `Read` 请求的最大记录数 |
| `IdlePollInterval` | `50ms` | source 返回空批次后的轮询间隔 |
| `DedupeWindow` | `4096` | 已提交记录 id 的去重环形窗口大小（内存上界） |

`Host.Run` 的两个映射参数：

- `routes map[string]Downstream`：source 连接器名 → 下游；未出现的 source 会被打开但不泵取；
- `order map[string]checkpoint.PositionLess`：连接器名 → position 全序比较，用于拒绝检查点位置回退；不传则只按 seq/epoch 防护。

所有存储均为本地文件，无网络组件，标准库实现。
