# 运行时隔离与调度语义

## 隔离模型

- 每个连接器由工厂函数创建**独立实例**，在独立 goroutine 中执行
  `Open/Read/Close`；
- 连接器到宿主之间只有一条容量为 `BufferSize` 的有界通道，
  是背压的唯一传导路径；
- 每个连接器拥有独立的检查点条目、统计（`Stats`）与运行标识（`RunID`），
  并发运行时互不干扰；
- 任一连接器的失败（报错、panic、超时）只影响自身，不影响其它连接器。

## 超时与取消兜底

- `ReadTimeout` 到期或外部取消后，宿主取消运行上下文；
- 连接器在 `CancelGrace` 宽限期内退出：按其返回的错误正常归类；
- 宽限期后仍不退出：宿主**隔离遗弃**该 goroutine（Go 无法强杀 goroutine），
  按确定语义返回 `KindTimeout`（超时）或 `KindHost`（外部取消），
  宿主进程与其它连接器不受影响。被遗弃的 goroutine 即使之后醒来，
  也只能阻塞在已无人读取的缓冲上，无法再提交任何检查点。

## panic 收敛

连接器 goroutine 内的 panic 被 `recover` 收敛，包装为
`RunError{Kind: KindPanic, PanicValue: ...}` 并归属到具体连接器与运行，
宿主进程不会崩溃。

## 背压

- 下游 `Sink.Write` 未确认前，宿主不会从缓冲取下一条；
- 缓冲满时连接器的 `emit` 阻塞，内存占用以 `BufferSize + 1` 条记录为上界；
- 取消信号沿 `emit -> Read -> 连接器` 传导，阻塞中的 `emit` 会随上下文取消而返回错误。

## 失败分类

每次失败返回 `*RunError`，`Kind` 区分来源，并携带 `ConnectorID` 与 `RunID` 便于定位：

| Kind | 含义 |
| --- | --- |
| `connector_error` | 连接器自身在 Open/Read/Close 返回的错误 |
| `contract_violation` | 违反契约（序号回退、缺必需字段等） |
| `timeout` | 超过截止时间，或取消后宽限期内仍不退出 |
| `panic` | 连接器执行中 panic，已收敛 |
| `host_action` | 宿主动作导致（主动取消、下游拒绝、检查点存储失败） |

## 并发调度

`Supervisor.RunAll` 为每个连接器启动独立 `Runner`，等待全部结束后
按连接器逐一返回 `Result{Stats, Err}`。各连接器的进度、临时状态与
统计完全隔离；建议配合 `go test -race` 验证并发正确性（本仓库测试已覆盖）。
