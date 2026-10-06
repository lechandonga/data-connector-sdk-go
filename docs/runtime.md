# 隔离与调度语义

代码入口：`runtime`（`Host`、`Runner`、看门狗、有界队列、统计）。

## 1. 每个连接器一个独立 Runner

`Host.Run` 为每个成功装载的 source 连接器建立一个 `Runner`，各自运行在独立 goroutine，拥有互不共享的：

- 有界批次队列（容量 `BufferCapacity`）；
- 去重窗口与在途集合；
- 检查点 store、序号（seq）与世代号（epoch）；
- 统计计数器（`Stats`：读取数、投递数、去重数、在途批次、提交数、超时数、panic 数、契约错误数）。

一个连接器失败/超时/panic 不会终止其它连接器（见测试 `multi-connector/concurrent-isolated`）。

## 2. 超时与取消兜底

连接器被要求通过 `SourceContext`/`SinkContext` 的 `Done()` 协作响应取消，但宿主不依赖连接器“守规矩”：

- 每次 `Read` 在独立 goroutine 中执行，外层 `select` 由 `ReadTimeout` 看门狗兜底；即使连接器永远不返回，宿主也会判定 `timeout` 并停止该连接器；
- 下游 `Deliver`、sink 的 `Write`/`Commit` 同样由 `CommitTimeout` 兜底；
- 生命周期钩子 `Open`/`Close`/`Resume` 由 `guard()` 统一做超时+panic 收敛。

Go 无法安全强杀 goroutine：超时后失控 goroutine 仍会在底层调用返回时结束，但其影响被限制在该连接器内部，宿主进程与其它连接器继续运行。

## 3. Panic 收敛与归属

连接器构造、`Declare`、`Open`、`Read`、`Write`、`Commit`、`Deliver` 的 panic 全部在边界 recover，转成
`Failure{Connector: <名字>, Class: connector-panic, Op: <操作>}`，并累加该连接器的 `Panics` 统计。宿主进程不会因此崩溃。

## 4. 有界缓冲与背压

数据流：

```
Source.Read ──(记录级契约校验)──► 有界队列(BufferCapacity 个批次)
                                   │ 消费者逐个批次
                                   ▼
                           Downstream.Deliver(每条)
                                   ▼
                           (可选) Sink.Write + Commit
                                   ▼
                           checkpoint.Commit(批次位置)
```

- 队列满时生产者在 `queue <- batch` 处阻塞，且阻塞同时监听取消与 halt；**下游慢不会导致无界内存增长**，内存上界 ≈ `BufferCapacity` 个批次 + 1 个正在投递的批次。
- 取消沿链路传导：`context` 取消会关闭所有连接器上下文的 `Done()`；生产者停止读入，消费者**排空已取出的在途批次**（投递+提交）后退出，避免“已从 source 取走但未处理”的数据悬空。
- 不可恢复错误（panic/超时/契约违规）立即关闭 runner 级 `halt` 通道，读取链路在下一次循环边界停止，不会继续空转。

## 5. 失败分类（可定位、可区分）

所有失败统一为 `runtime.Failure`，带 `Connector` 名称与稳定 `Class`：

| Class | 含义 |
| --- | --- |
| `connector-error` | 连接器自己返回普通错误 |
| `contract-violation` | 产出违反声明契约 |
| `timeout` | 看门狗超时 |
| `cancelled` | 宿主动作（关闭/恢复）导致的取消，不作为异常失败上报 |
| `host-error` | 宿主设施错误（如检查点提交被拒/IO 错误） |
| `connector-panic` | 连接器 panic，已收敛 |

装载阶段错误见 `connector.LoadError`（`duplicate`/`declaration-missing`/`declaration-conflict`/`schema-incompatible`/`unsupported-api-version`/`connector-panic`/`open-failed`）。检查点错误见下一章。

## 6. 下游与投递语义

- `runtime.Downstream` 是最小消费接口；`Deliver` 阻塞即天然背压。
- `SinkDownstream` 把一个批次攒齐后调用一次 `Sink.Write` 再 `Sink.Commit`，保证每批一次写入一次提交。
- `runtime.DurableDownstream` 是本地 JSONL 持久下游：构造时回放已有记录 id，之后按 id 幂等追加并 fsync。它与 source 侧的 at-least-once 重放组合后，对外呈现“不重不漏”的效果。**非幂等/非持久下游仍可能看到重放**——这是 at-least-once 契约的明确语义，不做隐藏。
