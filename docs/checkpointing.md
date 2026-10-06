# 检查点提交与恢复规则

代码入口：`checkpoint`（原子存储、校验、世代围栏）、`runtime.Runner`（提交时机与重放去重）。

## 1. 存储布局

- 每个连接器一个目录：`<CheckpointDir>/<connectorName>/<connectorName>.ckpt.json`；
- 每个连接器一份已接受 schema：`<SchemaDir>/schema-<connectorName>.json`；
- 检查点内容为 JSON：`connector`、`seq`、`epoch`、base64 编码的不透明 `position`、`recordId`（诊断用）。

`position` 对宿主是不透明字节，仅由产生它的 source 解释；顺序由连接器在 `Host.Run` 的 `order` 参数中提供（内置连接器用 `std.PositionLess`，即按记录索引）。

## 2. 原子提交（无半提交）

写入流程：临时文件 → `fsync` → `rename` → 目录 `fsync`。提交在每批记录被下游**全部确认**（以及 sink `Commit` 成功）之后发生；写入前的所有校验失败都会保留旧文件不动，因此：

- 不会出现 torn/half-written 的检查点；
- 并发提交由 store 内互斥串行化；
- 提交与取消同时发生时，要么旧检查点保持有效，要么新检查点完整可见，没有中间态。

## 3. 提交校验（回退/乱序/损坏）

| 情况 | 判定 | 错误 kind |
| --- | --- | --- |
| 文件无法解析为 JSON / position 非 base64 | 拒绝启动 | `checkpoint-corrupt`（wrap `ErrCorrupt`） |
| 同 epoch 内新 `seq` ≤ 已提交 `seq` | 拒绝 | `checkpoint-rollback` |
| 同 epoch 内新位置严格早于已提交位置（按连接器顺序） | 拒绝 | `checkpoint-rollback` |
| 新位置等于已提交位置且 seq 推进 | 接受（幂等重提交） | — |
| `epoch` 严格小于已提交世代 | 拒绝 | `checkpoint-epoch-fenced` |
| 目录/IO 失败 | 拒绝 | `checkpoint-io` |

所有检查点错误都带连接器名与稳定 kind，归为宿主侧 `host-error`，与连接器自身错误、契约错误、超时明确区分。

## 4. 恢复与“未确认不越过”

重启后 Runner：

1. 打开该连接器的 store；无记录则从头开始；
2. 损坏立即失败（不会当作“没有进度”静默重跑全量）；
3. 把 position 交给 source 的 `Resume`（未实现 `ResumableSource` 却存在检查点 → `contract-violation`）；
4. 从该位置继续读取。

**检查点只在下游确认整批后推进**，因此“未被确认消费的记录”不可能被检查点越过。崩溃发生在“已投递但未提交检查点”窗口时，恢复后 source 从上一已提交位置重放这批记录。

### 重放为何不会重复生效

Runner 维护两类 id 集合：

- `pending`：本世代已投递但尚未随批次提交；崩溃后这部分会重放，故**不**用于压制重放；
- `committed`：有界环形窗口（`DedupeWindow`），记录本世代已确认提交的 id；重放命中则丢弃并计入 `RecordsDeduped`。

跨进程的最终“不重复”由下游幂等保证：sink 契约要求 `Commit` 成功后重放同 id 不产生二次效果（内置 `MemorySink`/`FileSink` 按 id 去重；`runtime.DurableDownstream` 按日志 id 去重）。source+checkpoint 层提供的是 **at-least-once**，幂等下游叠加后达成不重不漏；非幂等下游看到重放是被明确文档化的语义。

## 5. 世代围栏

每次 `Host.Run` 递增 epoch。恢复/取消并发时，旧世代即使残留提交动作，其 epoch 也低于已提交值，会被 `checkpoint-epoch-fenced` 拒绝，不能覆盖新世代状态。
