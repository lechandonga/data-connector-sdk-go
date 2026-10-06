# 连接器契约与兼容规则

代码入口：`contract`（契约本身）、`connector`（注册与装载校验）、`connector/v1`（旧版适配）。

## 1. 接口版本

每个连接器通过 `contract.Declaration` 自描述：

| 字段 | 含义 | 缺失/非法时的结果 |
| --- | --- | --- |
| `name` | 连接器唯一种类名，非空且无空白 | `declaration-missing`，拒绝装载 |
| `connectorVersion` | 连接器实现版本 | `declaration-missing`，拒绝装载 |
| `apiVersion` | 接口代际 | 仅接受 `connector.api/v2` 与 `connector.api/v1`；其它值返回 `unsupported-api-version`，**绝不按 v2 语义猜测运行** |
| `capabilities` | `source` / `sink` / `resumable` / `at-least-once` | 空列表拒绝；未知能力、重复能力、能力自相矛盾（如无 `source` 却声明 `resumable`）返回 `declaration-conflict` |
| `outputSchema` / `inputSchema` | 产出/接收数据的模式 | source 必须有 outputSchema，sink 必须有 inputSchema，否则拒绝 |

宿主支持两个版本：

- **v2（当前原生）**：`Source.Read(ctx, max) -> Batch`，`Sink.Write/Commit`。
- **v1（仅经显式适配器）**：`Pull(cancelled)` 单条拉取、`Put/Flush` 单条写入。`connector/v1.Adapter` 做确定性翻译，并把 v1 的 `id int64 + message string` 固定映射为 v2 的两字段 schema；**映射不了的 schema 直接拒绝**，不做猜测。实现了可选 `Seek(position)` 的 v1 source 才会被标记为 `resumable`。

## 2. 数据模式（Schema）

字段类型只有四种：`string`、`int64`、`float64`、`bool`（JSON tag 保持可读）。

模式本身的静态校验（`contract.ValidateSchema`）拒绝：空 schema 名、无字段、重名字段、未命名字段、未知字段类型。

### 模式演进规则（`contract.DiffSchema`）

| 变化 | 结论 | 原因 |
| --- | --- | --- |
| 新增**可选**字段 | 兼容 | 老记录该字段显式为 `null` |
| 新增**必填**字段 | **不兼容** `field-added-required` | 老记录无法产生该值 |
| 删除字段 | **不兼容** `field-removed` | 不允许静默丢数据 |
| 字段改类型 | **不兼容** `field-type-changed` | 不做隐式转换 |
| 可选收紧为必填 | **不兼容** `optional-to-required` | 老数据不满足 |
| 必填放宽为可选 | 兼容 | — |

宿主把“最近一次接受的 schema”持久化在 schema 目录（每连接器一个 JSON 文件）。**进程重启后注册一个带着不兼容 schema 的新构建，会在注册阶段被拒绝**，错误携带连接器名、稳定 kind 与逐条原因。

## 3. 记录级契约（不静默糊默认值）

每条产出记录在到达任何下游之前都按声明 schema 校验（`contract.ValidateRecord`）：

- 出现 schema 未声明的字段：`unknown-field`，拒绝；
- 必填字段缺失：`missing-required`，拒绝，**不填默认值**；
- 值类型不精确匹配（如 int 当 int64、float 当 int64）：`wrong-type`，拒绝，**不做隐式数值转换**；
- 可选字段缺失：允许，但宿主在归一化（`NormalizeRecord`）时显式写成 `null`，而不是“缺着当默认”。

记录级违例归类为 `contract-violation`，归属到具体连接器，该连接器停止，其它连接器不受影响。

## 4. 装载隔离

`Registry.Register` 在改动注册表前完成全部校验；一个坏连接器不影响后续注册。`Registry.Load` 逐个构造与 `Open`，构造/`Declare`/`Open` 中的 panic 都会被 recover，转成带连接器名的 `connector-panic` / `open-failed` 装载错误；成功的连接器照常返回。
