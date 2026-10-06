# 连接器契约与兼容规则

## 统一接口

连接器实现 `connector.Connector`（契约接口 v1.x）：

```go
Open(ctx, cfg) error
Read(ctx, resume Checkpoint, emit func(Record) error) error
Close() error
```

宿主保证调用顺序 `Open -> Read -> Close`，`Close` 恰好一次。
`Read` 通过 `emit` 产出记录；`emit` 在下游背压时阻塞，在取消时返回错误，
连接器应把 `emit` 的错误原样返回并尽快退出。

## 声明（Manifest）

| 字段 | 说明 |
| --- | --- |
| `ID` | 全局唯一标识，注册表内不可重复 |
| `Interface` | 契约接口版本 `{Major, Minor}` |
| `Capabilities` | 能力集合：`read`（必需）、`resume`、`schema`、`cancelable` |
| `Schema` | 产出数据的模式；声明 `schema` 能力时必须提供 |

## 注册校验与拒绝原因

注册失败返回 `*connector.RejectError`，`Reason` 可区分：

- `duplicate_id`：同一 ID 以相同声明重复注册；
- `conflicting_manifest`：同一 ID 已登记不同声明；
- `missing_field`：缺 ID、缺能力、缺 `read` 能力、声明了 `schema` 但模式为 nil、字段名为空等；
- `unsupported_interface`：接口主版本与宿主不同，或 Minor 高于宿主；
- `incompatible_schema`：模式发生不兼容演进（见下）。

单个连接器被拒绝不影响其它连接器的注册与装载。

## 接口版本策略

宿主当前实现 `HostInterface = v1.0`：

- Major 不同（如 v0.x 写的旧连接器）：**明确拒绝**（`unsupported_interface`），
  绝不按新语义含糊执行；
- Major 相同、Minor 不高于宿主：接受；Minor 低于宿主时按旧语义**兼容适配**，
  缺失的可选行为一律按宿主默认语义确定性补齐；
- Minor 高于宿主：拒绝，避免执行连接器预期之外的语义。

## 数据模式演进规则

`host.CheckSchemaCompatible(old, new)` 判定演进是否兼容，规则确定、不做静默降级：

1. 已有字段不得删除、改名或改类型；
2. 已有字段不得由可选变为必需；
3. 新增字段必须是可选字段，且**必须显式声明 `Default`**——
   宿主不会用类型零值冒充默认值糊过去；
4. 模式名必须一致。

此外，恢复时已提交检查点中记录的 `SchemaHash` 与当前声明不一致时，
宿主以 `schema_drift` 拒绝启动，防止按不兼容的新模式继续消费。

## 记录契约

- `Record.Seq` 在连接器内从 1 起严格单调递增；
- 恢复点之后的记录若序号回退或重复，宿主判定为契约违规
  （`RunError.Kind = contract_violation`）并终止本次运行；
- 声明了模式时，每条记录必须包含全部必需字段，否则同样按契约违规处理。
