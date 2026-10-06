# Data Connector SDK for Go (bootstrap)

一套纯本地、仅依赖 Go 标准库的数据集成连接器 SDK 与宿主运行时：第三方连接器按统一版本化契约接入，数据只在本地文件或内存中读写，不连接任何真实外部服务。设计重点是版本不匹配、连接器卡死/崩溃、下游变慢、断点重跑等异常下宿主行为**确定、可复现、可解释**。

## 包结构

| 包 | 职责 |
| --- | --- |
| `contract` | 接口版本（v1/v2）、声明、schema 与演进规则、记录校验、Source/Sink/Connector 接口 |
| `connector` | 注册表：声明校验、重复/冲突拒绝、跨重启 schema 演进校验、装载隔离 |
| `connector/v1` | 旧版 v1 连接器的显式兼容适配器（含 Seek 恢复） |
| `connector/std` | 内置连接器：内存 source/sink、NDJSON 文件 source/sink（可恢复、可去重） |
| `checkpoint` | 原子检查点存储：seq/epoch/position 校验、回退与损坏拒绝、世代围栏 |
| `runtime` | 宿主与每连接器 Runner：看门狗超时、panic 收敛、有界背压队列、重放去重、统计 |
| `cmd/demo` | 本地可运行的演示（两个连接器并发 + 持久下游） |

## 文档

- 契约与兼容规则：[docs/contract.md](docs/contract.md)
- 隔离与调度语义：[docs/runtime.md](docs/runtime.md)
- 检查点提交与恢复：[docs/checkpointing.md](docs/checkpointing.md)
- 可配置项：[docs/configuration.md](docs/configuration.md)

## 快速开始

```bash
# 演示：两个本地 source 并发写入持久 JSONL 下游，Ctrl-C 后可重跑恢复
go run ./cmd/demo -work /tmp/host-demo

# 常规测试（-v 日志带 [biz:...] 业务标签与 case 名）
go test -v ./...

# 竞态检测（需要 cgo；推荐 CI 使用）
CGO_ENABLED=1 go test -race -count=3 ./...
```

## 测试覆盖的业务场景

`go test -v` 的日志中每个用例都输出 `[biz:<业务域>] case=<用例名>`，覆盖：

- `contract-compatibility`：新增可选/必填、删字段、改类型、可选收紧/放宽；
- `contract-records` / `contract-schema`：缺必填不默认、未知字段拒绝、类型不隐式转换；
- `load-registration`：重复注册、声明缺失/冲突、未知 API 版本、坏 schema、跨重启不兼容演进；
- `load-isolation`：一个连接器 `Open` panic 不影响其它连接器装载；
- `load-compat-v1`：v1 连接器经适配器装载与字段映射；`v1-compat`：Seek 恢复；
- `checkpoint-commit`：正常提交、seq 回退/相等、位置回退、幂等、epoch 围栏、并发串行化；
- `checkpoint-recovery`：损坏检查点明确拒绝；
- `isolation-timeout`：忽略取消的卡死 Read 被看门狗收敛；
- `isolation-panic`：Read 与下游 panic 被恢复并归属连接器；
- `isolation-contract`：坏记录永不进入下游；
- `isolation-backpressure`：下游阻塞时在途批次有界、生产者停住；
- `recovery`：中途停机后从最近已提交位置继续、重放 id 不产生重复效果、读完后再跑为空；
- `multi-connector`：好/卡死/panic 连接器并发互不干扰；宿主取消传导到读取链路。

## 实现一个连接器（v2）

```go
type myConnector struct{}

func (myConnector) Declare() contract.Declaration {
    return contract.Declaration{
        Name: "my-source", ConnectorVersion: "1.0.0",
        APIVersion:   contract.V2,
        Capabilities:  []contract.Capability{contract.CapSource, contract.CapResumable},
        OutputSchema: &contract.Schema{Name: "out", Fields: []contract.Field{
            {Name: "id", Type: contract.TypeString, Required: true},
        }},
    }
}
func (myConnector) Open() error { return nil }
func (myConnector) Close() error { return nil }
func (myConnector) Sink() contract.Sink { return nil }
func (myConnector) Source() contract.Source { return mySource{} }

type mySource struct{}
func (mySource) Read(ctx contract.SourceContext, max int) (contract.Batch, error) { /* ... */ }
func (mySource) Resume(pos []byte) error { /* 从检查点位置恢复 */ ; return nil }

host := runtime.NewHost(runtime.DefaultConfig())
host.Registry().Register(func() (contract.Connector, error) { return myConnector{}, nil })
```
