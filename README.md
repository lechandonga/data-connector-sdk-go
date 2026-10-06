# data-connector-sdk-go

一套数据集成连接器 SDK 与配套宿主运行时。第三方连接器按统一契约接入，
数据从本地文件或内存读写，仅依赖 Go 标准库，不访问外部服务。

## 目录结构

- `connector/` — 连接器契约：统一接口、声明（Manifest）、记录与检查点类型、拒绝原因
- `host/` — 宿主运行时：注册表与契约校验、模式兼容判定、隔离执行器、检查点存储、并发调度
- `localconn/` — 参考连接器：内存数据源与本地文件数据源
- `docs/` — 契约、运行时语义、检查点规则的详细说明

## 快速开始

```go
reg := host.NewRegistry()
store, _ := host.NewCheckpointStore("./checkpoints")

_ = reg.Register(connector.Manifest{
    ID:           "my-file",
    Interface:    connector.InterfaceVersion{Major: 1, Minor: 0},
    Capabilities: []connector.Capability{connector.CapabilityRead, connector.CapabilityResume},
}, func() connector.Connector {
    return &localconn.FileConnector{Path: "./data.log"}
})

runner := host.NewRunner(reg, store, host.Options{
    BufferSize:  64,
    ReadTimeout: time.Minute,
    CancelGrace: 5 * time.Second,
})
stats, err := runner.Run(ctx, "my-file", host.SinkFunc(func(ctx context.Context, rec connector.Record) error {
    fmt.Println(rec.Seq, rec.Data)
    return nil // 返回 nil 表示确认消费，宿主随即提交检查点
}))
```

## 文档

- [连接器契约与兼容规则](docs/contract.md)
- [运行时隔离与调度语义](docs/runtime.md)
- [检查点提交与恢复规则](docs/checkpoint.md)

## 本地验证

```sh
go build ./...
go test ./...            # 常规测试
go test -race -v ./host  # 竞态检测 + 场景日志（可看到各业务用例覆盖情况）
```

测试日志中以 `场景[业务-用例]` 前缀标注每条用例覆盖的业务点，例如
`场景[隔离-panic收敛]`、`场景[恢复-断点续跑]`、`场景[背压-下游阻塞]`。
