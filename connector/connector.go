package connector

import "context"

// Connector 是第三方连接器必须实现的统一接口（契约接口 v1.x）。
//
// 调用顺序由宿主保证：Open -> Read（可能多次，含恢复重跑）-> Close。
// Read 内通过 emit 回调产出记录；emit 在下游背压时会阻塞，
// 连接器应将其视为流控信号，并在 ctx 取消时尽快返回。
type Connector interface {
	// Open 初始化连接器。返回错误会使本次运行以连接器错误终止。
	Open(ctx context.Context, cfg Config) error
	// Read 从 resume 指定的位置开始产出记录。
	// resume 为零值 Checkpoint 表示从头开始。
	// emit 返回错误（通常是取消或下游关闭）时 Read 应停止并返回该错误。
	Read(ctx context.Context, resume Checkpoint, emit func(Record) error) error
	// Close 释放资源。宿主保证在 Read 返回后调用，且只调用一次。
	Close() error
}
