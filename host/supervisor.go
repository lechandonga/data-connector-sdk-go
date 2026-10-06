package host

import (
	"context"
	"sync"
)

// Result 是一次连接器运行的结果汇总。
type Result struct {
	ConnectorID string
	Stats       *Stats
	Err         error
}

// Supervisor 并发运行多个连接器。每个连接器有独立的实例、缓冲、
// 检查点条目与统计，任一连接器的失败（含 panic）不影响其它连接器。
type Supervisor struct {
	registry *Registry
	store    *CheckpointStore
	opts     Options
}

func NewSupervisor(reg *Registry, store *CheckpointStore, opts Options) *Supervisor {
	opts.withDefaults()
	return &Supervisor{registry: reg, store: store, opts: opts}
}

// RunAll 并发运行 ids 中的全部连接器，为每个连接器分配独立的 Sink
// （通过 sinkFor 按连接器 ID 构造）。返回与 ids 一一对应的结果。
func (s *Supervisor) RunAll(ctx context.Context, ids []string, sinkFor func(id string) Sink) []Result {
	results := make([]Result, len(ids))
	var wg sync.WaitGroup
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			runner := NewRunner(s.registry, s.store, s.opts)
			stats, err := runner.Run(ctx, id, sinkFor(id))
			results[i] = Result{ConnectorID: id, Stats: stats, Err: err}
		}(i, id)
	}
	wg.Wait()
	return results
}
