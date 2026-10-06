package host

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// Sink 是下游消费者。Write 返回 nil 表示该记录已被确认消费，
// 宿主只有在收到确认后才会提交对应检查点。
type Sink interface {
	Write(ctx context.Context, rec connector.Record) error
}

// SinkFunc 让普通函数适配 Sink。
type SinkFunc func(ctx context.Context, rec connector.Record) error

func (f SinkFunc) Write(ctx context.Context, rec connector.Record) error { return f(ctx, rec) }

// Options 控制单个连接器的运行行为。
type Options struct {
	// BufferSize 是连接器到下游之间的有界缓冲大小，必须 >= 1。
	// 下游变慢时 emit 阻塞，形成背压，内存占用以 BufferSize 为上界。
	BufferSize int
	// ReadTimeout 是单次运行的总截止时间；超时后宿主取消上下文。
	ReadTimeout time.Duration
	// CancelGrace 是取消后等待连接器自行退出的宽限期；
	// 超过仍未退出则按 KindTimeout 判定失败，连接器 goroutine 被隔离遗弃，
	// 不会阻塞宿主或其它连接器。
	CancelGrace time.Duration
	// Config 传给连接器 Open。
	Config connector.Config
}

func (o *Options) withDefaults() {
	if o.BufferSize <= 0 {
		o.BufferSize = 16
	}
	if o.ReadTimeout <= 0 {
		o.ReadTimeout = 30 * time.Second
	}
	if o.CancelGrace <= 0 {
		o.CancelGrace = 2 * time.Second
	}
}

// Stats 是单次运行的统计，各连接器实例之间完全独立。
type Stats struct {
	ConnectorID string
	RunID       string
	// RecordsProduced 是连接器产出的记录总数（含被去重跳过的）。
	RecordsProduced uint64
	// RecordsDelivered 是被下游确认消费的记录数。
	RecordsDelivered uint64
	// RecordsSkipped 是恢复后因已提交而被去重跳过的记录数。
	RecordsSkipped uint64
	// LastCheckpoint 是本次运行最后提交的检查点。
	LastCheckpoint connector.Checkpoint
}

var runCounter atomic.Uint64

// Runner 以隔离方式运行单个连接器。
type Runner struct {
	registry *Registry
	store    *CheckpointStore
	opts     Options
}

func NewRunner(reg *Registry, store *CheckpointStore, opts Options) *Runner {
	opts.withDefaults()
	return &Runner{registry: reg, store: store, opts: opts}
}

// readResult 是连接器 goroutine 的终态。
type readResult struct {
	err        error
	panicValue any
}

// Run 运行指定连接器直到其 Read 返回、出错、超时或被取消。
// 失败时返回 *RunError（Kind 区分连接器错误/契约违规/超时/panic/宿主动作）
// 或 *CheckpointError（恢复阶段检查点损坏、模式漂移）。
func (r *Runner) Run(ctx context.Context, id string, sink Sink) (*Stats, error) {
	manifest, factory, err := r.registry.Load(id)
	if err != nil {
		return nil, err
	}
	runID := fmt.Sprintf("%s-%d", id, runCounter.Add(1))
	stats := &Stats{ConnectorID: id, RunID: runID}

	resume, err := r.store.Load(id)
	if err != nil {
		return stats, err
	}
	// 恢复一致性：已提交检查点绑定的模式哈希必须与当前声明一致，
	// 不兼容的模式演进在此被明确拒绝，而不是按新语义继续跑。
	if manifest.Schema != nil && resume.SchemaHash != "" && resume.SchemaHash != manifest.Schema.Hash() {
		return stats, &CheckpointError{Reason: ReasonSchemaDrift, ConnectorID: id,
			Detail: fmt.Sprintf("committed checkpoint schema hash %s does not match manifest schema hash %s",
				resume.SchemaHash, manifest.Schema.Hash())}
	}

	runCtx, cancel := context.WithTimeout(ctx, r.opts.ReadTimeout)
	defer cancel()

	// buf 是连接器到消费端的有界缓冲，是背压的唯一通道。
	buf := make(chan connector.Record, r.opts.BufferSize)
	done := make(chan readResult, 1)

	// resumeSeq 是运行开始时已提交的序号：seq <= resumeSeq 的记录视为
	// at-least-once 重放，直接跳过；seq > resumeSeq 的记录必须严格递增，
	// 否则判定为契约违规。
	resumeSeq := resume.Seq
	lastSeen := resume.Seq

	emit := func(rec connector.Record) error {
		select {
		case buf <- rec:
			return nil
		case <-runCtx.Done():
			return runCtx.Err()
		}
	}

	cfg := r.opts.Config
	cfg.ConnectorID = id
	instance := factory()
	go func() {
		defer func() {
			if pv := recover(); pv != nil {
				done <- readResult{panicValue: pv}
			}
		}()
		if err := instance.Open(runCtx, cfg); err != nil {
			done <- readResult{err: err}
			return
		}
		done <- readResult{err: instance.Read(runCtx, resume, emit)}
	}()

	schemaHash := ""
	if manifest.Schema != nil {
		schemaHash = manifest.Schema.Hash()
	}

	consume := func(rec connector.Record) error {
		stats.RecordsProduced++
		if rec.Seq > resumeSeq && rec.Seq <= lastSeen {
			return &RunError{Kind: KindContract, ConnectorID: id, RunID: runID,
				Err: fmt.Errorf("record seq %d is not monotonically increasing (last %d)", rec.Seq, lastSeen)}
		}
		// 断点恢复去重：已提交序号之前的重复记录不得再次投递。
		if rec.Seq <= resumeSeq {
			stats.RecordsSkipped++
			return nil
		}
		lastSeen = rec.Seq
		if err := validateRecord(manifest, rec); err != nil {
			return &RunError{Kind: KindContract, ConnectorID: id, RunID: runID, Err: err}
		}
		if err := sink.Write(runCtx, rec); err != nil {
			return &RunError{Kind: KindHost, ConnectorID: id, RunID: runID,
				Err: fmt.Errorf("sink rejected record %d: %w", rec.Seq, err)}
		}
		cp := connector.Checkpoint{Seq: rec.Seq, Offset: rec.Offset, SchemaHash: schemaHash}
		if err := r.store.Commit(id, cp); err != nil {
			return &RunError{Kind: KindHost, ConnectorID: id, RunID: runID, Err: err}
		}
		stats.LastCheckpoint = cp
		stats.RecordsDelivered++
		return nil
	}

	var result readResult
	haveResult := false
	// abort 在消费侧失败时取消运行并等待连接器 goroutine 收敛（含宽限期兜底）。
	abort := func(cause error) (*Stats, error) {
		cancel()
		select {
		case <-done:
		case <-time.After(r.opts.CancelGrace):
		}
		return stats, joinErr(cause, instance.Close(), id, runID)
	}

	for !haveResult {
		select {
		case rec := <-buf:
			if err := consume(rec); err != nil {
				return abort(err)
			}
		case result = <-done:
			haveResult = true
		case <-runCtx.Done():
			// 截止时间到或外部取消：给连接器一个宽限期自行退出，
			// 仍不退出则隔离遗弃该 goroutine，宿主按确定语义返回。
			select {
			case result = <-done:
				haveResult = true
			case <-time.After(r.opts.CancelGrace):
				if runCtx.Err() == context.DeadlineExceeded {
					return stats, &RunError{Kind: KindTimeout, ConnectorID: id, RunID: runID,
						Err: fmt.Errorf("connector did not finish within %s and ignored cancellation for %s",
							r.opts.ReadTimeout, r.opts.CancelGrace)}
				}
				return stats, &RunError{Kind: KindHost, ConnectorID: id, RunID: runID,
					Err: fmt.Errorf("cancelled and connector ignored cancellation: %w", runCtx.Err())}
			}
		}
	}
	// 连接器已返回，排空缓冲中残留的记录。
	for drained := false; !drained; {
		select {
		case rec := <-buf:
			if err := consume(rec); err != nil {
				return abort(err)
			}
		default:
			drained = true
		}
	}

	closeErr := instance.Close()

	switch {
	case result.panicValue != nil:
		return stats, &RunError{Kind: KindPanic, ConnectorID: id, RunID: runID, PanicValue: result.panicValue}
	case result.err != nil:
		if errors.Is(result.err, context.DeadlineExceeded) && runCtx.Err() == context.DeadlineExceeded {
			return stats, &RunError{Kind: KindTimeout, ConnectorID: id, RunID: runID, Err: result.err}
		}
		if errors.Is(result.err, context.Canceled) && ctx.Err() != nil {
			return stats, &RunError{Kind: KindHost, ConnectorID: id, RunID: runID, Err: result.err}
		}
		return stats, &RunError{Kind: KindConnector, ConnectorID: id, RunID: runID, Err: result.err}
	}
	// 连接器对取消无响应时的兜底：Read 已返回的情形不会走到这里；
	// 未返回的情形由下方 select 在宽限期后判定超时。
	if runCtx.Err() == context.DeadlineExceeded {
		return stats, &RunError{Kind: KindTimeout, ConnectorID: id, RunID: runID, Err: runCtx.Err()}
	}
	if err := closeErr; err != nil {
		return stats, &RunError{Kind: KindConnector, ConnectorID: id, RunID: runID, Err: fmt.Errorf("close: %w", err)}
	}
	return stats, nil
}

func validateRecord(m connector.Manifest, rec connector.Record) error {
	if m.Schema == nil {
		return nil
	}
	for _, f := range m.Schema.Fields {
		if f.Required {
			if _, ok := rec.Data[f.Name]; !ok {
				return fmt.Errorf("record %d missing required field %q", rec.Seq, f.Name)
			}
		}
	}
	return nil
}

func joinErr(err, closeErr error, id, runID string) error {
	if closeErr == nil {
		return err
	}
	return errors.Join(err, &RunError{Kind: KindConnector, ConnectorID: id, RunID: runID,
		Err: fmt.Errorf("close: %w", closeErr)})
}
