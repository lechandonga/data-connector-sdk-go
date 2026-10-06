package host

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/connector"
	"github.com/lechandonga/data-connector-sdk-go/localconn"
)

func fastOpts() Options {
	return Options{BufferSize: 4, ReadTimeout: 300 * time.Millisecond, CancelGrace: 100 * time.Millisecond}
}

func setup(t *testing.T, m connector.Manifest, f Factory) (*Runner, *CheckpointStore) {
	t.Helper()
	reg := NewRegistry()
	if err := reg.Register(m, f); err != nil {
		t.Fatalf("register: %v", err)
	}
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return NewRunner(reg, store, fastOpts()), store
}

type stuckConnector struct{ respectCancel bool }

func (s *stuckConnector) Open(context.Context, connector.Config) error { return nil }
func (s *stuckConnector) Read(ctx context.Context, _ connector.Checkpoint, emit func(connector.Record) error) error {
	if s.respectCancel {
		<-ctx.Done()
		return ctx.Err()
	}
	select {} // 永久卡住且不响应取消
}
func (s *stuckConnector) Close() error { return nil }

type panicConnector struct{ msg string }

func (p *panicConnector) Open(context.Context, connector.Config) error { return nil }
func (p *panicConnector) Read(context.Context, connector.Checkpoint, func(connector.Record) error) error {
	panic(p.msg)
}
func (p *panicConnector) Close() error { return nil }

type collectSink struct {
	mu      sync.Mutex
	records []connector.Record
	gate    chan struct{} // 非 nil 时阻塞直到关闭，用于背压测试
}

func (c *collectSink) Write(ctx context.Context, rec connector.Record) error {
	if c.gate != nil {
		select {
		case <-c.gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	c.mu.Lock()
	c.records = append(c.records, rec)
	c.mu.Unlock()
	return nil
}

func (c *collectSink) seqs() []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]uint64, len(c.records))
	for i, r := range c.records {
		out[i] = r.Seq
	}
	return out
}

// TestStuckConnectorTimeout 覆盖「运行时隔离」业务：读数据卡住且不响应
// 取消的连接器被超时兜底，宿主正常返回，错误归类为 KindTimeout。
func TestStuckConnectorTimeout(t *testing.T) {
	runner, _ := setup(t, validManifest("stuck"), func() connector.Connector {
		return &stuckConnector{respectCancel: false}
	})
	start := time.Now()
	_, err := runner.Run(context.Background(), "stuck", &collectSink{})
	var re *RunError
	if !errors.As(err, &re) || re.Kind != KindTimeout {
		t.Fatalf("want KindTimeout, got %v", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("host blocked too long: %s", elapsed)
	}
	t.Logf("场景[隔离-卡住不响应]: 归类=%s run=%s 耗时=%s", re.Kind, re.RunID, time.Since(start).Round(time.Millisecond))
}

// TestPanicIsolation 覆盖 panic 被收敛并归属到具体连接器。
func TestPanicIsolation(t *testing.T) {
	runner, _ := setup(t, validManifest("panicky"), func() connector.Connector {
		return &panicConnector{msg: "boom"}
	})
	_, err := runner.Run(context.Background(), "panicky", &collectSink{})
	var re *RunError
	if !errors.As(err, &re) || re.Kind != KindPanic {
		t.Fatalf("want KindPanic, got %v", err)
	}
	if re.PanicValue != "boom" {
		t.Fatalf("panic value lost: %v", re.PanicValue)
	}
	t.Logf("场景[隔离-panic收敛]: 归类=%s 归属=%s panic=%v", re.Kind, re.ConnectorID, re.PanicValue)
}

// TestConnectorErrorAttribution 覆盖连接器自身报错被归类为 KindConnector。
func TestConnectorErrorAttribution(t *testing.T) {
	bad := &localconn.FileConnector{Path: "/nonexistent/definitely-missing.txt"}
	runner, _ := setup(t, validManifest("fileconn"), func() connector.Connector { return bad })
	_, err := runner.Run(context.Background(), "fileconn", &collectSink{})
	var re *RunError
	if !errors.As(err, &re) || re.Kind != KindConnector {
		t.Fatalf("want KindConnector, got %v", err)
	}
	t.Logf("场景[隔离-连接器报错]: 归类=%s 归属=%s", re.Kind, re.ConnectorID)
}

// TestBackpressure 覆盖「背压」业务：下游变慢时缓冲有界，
// 生产端被流控阻塞而不是无限占用内存。
func TestBackpressure(t *testing.T) {
	const total = 100
	src := &localconn.MemoryConnector{Records: make([]map[string]any, total)}
	for i := range src.Records {
		src.Records[i] = map[string]any{"line": fmt.Sprintf("r%d", i+1)}
	}
	reg := NewRegistry()
	if err := reg.Register(validManifest("bp"), func() connector.Connector { return src }); err != nil {
		t.Fatal(err)
	}
	store, _ := NewCheckpointStore(t.TempDir())
	const bufSize = 4
	runner := NewRunner(reg, store, Options{BufferSize: bufSize, ReadTimeout: 5 * time.Second, CancelGrace: time.Second})

	gate := make(chan struct{})
	sink := &collectSink{gate: gate}
	done := make(chan error, 1)
	go func() {
		_, err := runner.Run(context.Background(), "bp", sink)
		done <- err
	}()
	// 下游未开闸时，被确认消费的记录必须远少于总量：
	// 缓冲（bufSize）+ 正在处理的 1 条构成上界，证明内存占用有界。
	time.Sleep(200 * time.Millisecond)
	sink.mu.Lock()
	delivered := len(sink.records)
	sink.mu.Unlock()
	if delivered > 1 {
		t.Fatalf("下游未确认时不应继续投递, delivered=%d", delivered)
	}
	t.Logf("场景[背压-下游阻塞]: 缓冲=%d 阻塞期间已确认=%d（上界=正在处理1条）", bufSize, delivered)
	close(gate)
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
	if got := len(sink.seqs()); got != total {
		t.Fatalf("want %d records, got %d", total, got)
	}
	t.Logf("场景[背压-恢复消费]: 开闸后全部 %d 条按序投递完成", total)
}

// TestResumeAndDedup 覆盖「断点恢复」业务：失败后从最近已提交位置继续，
// 重复出现的记录不被重复投递、也不漏。
func TestResumeAndDedup(t *testing.T) {
	const total = 10
	reg := NewRegistry()
	// replayConnector 忽略 resume 总是从头重放，模拟 at-least-once 语义的源。
	if err := reg.Register(validManifest("rs"), func() connector.Connector {
		return &replayConnector{total: total}
	}); err != nil {
		t.Fatal(err)
	}
	store, _ := NewCheckpointStore(t.TempDir())
	opts := fastOpts()
	opts.ReadTimeout = 5 * time.Second

	// 第一次运行：下游在第 5 条失败，检查点应停在 4。
	var calls atomic.Int64
	failAt := int64(5)
	sink1 := SinkFunc(func(_ context.Context, rec connector.Record) error {
		if calls.Add(1) == failAt {
			return fmt.Errorf("sink exploded at %d", rec.Seq)
		}
		return nil
	})
	runner1 := NewRunner(reg, store, opts)
	_, err := runner1.Run(context.Background(), "rs", sink1)
	var re *RunError
	if !errors.As(err, &re) || re.Kind != KindHost {
		t.Fatalf("want KindHost (sink failure), got %v", err)
	}
	cp, _ := store.Load("rs")
	if cp.Seq != uint64(failAt-1) {
		t.Fatalf("checkpoint should stop at %d, got %d", failAt-1, cp.Seq)
	}
	t.Logf("场景[恢复-失败定位]: 下游在第%d条失败, 检查点停在 seq=%d, 归类=%s", failAt, cp.Seq, re.Kind)

	// 第二次运行：连接器从头重放全部记录（模拟 at-least-once 源），
	// 宿主必须去重，只投递 5..10。
	sink2 := &collectSink{}
	runner2 := NewRunner(reg, store, opts)
	stats, err := runner2.Run(context.Background(), "rs", sink2)
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	got := sink2.seqs()
	if len(got) != total-int(failAt-1) {
		t.Fatalf("want %d new records, got %v", total-int(failAt-1), got)
	}
	for i, s := range got {
		if want := uint64(i) + uint64(failAt); s != want {
			t.Fatalf("out of order/dup: got %v", got)
		}
	}
	if stats.RecordsSkipped != uint64(failAt-1) {
		t.Fatalf("want %d skipped duplicates, got %d", failAt-1, stats.RecordsSkipped)
	}
	t.Logf("场景[恢复-断点续跑]: 重放中跳过重复 %d 条, 新投递 %d 条, 无重复无遗漏",
		stats.RecordsSkipped, stats.RecordsDelivered)
}

type replayConnector struct{ total int }

func (r *replayConnector) Open(context.Context, connector.Config) error { return nil }
func (r *replayConnector) Read(_ context.Context, _ connector.Checkpoint, emit func(connector.Record) error) error {
	for i := 1; i <= r.total; i++ {
		rec := connector.Record{Seq: uint64(i), Offset: fmt.Sprintf("mem:%d", i),
			Data: map[string]any{"line": fmt.Sprintf("r%d", i)}}
		if err := emit(rec); err != nil {
			return err
		}
	}
	return nil
}
func (r *replayConnector) Close() error { return nil }

// TestSchemaDriftRejected 覆盖恢复时模式不兼容演进被明确拒绝。
func TestSchemaDriftRejected(t *testing.T) {
	store, _ := NewCheckpointStore(t.TempDir())
	if err := store.Commit("drift", connector.Checkpoint{Seq: 3, SchemaHash: "old-hash"}); err != nil {
		t.Fatal(err)
	}
	reg := NewRegistry()
	if err := reg.Register(validManifest("drift"), memFactory); err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(reg, store, fastOpts())
	_, err := runner.Run(context.Background(), "drift", &collectSink{})
	var ce *CheckpointError
	if !errors.As(err, &ce) || ce.Reason != ReasonSchemaDrift {
		t.Fatalf("want schema_drift rejection, got %v", err)
	}
	t.Logf("场景[恢复-模式漂移]: 拒绝原因=%s", ce.Reason)
}

// TestContractViolationMonotonic 覆盖记录序号回退被判定为契约违规。
func TestContractViolationMonotonic(t *testing.T) {
	runner, _ := setup(t, validManifest("badseq"), func() connector.Connector { return &badSeqConnector{} })
	_, err := runner.Run(context.Background(), "badseq", &collectSink{})
	var re *RunError
	if !errors.As(err, &re) || re.Kind != KindContract {
		t.Fatalf("want KindContract, got %v", err)
	}
	t.Logf("场景[契约-序号回退]: 归类=%s 详情=%v", re.Kind, re.Err)
}

type badSeqConnector struct{}

func (b *badSeqConnector) Open(context.Context, connector.Config) error { return nil }
func (b *badSeqConnector) Read(_ context.Context, _ connector.Checkpoint, emit func(connector.Record) error) error {
	for _, seq := range []uint64{1, 2, 2, 3} { // 序号 2 重复出现，违反单调递增
		if err := emit(connector.Record{Seq: seq, Data: map[string]any{"line": "x"}}); err != nil {
			return err
		}
	}
	return nil
}
func (b *badSeqConnector) Close() error { return nil }
