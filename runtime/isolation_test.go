package runtime

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/contract"
)

func testConfig(dir string) Config {
	c := DefaultConfig()
	c.CheckpointDir = dir + "/ckpt"
	c.SchemaDir = dir + "/schema"
	c.ReadTimeout = 300 * time.Millisecond
	c.CommitTimeout = 300 * time.Millisecond
	c.OpenTimeout = 300 * time.Millisecond
	c.IdlePollInterval = 5 * time.Millisecond
	c.BufferCapacity = 2
	c.BatchSize = 2
	c.DedupeWindow = 64
	return c
}

// rec builds a valid record matching the test connector's schema.
func rec(id string, payload string) contract.Record {
	return contract.Record{ID: id, Values: map[string]any{"id": id, "payload": payload}}
}

func waitFailure(t testing.TB, r *Runner, timeout time.Duration) *Failure {
	t.Helper()
	select {
	case <-r.Done():
	case <-time.After(timeout):
		t.Fatalf("runner %q did not stop in time", r.Name())
	}
	f := r.Failure()
	if f == nil {
		t.Fatalf("runner %q stopped without failure", r.Name())
	}
	return f
}

// controllableSource is a test connector whose Read is fully scripted.
type controllableSource struct {
	decl   contract.Declaration
	readFn func(ctx contract.SourceContext, max int) (contract.Batch, error)
}

func (c *controllableSource) Declare() contract.Declaration { return c.decl }
func (c *controllableSource) Open() error                   { return nil }
func (c *controllableSource) Close() error                  { return nil }
func (c *controllableSource) Source() contract.Source       { return c }
func (c *controllableSource) Sink() contract.Sink           { return nil }

func (c *controllableSource) Read(ctx contract.SourceContext, max int) (contract.Batch, error) {
	return c.readFn(ctx, max)
}

func ctrlConnector(name string, readFn func(ctx contract.SourceContext, max int) (contract.Batch, error)) *controllableSource {
	return &controllableSource{
		readFn: readFn,
		decl: contract.Declaration{
			Name: name, ConnectorVersion: "test", APIVersion: contract.V2,
			Capabilities: []contract.Capability{contract.CapSource},
			OutputSchema: &contract.Schema{Name: "t", Fields: []contract.Field{
				{Name: "id", Type: contract.TypeString, Required: true},
				{Name: "payload", Type: contract.TypeString, Required: false},
			}},
		},
	}
}

type blockingDownstream struct {
	gate chan struct{}
	mu   sync.Mutex
	got  []contract.Record
}

func newBlockingDownstream() *blockingDownstream {
	return &blockingDownstream{gate: make(chan struct{})}
}

func (b *blockingDownstream) Deliver(r contract.Record) error {
	<-b.gate
	b.mu.Lock()
	b.got = append(b.got, r)
	b.mu.Unlock()
	return nil
}

func (b *blockingDownstream) count() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.got)
}

type sliceDownstream struct {
	mu      sync.Mutex
	records []contract.Record
}

func (s *sliceDownstream) Deliver(r contract.Record) error {
	s.mu.Lock()
	s.records = append(s.records, r)
	s.mu.Unlock()
	return nil
}

func (s *sliceDownstream) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

func TestStuckConnectorIsTimedOut(t *testing.T) {
	bizLog(t, "isolation-timeout", "read-ignores-cancel", "Read that blocks forever is contained by watchdog")
	dir := t.TempDir()
	cfg := testConfig(dir)
	conn := ctrlConnector("stuck", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		<-ctx.Done()
		time.Sleep(time.Hour) // ignores cancellation
		return contract.Batch{}, nil
	})
	store, _ := checkpoint.OpenStore(cfg.CheckpointDir, "stuck", nil)
	r := NewRunner(cfg, conn, store, &sliceDownstream{}, 1)
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	f := waitFailure(t, r, 3*time.Second)
	if f.Class != ClassTimeout {
		t.Fatalf("want timeout, got %+v", f)
	}
	if got := r.Stats().Timeouts; got != 1 {
		t.Fatalf("want 1 timeout stat, got %d", got)
	}
}

func TestPanickingConnectorIsContained(t *testing.T) {
	t.Run("panic in read", func(t *testing.T) {
		bizLog(t, "isolation-panic", "panic-read", "panic recovered and attributed to connector")
		dir := t.TempDir()
		cfg := testConfig(dir)
		conn := ctrlConnector("panicker", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
			panic("kaboom")
		})
		store, _ := checkpoint.OpenStore(cfg.CheckpointDir, "panicker", nil)
		r := NewRunner(cfg, conn, store, &sliceDownstream{}, 1)
		_ = r.Start(context.Background())
		f := waitFailure(t, r, 3*time.Second)
		if f.Class != ClassPanic || f.Connector != "panicker" {
			t.Fatalf("want panic attributed to panicker, got %+v", f)
		}
		if r.Stats().Panics != 1 {
			t.Fatalf("panic stat missing: %+v", r.Stats())
		}
	})

	t.Run("panic in downstream", func(t *testing.T) {
		bizLog(t, "isolation-panic", "panic-deliver", "downstream panic also contained")
		dir := t.TempDir()
		cfg := testConfig(dir)
		conn := ctrlConnector("p2", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
			return contract.Batch{Records: []contract.Envelope{{Record: rec("1", "x"), Position: []byte{1}}}, Position: []byte{1}}, nil
		})
		store, _ := checkpoint.OpenStore(cfg.CheckpointDir, "p2", nil)
		panicDown := FuncDownstream(func(contract.Record) error { panic("down boom") })
		r := NewRunner(cfg, conn, store, panicDown, 1)
		_ = r.Start(context.Background())
		f := waitFailure(t, r, 3*time.Second)
		if f.Class != ClassPanic {
			t.Fatalf("want panic, got %+v", f)
		}
	})
}

func TestContractViolationStopsConnector(t *testing.T) {
	bizLog(t, "isolation-contract", "bad-record", "record missing required field never reaches downstream")
	dir := t.TempDir()
	cfg := testConfig(dir)
	conn := ctrlConnector("liar", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		bad := contract.Record{ID: "1", Values: map[string]any{"payload": "no id field"}}
		return contract.Batch{Records: []contract.Envelope{{Record: bad, Position: []byte{1}}}, Position: []byte{1}}, nil
	})
	down := &sliceDownstream{}
	store, _ := checkpoint.OpenStore(cfg.CheckpointDir, "liar", nil)
	r := NewRunner(cfg, conn, store, down, 1)
	_ = r.Start(context.Background())
	f := waitFailure(t, r, 3*time.Second)
	if f.Class != ClassContract {
		t.Fatalf("want contract violation, got %+v", f)
	}
	if down.len() != 0 {
		t.Fatalf("invalid record reached downstream: %d", down.len())
	}
}

func TestBackpressureBoundsMemory(t *testing.T) {
	bizLog(t, "isolation-backpressure", "slow-downstream", "producer stalls; in-flight bounded by BufferCapacity")
	dir := t.TempDir()
	cfg := testConfig(dir)
	cfg.BufferCapacity = 2

	var produced atomic.Int64
	conn := ctrlConnector("fast", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		n := produced.Add(1)
		if n > 1000 {
			<-ctx.Done()
			return contract.Batch{}, ctx.Err()
		}
		batch := contract.Batch{Position: []byte{byte(n)}}
		batch.Records = append(batch.Records, contract.Envelope{Record: rec(fmt.Sprintf("r%d", n), "z"), Position: []byte{byte(n)}})
		return batch, nil
	})
	down := newBlockingDownstream()
	store, _ := checkpoint.OpenStore(cfg.CheckpointDir, "fast", nil)
	r := NewRunner(cfg, conn, store, down, 1)
	_ = r.Start(context.Background())

	time.Sleep(150 * time.Millisecond)
	maxInFlight := r.Stats().BatchesInFlight
	for i := 0; i < 10; i++ {
		time.Sleep(20 * time.Millisecond)
		if s := r.Stats().BatchesInFlight; s > maxInFlight {
			maxInFlight = s
		}
	}
	// queue holds BufferCapacity batches, plus at most one being delivered
	// and one observed mid-enqueue before the stall registers.
	if maxInFlight > int64(cfg.BufferCapacity)+2 {
		t.Fatalf("in-flight batches unbounded: observed %d, bound %d", maxInFlight, cfg.BufferCapacity+2)
	}
	if p := produced.Load(); p > 20 {
		t.Fatalf("producer did not stall under backpressure: produced %d", p)
	}
	close(down.gate)
	r.Stop(time.Second)
}
