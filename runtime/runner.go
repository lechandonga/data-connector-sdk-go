package runtime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// Stats are per-connector counters, isolated from every other connector.
type Stats struct {
	Reads            atomic.Int64
	RecordsRead      atomic.Int64
	RecordsDelivered atomic.Int64
	RecordsDeduped   atomic.Int64
	BatchesInFlight  atomic.Int64
	Commits          atomic.Int64
	Timeouts         atomic.Int64
	Panics           atomic.Int64
	ContractErrors   atomic.Int64
}

// Snapshot is a consistent point-in-time copy of Stats.
type Snapshot struct {
	Reads            int64 `json:"reads"`
	RecordsRead      int64 `json:"recordsRead"`
	RecordsDelivered int64 `json:"recordsDelivered"`
	RecordsDeduped   int64 `json:"recordsDeduped"`
	BatchesInFlight  int64 `json:"batchesInFlight"`
	Commits          int64 `json:"commits"`
	Timeouts         int64 `json:"timeouts"`
	Panics           int64 `json:"panics"`
	ContractErrors   int64 `json:"contractErrors"`
}

func (s *Stats) snapshot() Snapshot {
	return Snapshot{
		Reads:            s.Reads.Load(),
		RecordsRead:      s.RecordsRead.Load(),
		RecordsDelivered: s.RecordsDelivered.Load(),
		RecordsDeduped:   s.RecordsDeduped.Load(),
		BatchesInFlight:  s.BatchesInFlight.Load(),
		Commits:          s.Commits.Load(),
		Timeouts:         s.Timeouts.Load(),
		Panics:           s.Panics.Load(),
		ContractErrors:   s.ContractErrors.Load(),
	}
}

// ResumableSource is implemented by sources that can restart from a
// checkpoint position. Built-in sources do; a V1 source does only through
// its adapter Seek hook.
type ResumableSource interface {
	Resume(position []byte) error
}

type queuedBatch struct {
	batch contract.Batch
	kept  []contract.Envelope
}

// Runner owns the full lifecycle of one connector instance.
type Runner struct {
	name   string
	cfg    Config
	conn   contract.Connector
	source contract.Source
	down   Downstream
	store  *checkpoint.Store

	stats  Stats
	dedupe *deduper

	cancel   context.CancelFunc
	done     chan struct{}
	halt     chan struct{}
	stopOnce sync.Once

	mu      sync.Mutex
	failure *Failure

	epoch uint64
	seq   uint64
}

// NewRunner wires one source connector. down may be nil for sink-only
// connectors (which are not pumped by this runtime).
func NewRunner(cfg Config, conn contract.Connector, store *checkpoint.Store, down Downstream, epoch uint64) *Runner {
	return &Runner{
		name:   conn.Declare().Name,
		cfg:    cfg,
		conn:   conn,
		source: conn.Source(),
		down:   down,
		store:  store,
		dedupe: newDeduper(cfg.DedupeWindow),
		done:   make(chan struct{}),
		halt:   make(chan struct{}),
		epoch:  epoch,
	}
}

// Name returns the connector name.
func (r *Runner) Name() string { return r.name }

// Done is closed when the runner has stopped.
func (r *Runner) Done() <-chan struct{} { return r.done }

// Failure returns the terminal failure, if any.
func (r *Runner) Failure() *Failure {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.failure
}

// Stats returns a snapshot.
func (r *Runner) Stats() Snapshot { return r.stats.snapshot() }

func (r *Runner) failIfUnset(f *Failure) *Failure {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failure == nil {
		r.failure = f
	}
	r.stopOnce.Do(func() { close(r.halt) })
	return r.failure
}

// Start launches the connector goroutine. Recovery from the last committed
// position happens before the first read.
func (r *Runner) Start(parent context.Context) error {
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel

	if r.source != nil {
		if err := r.resumeFromCheckpoint(); err != nil {
			cancel()
			close(r.done)
			return err
		}
		go r.runSource(ctx)
		return nil
	}

	cancel()
	close(r.done)
	return nil
}

func (r *Runner) resumeFromCheckpoint() error {
	st, err := r.store.Current()
	if errors.Is(err, checkpoint.ErrNothing) {
		return nil
	}
	if err != nil {
		return r.failIfUnset(&Failure{Connector: r.name, Class: ClassHost, Op: "restore", Detail: err.Error(), Err: err})
	}
	pos, err := checkpoint.DecodePosition(st.Position)
	if err != nil {
		return r.failIfUnset(&Failure{Connector: r.name, Class: ClassHost, Op: "restore", Detail: err.Error(), Err: err})
	}
	if len(pos) > 0 {
		rs, ok := r.source.(ResumableSource)
		if !ok {
			return r.failIfUnset(&Failure{Connector: r.name, Class: ClassContract, Op: "restore", Detail: "checkpoint exists but source is not resumable"})
		}
		if gerr := guard(func() error { return rs.Resume(pos) }, r.cfg.OpenTimeout); gerr != nil {
			return r.failIfUnset(classify(gerr, r.name, "resume", &r.stats))
		}
	}
	r.seq = st.Seq
	return nil
}

// Stop cancels the run and waits up to grace for the goroutine to drain. A
// connector ignoring cancellation is bounded by the watchdog in every
// guarded call; Stop returning early does not crash or affect anyone else.
func (r *Runner) Stop(grace time.Duration) {
	if r.cancel != nil {
		r.cancel()
	}
	if grace <= 0 {
		grace = r.cfg.ReadTimeout + r.cfg.CommitTimeout + time.Second
	}
	select {
	case <-r.done:
	case <-time.After(grace):
	}
}

func (r *Runner) runSource(ctx context.Context) {
	defer close(r.done)
	defer func() { _ = guard(r.conn.Close, r.cfg.CommitTimeout) }()

	queue := make(chan queuedBatch, r.cfg.BufferCapacity)

	var consumerWG sync.WaitGroup
	consumerWG.Add(1)
	go func() {
		defer consumerWG.Done()
		r.consume(ctx, queue)
	}()

	// On producer exit the queue is closed and the consumer drains all
	// batches still in flight: their records were already taken from the
	// source, so they must be delivered and committed, or recovery would
	// replay them (which is also safe, but draining avoids duplicate work
	// and makes cancellation deterministic in tests).
	defer func() {
		close(queue)
		consumerWG.Wait()
	}()

	for ctx.Err() == nil {
		select {
		case <-r.halt:
			return
		default:
		}
		batch, ferr := r.readOnce(ctx)
		if ferr != nil {
			if errors.Is(ferr.Err, io.EOF) {
				r.stopOnce.Do(func() { close(r.halt) })
				return
			}
			r.failIfUnset(ferr)
			return
		}
		r.stats.Reads.Add(1)
		if len(batch.Records) == 0 {
			select {
			case <-ctx.Done():
				return
			case <-r.halt:
				return
			case <-time.After(r.cfg.IdlePollInterval):
			}
			continue
		}
		r.stats.RecordsRead.Add(int64(len(batch.Records)))

		// Contract-validate every record against the declared output schema
		// BEFORE anything downstream can see it.
		schema := *r.conn.Declare().OutputSchema
		kept := make([]contract.Envelope, 0, len(batch.Records))
		bad := false
		for _, env := range batch.Records {
			if violations := contract.ValidateRecord(schema, env.Record); len(violations) > 0 {
				r.stats.ContractErrors.Add(1)
				r.failIfUnset(&Failure{Connector: r.name, Class: ClassContract, Op: "read", Detail: violations[0].Error()})
				bad = true
				break
			}
			env.Record = contract.NormalizeRecord(schema, env.Record)
			kept = append(kept, env)
		}
		if bad {
			return
		}
		if len(kept) == 0 {
			continue
		}

		// Bounded enqueue: a full queue blocks the read loop. A slow
		// downstream therefore applies back-pressure all the way to the
		// connector; memory use is bounded by BufferCapacity batches.
		r.stats.BatchesInFlight.Add(1)
		select {
		case queue <- queuedBatch{batch: batch, kept: kept}:
		case <-ctx.Done():
			r.stats.BatchesInFlight.Add(-1)
			return
		case <-r.halt:
			r.stats.BatchesInFlight.Add(-1)
			return
		}
	}
}

func (r *Runner) readOnce(ctx context.Context) (contract.Batch, *Failure) {
	type result struct {
		batch contract.Batch
		err   error
	}
	resCh := make(chan result, 1)
	sc := &ctxSource{ctx: ctx}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				resCh <- result{err: panicErr(rec)}
			}
		}()
		b, err := r.source.Read(sc, r.cfg.BatchSize)
		resCh <- result{batch: b, err: err}
	}()
	select {
	case res := <-resCh:
		if res.err != nil {
			if errors.Is(res.err, io.EOF) {
				return contract.Batch{}, &Failure{Connector: r.name, Class: ClassConnector, Op: "read", Detail: io.EOF.Error(), Err: io.EOF}
			}
			return contract.Batch{}, classify(res.err, r.name, "read", &r.stats)
		}
		return res.batch, nil
	case <-time.After(r.cfg.ReadTimeout):
		r.stats.Timeouts.Add(1)
		return contract.Batch{}, &Failure{Connector: r.name, Class: ClassTimeout, Op: "read", Detail: fmt.Sprintf("Read did not return within %s", r.cfg.ReadTimeout), Err: ErrTimeout}
	case <-ctx.Done():
		return contract.Batch{}, &Failure{Connector: r.name, Class: ClassCancelled, Op: "read", Detail: ctx.Err().Error(), Err: ErrCancelled}
	}
}

func (r *Runner) consume(ctx context.Context, queue <-chan queuedBatch) {
	// Drain until the producer closes the queue. A bounded drain context
	// prevents a hung downstream from blocking shutdown forever: after
	// cancellation, CommitTimeout still bounds each remaining call.
	drainCtx, cancelDrain := context.WithCancel(ctx)
	defer cancelDrain()
	go func() {
		select {
		case <-r.halt:
			// Hard failure (panic/timeout/contract): do not block producer
			// shutdown indefinitely, but still try to finish queued work; the
			// runner failure is already recorded and takes precedence.
		case <-ctx.Done():
		}
	}()
	for qb := range queue {
		r.deliverAndCommit(drainCtx, qb)
	}
}

func (r *Runner) deliverAndCommit(ctx context.Context, qb queuedBatch) {
	defer r.stats.BatchesInFlight.Add(-1)

	for _, env := range qb.kept {
		if r.dedupe.beginDelivery(env.Record.ID) {
			r.stats.RecordsDeduped.Add(1)
			continue
		}
		if ferr := r.deliverOne(ctx, env.Record); ferr != nil {
			r.failIfUnset(ferr)
			return
		}
		r.stats.RecordsDelivered.Add(1)
	}

	// Every record of the batch is now accepted by the downstream. Commit
	// the batch position exactly once. On restart the source replays from
	// the previous position; dedupe suppresses double effects.
	if sd, ok := r.down.(*SinkDownstream); ok {
		if ferr := r.sinkWrite(ctx, sd); ferr != nil {
			r.failIfUnset(ferr)
			return
		}
		if ferr := r.sinkCommit(ctx, sd.Sink()); ferr != nil {
			r.failIfUnset(ferr)
			return
		}
	}

	r.seq++
	next := checkpoint.State{
		Connector: r.name,
		Seq:       r.seq,
		Epoch:     r.epoch,
		Position:  checkpoint.EncodePosition(qb.batch.Position),
	}
	if len(qb.kept) > 0 {
		next.RecordID = qb.kept[len(qb.kept)-1].Record.ID
	}
	if _, err := r.store.Commit(next); err != nil {
		r.failIfUnset(&Failure{Connector: r.name, Class: ClassHost, Op: "checkpoint-commit", Detail: err.Error(), Err: err})
		return
	}
	ids := make([]string, 0, len(qb.kept))
	for _, env := range qb.kept {
		ids = append(ids, env.Record.ID)
	}
	r.dedupe.commit(ids)
	r.stats.Commits.Add(1)
}

func (r *Runner) deliverOne(ctx context.Context, rec contract.Record) *Failure {
	resCh := make(chan error, 1)
	go func() {
		defer func() {
			if v := recover(); v != nil {
				select {
				case resCh <- panicErr(v):
				default:
				}
			}
		}()
		resCh <- r.down.Deliver(rec)
	}()
	select {
	case err := <-resCh:
		if err != nil {
			return classify(err, r.name, "deliver", &r.stats)
		}
		return nil
	case <-time.After(r.cfg.CommitTimeout):
		r.stats.Timeouts.Add(1)
		return &Failure{Connector: r.name, Class: ClassTimeout, Op: "deliver", Detail: "downstream did not accept record within timeout", Err: ErrTimeout}
	case <-ctx.Done():
		return &Failure{Connector: r.name, Class: ClassCancelled, Op: "deliver", Detail: ctx.Err().Error(), Err: ErrCancelled}
	}
}

func (r *Runner) sinkWrite(ctx context.Context, sd *SinkDownstream) *Failure {
	resCh := make(chan error, 1)
	sc := &ctxSink{ctx: ctx}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				resCh <- panicErr(rec)
			}
		}()
		resCh <- sd.flush(sc)
	}()
	select {
	case err := <-resCh:
		if err != nil {
			return classify(err, r.name, "sink-write", &r.stats)
		}
		return nil
	case <-time.After(r.cfg.CommitTimeout):
		r.stats.Timeouts.Add(1)
		return &Failure{Connector: r.name, Class: ClassTimeout, Op: "sink-write", Detail: "sink Write did not return within timeout", Err: ErrTimeout}
	case <-ctx.Done():
		return &Failure{Connector: r.name, Class: ClassCancelled, Op: "sink-write", Detail: ctx.Err().Error(), Err: ErrCancelled}
	}
}

func (r *Runner) sinkCommit(ctx context.Context, sink contract.Sink) *Failure {
	resCh := make(chan error, 1)
	sc := &ctxSink{ctx: ctx}
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				resCh <- panicErr(rec)
			}
		}()
		resCh <- sink.Commit(sc)
	}()
	select {
	case err := <-resCh:
		if err != nil {
			return classify(err, r.name, "sink-commit", &r.stats)
		}
		return nil
	case <-time.After(r.cfg.CommitTimeout):
		r.stats.Timeouts.Add(1)
		return &Failure{Connector: r.name, Class: ClassTimeout, Op: "sink-commit", Detail: "sink Commit did not return within timeout", Err: ErrTimeout}
	case <-ctx.Done():
		return &Failure{Connector: r.name, Class: ClassCancelled, Op: "sink-commit", Detail: ctx.Err().Error(), Err: ErrCancelled}
	}
}
