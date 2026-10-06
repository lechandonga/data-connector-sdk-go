package runtime

import (
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// Downstream is the consumer side for records emitted by a source
// connector. Implementations must be safe for sequential use; the runtime
// never calls Deliver concurrently for the same connector.
//
// Deliver MUST block while the downstream is busy: that blocking is the
// back-pressure signal. The runtime calls it on a cancellable channel
// boundary, so a hung downstream unblocks on shutdown.
type Downstream interface {
	Deliver(rec contract.Record) error
}

// FuncDownstream adapts a function to Downstream.
type FuncDownstream func(contract.Record) error

func (f FuncDownstream) Deliver(rec contract.Record) error { return f(rec) }

// ChannelDownstream delivers records into a user-provided channel. A slow
// receiver therefore exerts natural back-pressure through the bounded queue
// in front of this downstream.
type ChannelDownstream struct {
	ch chan<- contract.Record
}

// NewChannelDownstream wraps ch.
func NewChannelDownstream(ch chan<- contract.Record) *ChannelDownstream {
	return &ChannelDownstream{ch: ch}
}

func (c *ChannelDownstream) Deliver(rec contract.Record) error {
	c.ch <- rec
	return nil
}

// SinkDownstream pushes records into a V2 sink connector; the runtime is
// responsible for Write/Commit timeout and checkpoint sequencing.
type SinkDownstream struct {
	sink    contract.Sink
	mu      sync.Mutex
	pending []contract.Envelope
}

// NewSinkDownstream wraps a sink.
func NewSinkDownstream(sink contract.Sink) *SinkDownstream {
	return &SinkDownstream{sink: sink}
}

func (s *SinkDownstream) Sink() contract.Sink { return s.sink }

// Deliver stages one record; the staged batch is written to the sink once on
// Commit via flush, preserving one Write + one Commit per source batch.
func (s *SinkDownstream) Deliver(rec contract.Record) error {
	s.mu.Lock()
	s.pending = append(s.pending, contract.Envelope{Record: rec})
	s.mu.Unlock()
	return nil
}

// flush writes staged records to the sink and clears the staging buffer. It
// is invoked by the runner immediately before sink.Commit.
func (s *SinkDownstream) flush(ctx contract.SinkContext) error {
	s.mu.Lock()
	batch := contract.Batch{Records: s.pending}
	s.pending = nil
	s.mu.Unlock()
	if len(batch.Records) == 0 {
		return nil
	}
	return s.sink.Write(ctx, batch)
}

// deduper is a bounded recently-seen set used to suppress replayed record
// ids after recovery while their commit may already have reached the
// downstream. Ring eviction keeps memory bounded; ids survive until evicted.
type deduper struct {
	mu        sync.Mutex
	committed map[string]struct{}
	commOrder []string
	pending   map[string]struct{}
	cap       int
}

func newDeduper(capacity int) *deduper {
	if capacity < 1 {
		capacity = 1
	}
	return &deduper{
		committed: map[string]struct{}{},
		commOrder: make([]string, 0, capacity),
		pending:   map[string]struct{}{},
		cap:       capacity,
	}
}

// beginDelivery returns true if id is already committed (or already in
// flight in THIS generation). Otherwise the id becomes pending: delivered
// but not yet checkpoint-committed, so a replay after crash is allowed to
// deliver it again.
func (d *deduper) beginDelivery(id string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.committed[id]; ok {
		return true
	}
	if _, ok := d.pending[id]; ok {
		return true
	}
	d.pending[id] = struct{}{}
	return false
}

// commit moves ids of a delivered-and-committed batch into the committed
// window (bounded) and clears them from pending.
func (d *deduper) commit(ids []string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, id := range ids {
		delete(d.pending, id)
		if _, ok := d.committed[id]; ok {
			continue
		}
		d.committed[id] = struct{}{}
		d.commOrder = append(d.commOrder, id)
		if len(d.commOrder) > d.cap {
			drop := d.commOrder[0]
			d.commOrder = d.commOrder[1:]
			delete(d.committed, drop)
		}
	}
}
