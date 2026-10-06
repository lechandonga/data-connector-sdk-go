package runtime

import (
	"context"
	"sync"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/connector"
	"github.com/lechandonga/data-connector-sdk-go/contract"
	"path/filepath"
)

// Host runs many connectors concurrently with isolated state.
type Host struct {
	cfg      Config
	registry *connector.Registry

	mu      sync.Mutex
	runners map[string]*Runner
	sinks   map[string]contract.Connector
	cancel  context.CancelFunc
	epoch   uint64
}

// NewHost constructs a host with the given config (defaults applied).
func NewHost(cfg Config) *Host {
	cfg = cfg.withDefaults()
	return &Host{
		cfg:      cfg,
		registry: connector.NewRegistry(checkpoint.NewFileSchemaStore(cfg.SchemaDir)),
		runners:  map[string]*Runner{},
		sinks:    map[string]contract.Connector{},
		epoch:    1,
	}
}

// Registry exposes the registry for connector registration.
func (h *Host) Registry() *connector.Registry { return h.registry }

// Config returns the effective configuration.
func (h *Host) Config() Config { return h.cfg }

// Run builds, opens and starts all registered connectors. A connector that
// fails to load appears in loadFailures but does not stop the others.
// routes gives the downstream for each named source connector; source names
// absent from the map are opened but not pumped. order gives the optional
// position ordering used to reject checkpoint rollback per connector.
func (h *Host) Run(parent context.Context, routes map[string]Downstream, order map[string]checkpoint.PositionLess) (map[string]*Runner, map[string]*connector.LoadError) {
	loaded, failures := h.registry.Load()
	runners := map[string]*Runner{}

	ctx, cancel := context.WithCancel(parent)
	h.mu.Lock()
	h.cancel = cancel
	h.epoch++
	epoch := h.epoch
	h.mu.Unlock()

	for name, conn := range loaded {
		if conn.Source() == nil && conn.Sink() != nil {
			// sink-only: keep it open and reachable for routing.
			h.mu.Lock()
			h.sinks[name] = conn
			h.mu.Unlock()
			continue
		}
		if conn.Source() == nil {
			_ = guard(conn.Close, h.cfg.CommitTimeout)
			continue
		}
		var less checkpoint.PositionLess
		if order != nil {
			less = order[name]
		}
		store, err := checkpoint.OpenStore(filepath.Join(h.cfg.CheckpointDir, name), name, less)
		if err != nil {
			failures[name] = &connector.LoadError{Connector: name, Kind: connector.LoadKindOpenFailed, Detail: err.Error()}
			continue
		}
		var d Downstream
		if routes != nil {
			d = routes[name]
		}
		runner := NewRunner(h.cfg, conn, store, d, epoch)
		if d == nil {
			// Unrouted source: nothing to pump; close it.
			_ = guard(conn.Close, h.cfg.CommitTimeout)
			continue
		}
		if err := runner.Start(ctx); err != nil {
			failures[name] = &connector.LoadError{Connector: name, Kind: connector.LoadKindOpenFailed, Detail: err.Error()}
			continue
		}
		h.mu.Lock()
		h.runners[name] = runner
		h.mu.Unlock()
		runners[name] = runner
	}

	h.cancelAllOnParentDone(parent)
	return runners, failures
}

func (h *Host) cancelAllOnParentDone(parent context.Context) {
	if parent == nil {
		return
	}
	go func() {
		<-parent.Done()
		h.mu.Lock()
		cancel := h.cancel
		h.mu.Unlock()
		if cancel != nil {
			cancel()
		}
	}()
}

// Stop cancels every runner and waits up to grace per connector for a clean
// drain. Returns terminal failures keyed by connector name.
func (h *Host) Stop(grace time.Duration) map[string]*Failure {
	h.mu.Lock()
	runners := make([]*Runner, 0, len(h.runners))
	for _, r := range h.runners {
		runners = append(runners, r)
	}
	h.runners = map[string]*Runner{}
	sinks := h.sinks
	h.sinks = map[string]contract.Connector{}
	cancel := h.cancel
	h.cancel = nil
	h.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	out := map[string]*Failure{}
	for _, r := range runners {
		wg.Add(1)
		go func(r *Runner) {
			defer wg.Done()
			r.Stop(grace)
			if f := r.Failure(); f != nil && f.Class != ClassCancelled {
				mu.Lock()
				out[r.Name()] = f
				mu.Unlock()
			}
		}(r)
	}
	wg.Wait()
	for _, s := range sinks {
		_ = guard(s.Close, h.cfg.CommitTimeout)
	}
	return out
}

// Sink returns the sink interface of a loaded sink-only or dual connector.
func (h *Host) Sink(name string) contract.Sink {
	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.sinks[name]; ok {
		return c.Sink()
	}
	return nil
}

// Stats returns per-connector stats snapshots.
func (h *Host) Stats() map[string]Snapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]Snapshot{}
	for name, r := range h.runners {
		out[name] = r.Stats()
	}
	return out
}

// Runners returns the active runner set (for tests/tooling).
func (h *Host) Runners() map[string]*Runner {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := map[string]*Runner{}
	for k, v := range h.runners {
		out[k] = v
	}
	return out
}

// Schema is a re-export for convenience.
type Schema = contract.Schema
