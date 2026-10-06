package runtime

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type ctxSource struct{ ctx context.Context }

func (c *ctxSource) Done() <-chan struct{} { return c.ctx.Done() }
func (c *ctxSource) Err() error            { return c.ctx.Err() }

type ctxSink struct{ ctx context.Context }

func (c *ctxSink) Done() <-chan struct{} { return c.ctx.Done() }
func (c *ctxSink) Err() error            { return c.ctx.Err() }

type panicError struct {
	value any
}

func (e *panicError) Error() string { return fmt.Sprintf("connector panicked: %v", e.value) }

func panicErr(v any) error { return &panicError{value: v} }

// classify maps errors returned from connector/downstream calls onto a
// Failure class. Panic values carry ClassPanic, cancellations
// ClassCancelled, everything else ClassConnector.
func classify(err error, name, op string, stats *Stats) *Failure {
	var pe *panicError
	if errors.As(err, &pe) {
		stats.Panics.Add(1)
		return &Failure{Connector: name, Class: ClassPanic, Op: op, Detail: pe.Error(), Err: err}
	}
	if errors.Is(err, ErrCancelled) || errors.Is(err, context.Canceled) {
		return &Failure{Connector: name, Class: ClassCancelled, Op: op, Detail: err.Error(), Err: ErrCancelled}
	}
	return &Failure{Connector: name, Class: ClassConnector, Op: op, Detail: err.Error(), Err: err}
}

// guard runs fn in a goroutine with a hard timeout and panic recovery. It is
// used for lifecycle hooks (Open/Close/Resume) that do not take a context.
// A timeout does not kill the underlying goroutine - Go cannot safely kill
// goroutines - but unblocks the host; every other connector continues.
func guard(fn func() error, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ch := make(chan error, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				ch <- panicErr(rec)
			}
		}()
		ch <- fn()
	}()
	select {
	case err := <-ch:
		return err
	case <-time.After(timeout):
		return ErrTimeout
	}
}
