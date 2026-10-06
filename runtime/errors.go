// Package runtime implements the host runtime: isolated per-connector
// scheduling, cancellation/timeout watchdogs, panic containment, bounded
// back-pressured delivery and checkpoint-coordinated at-least-once
// processing.
package runtime

import (
	"errors"
	"fmt"
)

// FailureClass is the top-level attribution of a failure.
type FailureClass string

const (
	// ClassConnector: the connector returned an error.
	ClassConnector FailureClass = "connector-error"
	// ClassContract: the connector violated the declared contract.
	ClassContract FailureClass = "contract-violation"
	// ClassTimeout: a connector call exceeded its watchdog budget.
	ClassTimeout FailureClass = "timeout"
	// ClassCancelled: the host cancelled the run (shutdown / recovery).
	ClassCancelled FailureClass = "cancelled"
	// ClassHost: an error in host infrastructure (checkpoint store, etc.).
	ClassHost FailureClass = "host-error"
	// ClassPanic: the connector panicked; recovered and attributed.
	ClassPanic FailureClass = "connector-panic"
)

// Failure attributes every error to a connector and a class.
type Failure struct {
	Connector string
	Class     FailureClass
	Op        string
	Detail    string
	Err       error
}

func (f *Failure) Error() string {
	return fmt.Sprintf("[%s] connector %q op %s: %s", f.Class, f.Connector, f.Op, f.Detail)
}

func (f *Failure) Unwrap() error { return f.Err }

// ErrTimeout is the sentinel wrapped when a watchdog fires.
var ErrTimeout = errors.New("connector call timed out")

// ErrCancelled is the sentinel wrapped on host-initiated cancellation.
var ErrCancelled = errors.New("run cancelled by host")
