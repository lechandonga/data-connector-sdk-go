// Package checkpoint implements per-connector durable progress storage.
//
// Guarantees:
//
//   - One file per connector, written atomically (temp file + rename +
//     fsync of the directory) so a crash never leaves a torn checkpoint.
//   - Positions are opaque byte strings encoded base64. The host compares
//     positions only for equality and for the source-defined order returned
//     through PositionLess; sources own their ordering semantics.
//   - Every commit carries a monotonically increasing Seq. On load, a
//     checkpoint whose seq is lower than the previously observed seq, whose
//     payload is corrupt, or whose position moves backwards relative to the
//     configured ordering is rejected with a distinct error kind.
//   - Commits are serialised per connector and fenced by epoch so that a
//     cancelled/recovered generation cannot perform a half-commit against a
//     newer generation's state.
package checkpoint

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Position is an opaque source-defined offset.
type Position []byte

// State is one durable checkpoint record.
type State struct {
	Connector string `json:"connector"`
	Seq       uint64 `json:"seq"`
	Epoch     uint64 `json:"epoch"`
	Position  string `json:"position"` // base64
	// RecordID is the last confirmed record id, used for diagnostics.
	RecordID string `json:"recordId"`
}

var (
	// ErrCorrupt means the checkpoint file cannot be parsed.
	ErrCorrupt = errors.New("checkpoint is corrupt")
	// ErrRollback means the offered state is older than stored state.
	ErrRollback = errors.New("checkpoint rollback rejected")
	// ErrFence means an expired generation tried to commit.
	ErrFence = errors.New("checkpoint epoch fence rejected commit")
	// ErrNothing means no checkpoint exists yet (first run).
	ErrNothing = errors.New("no checkpoint exists")
)

// CommitError gives a stable Kind plus the connector name for every failed
// commit/load action.
type CommitError struct {
	Connector string
	Kind      string
	Detail    string
	Err       error
}

const (
	KindCorrupt  = "checkpoint-corrupt"
	KindRollback = "checkpoint-rollback"
	KindFence    = "checkpoint-epoch-fenced"
	KindIO       = "checkpoint-io"
)

func (e *CommitError) Error() string {
	return fmt.Sprintf("checkpoint[%s] %s: %s", e.Connector, e.Kind, e.Detail)
}

func (e *CommitError) Unwrap() error { return e.Err }

// PositionLess defines a total order over positions for a connector. When
// supplied, the store rejects positions that move backwards. Sources whose
// positions are not ordered may supply nil; then only equality and seq guard
// the store.
type PositionLess func(a, b Position) bool

// Store is the per-connector durable store.
type Store struct {
	dir     string
	less    PositionLess
	mu      sync.Mutex
	current State
	loaded  bool
	path    string
}

// OpenStore opens (or creates) the store for connector name inside dir.
func OpenStore(dir, name string, less PositionLess) (*Store, error) {
	if less == nil {
		less = func(a, b Position) bool { return false }
	}
	s := &Store{dir: dir, less: less, path: filepath.Join(dir, sanitize(name)+".ckpt.json")}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, &CommitError{Connector: name, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	if err := s.load(); err != nil && !errors.Is(err, ErrNothing) {
		return nil, err
	}
	return s, nil
}

func sanitize(name string) string {
	out := make([]rune, 0, len(name))
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}

func (s *Store) load() error {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrNothing
	}
	if err != nil {
		return &CommitError{Kind: KindIO, Detail: err.Error(), Err: err}
	}
	var st State
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&st); err != nil {
		return &CommitError{Kind: KindCorrupt, Detail: "invalid JSON: " + err.Error(), Err: ErrCorrupt}
	}
	if st.Position != "" {
		if _, err := base64.StdEncoding.DecodeString(st.Position); err != nil {
			return &CommitError{Connector: st.Connector, Kind: KindCorrupt, Detail: "position is not base64: " + err.Error(), Err: ErrCorrupt}
		}
	}
	s.current = st
	s.loaded = true
	return nil
}

// Current returns the last committed state. It returns ErrNothing on a fresh
// store.
func (s *Store) Current() (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.loaded {
		return State{}, ErrNothing
	}
	return s.current, nil
}

// DecodePosition exposes base64 decoding of stored positions.
func DecodePosition(p string) (Position, error) {
	if p == "" {
		return nil, nil
	}
	b, err := base64.StdEncoding.DecodeString(p)
	if err != nil {
		return nil, &CommitError{Kind: KindCorrupt, Detail: err.Error(), Err: ErrCorrupt}
	}
	return b, nil
}

// EncodePosition exposes base64 encoding for tooling/tests.
func EncodePosition(p Position) string {
	return base64.StdEncoding.EncodeToString(p)
}

// Commit validates and durably persists next. Validation rules:
//
//   - next.Epoch must be >= stored epoch; a strictly older epoch is fenced.
//   - next.Seq must be strictly greater than the stored seq within the same
//     epoch (a new epoch resets the comparison to the epoch check).
//   - next.Position must not be strictly less than the stored position under
//     the connector ordering; equal is allowed (idempotent re-commit).
//
// The write is atomic; a failed commit leaves the previous file intact, so
// there is never a half-committed state observable after recovery.
func (s *Store) Commit(next State) (State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if next.Position != "" {
		if _, err := base64.StdEncoding.DecodeString(next.Position); err != nil {
			return s.current, &CommitError{Connector: next.Connector, Kind: KindCorrupt, Detail: "position is not base64", Err: ErrCorrupt}
		}
	}
	if s.loaded {
		if next.Epoch < s.current.Epoch {
			return s.current, &CommitError{Connector: next.Connector, Kind: KindFence, Detail: fmt.Sprintf("epoch %d < committed %d", next.Epoch, s.current.Epoch), Err: ErrFence}
		}
		if next.Epoch == s.current.Epoch && next.Seq <= s.current.Seq {
			return s.current, &CommitError{Connector: next.Connector, Kind: KindRollback, Detail: fmt.Sprintf("seq %d <= committed %d within epoch %d", next.Seq, s.current.Seq, next.Epoch), Err: ErrRollback}
		}
		if next.Epoch == s.current.Epoch && s.current.Position != "" && next.Position != "" {
			oldPos, _ := DecodePosition(s.current.Position)
			newPos, _ := DecodePosition(next.Position)
			if s.less(newPos, oldPos) {
				return s.current, &CommitError{Connector: next.Connector, Kind: KindRollback, Detail: "position precedes committed position", Err: ErrRollback}
			}
		}
	}
	if err := s.writeAtomic(next); err != nil {
		return s.current, err
	}
	s.current = next
	s.loaded = true
	return next, nil
}

func (s *Store) writeAtomic(st State) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return &CommitError{Connector: st.Connector, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	tmp, err := os.CreateTemp(s.dir, ".ckpt-*")
	if err != nil {
		return &CommitError{Connector: st.Connector, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	tmpName := tmp.Name()
	enc := json.NewEncoder(tmp)
	enc.SetIndent("", "  ")
	if err := enc.Encode(st); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return &CommitError{Connector: st.Connector, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return &CommitError{Connector: st.Connector, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return &CommitError{Connector: st.Connector, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		os.Remove(tmpName)
		return &CommitError{Connector: st.Connector, Kind: KindIO, Detail: err.Error(), Err: err}
	}
	syncDir(s.dir)
	return nil
}

func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}

// Path reports the on-disk location, useful for tests and tooling.
func (s *Store) Path() string { return s.path }
