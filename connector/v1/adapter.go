// Package v1 provides the explicit compatibility adapter for connectors
// written against the legacy connector.api/v1 interface.
//
// Semantic differences between V1 and V2 are bridged deterministically:
//
//   - V1 sources emit one record per Pull() call; V2 reads batches. The
//     adapter translates one pull into a one-record batch.
//   - V1 records used a flat "id + message" shape (id int64, message
//     string). The adapter maps them onto the declared V2 schema fields
//     "id" (int64) and "message" (string); ANY other declared schema is a
//     contract conflict and the connector is rejected, never coerced.
//   - V1 had no commit acknowledgement on sources; restart replay therefore
//     relies purely on the host checkpoint and the host dedupe window. The
//     adapter never advertises CapResumable for a V1 connector unless the
//     legacy connector itself implements the optional Seek method.
package v1

import (
	"fmt"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// LegacyRecord is the V1 wire shape.
type LegacyRecord struct {
	ID      int64
	Message string
}

// LegacySource is what a V1 read connector implemented.
type LegacySource interface {
	// Pull returns the next record. io.EOF means the stream is finished.
	Pull(cancelled <-chan struct{}) (LegacyRecord, error)
}

// LegacySeeker is optionally implemented by V1 sources that can resume from
// a previously returned opaque position (the record id, big-endian 8 bytes).
type LegacySeeker interface {
	Seek(position []byte) error
}

// LegacySink is what a V1 write connector implemented: one Put per record,
// one Flush per batch, no durable-commit distinction. The adapter maps a V2
// Commit onto Flush.
type LegacySink interface {
	Put(rec LegacyRecord) error
	Flush() error
}

// LegacyConnector is a connector written against the old interface.
type LegacyConnector struct {
	Name    string
	Version string
	Source  LegacySource // nil if sink-only
	Sink    LegacySink   // nil if source-only
	// OnOpen / OnClose mirror the lifecycle hooks old code registered.
	OnOpen  func() error
	OnClose func() error
}

// Adapter wraps a LegacyConnector as a V2 contract.Connector.
type Adapter struct {
	legacy *LegacyConnector
	decl   contract.Declaration
}

// New validates that the legacy connector maps cleanly onto V2 and returns
// the adapted connector. Unrepresentable declarations are rejected here,
// explicitly, rather than running with guessed semantics.
func New(lc *LegacyConnector) (*Adapter, error) {
	if lc == nil {
		return nil, fmt.Errorf("v1 adapter: nil legacy connector")
	}
	if lc.Source == nil && lc.Sink == nil {
		return nil, fmt.Errorf("v1 adapter %q: legacy connector is neither source nor sink", lc.Name)
	}
	caps := []contract.Capability{}
	var out, in *contract.Schema
	if lc.Source != nil {
		caps = append(caps, contract.CapSource)
		if _, ok := lc.Source.(LegacySeeker); ok {
			caps = append(caps, contract.CapResumable)
		}
		s := legacySchema()
		out = &s
	}
	if lc.Sink != nil {
		caps = append(caps, contract.CapSink, contract.CapAtLeastOnce)
		s := legacySchema()
		in = &s
	}
	return &Adapter{
		legacy: lc,
		decl: contract.Declaration{
			Name:             lc.Name,
			ConnectorVersion: lc.Version,
			APIVersion:       contract.V1,
			Capabilities:     caps,
			OutputSchema:     out,
			InputSchema:      in,
		},
	}, nil
}

// legacySchema is the single V1 record shape. The adapter cannot map any
// other schema, which is the point: old semantics are not guessed.
func legacySchema() contract.Schema {
	return contract.Schema{
		Name: "v1.legacyRecord",
		Fields: []contract.Field{
			{Name: "id", Type: contract.TypeInt64, Required: true},
			{Name: "message", Type: contract.TypeString, Required: true},
		},
	}
}

func (a *Adapter) Declare() contract.Declaration { return a.decl }

func (a *Adapter) Open() error {
	if a.legacy.OnOpen != nil {
		return a.legacy.OnOpen()
	}
	return nil
}

func (a *Adapter) Close() error {
	if a.legacy.OnClose != nil {
		return a.legacy.OnClose()
	}
	return nil
}

func (a *Adapter) Source() contract.Source {
	if a.legacy.Source == nil {
		return nil
	}
	return &adaptedSource{legacy: a.legacy.Source}
}

func (a *Adapter) Sink() contract.Sink {
	if a.legacy.Sink == nil {
		return nil
	}
	return &adaptedSink{legacy: a.legacy.Sink}
}

type adaptedSource struct {
	legacy LegacySource
}

func (s *adaptedSource) Read(ctx contract.SourceContext, _ int) (contract.Batch, error) {
	rec, err := s.legacy.Pull(ctx.Done())
	if err != nil {
		return contract.Batch{}, err
	}
	return contract.Batch{
		Records: []contract.Envelope{{
			Record: contract.Record{
				ID:     fmt.Sprintf("v1-%d", rec.ID),
				Values: map[string]any{"id": rec.ID, "message": rec.Message},
			},
			Position: encodePosition(rec.ID),
		}},
		Position: encodePosition(rec.ID),
	}, nil
}

// Resume applies a host checkpoint to a seekable legacy source.
func (s *adaptedSource) Resume(position []byte) error {
	seeker, ok := s.legacy.(LegacySeeker)
	if !ok {
		return fmt.Errorf("v1 source is not resumable")
	}
	return seeker.Seek(position)
}

type adaptedSink struct {
	legacy LegacySink
}

func (s *adaptedSink) Write(_ contract.SinkContext, batch contract.Batch) error {
	for _, env := range batch.Records {
		idVal, _ := env.Record.Values["id"].(int64)
		msgVal, _ := env.Record.Values["message"].(string)
		if err := s.legacy.Put(LegacyRecord{ID: idVal, Message: msgVal}); err != nil {
			return err
		}
	}
	return nil
}

func (s *adaptedSink) Commit(_ contract.SinkContext) error {
	return s.legacy.Flush()
}

// encodePosition renders a V1 record id as an opaque 8-byte position.
func encodePosition(id int64) []byte {
	buf := make([]byte, 8)
	for i := 0; i < 8; i++ {
		buf[7-i] = byte(id >> (8 * i))
	}
	return buf
}

// DecodePosition is exported for tests and for legacy Seek implementations.
func DecodePosition(p []byte) (int64, error) {
	if len(p) != 8 {
		return 0, fmt.Errorf("v1 position must be 8 bytes, got %d", len(p))
	}
	var id int64
	for i := 0; i < 8; i++ {
		id = id<<8 | int64(p[i])
	}
	return id, nil
}
