// Package contract defines the versioned connector contract: connector
// declarations, data schemas, capability sets and the typed interfaces a
// third-party connector must implement.
//
// The host supports API version V2 natively. Connectors written against the
// legacy V1 interface are wrapped by an explicit adapter (see connector/v1);
// any other version is rejected outright, never reinterpreted with V2
// semantics.
package contract

import (
	"errors"
	"fmt"
	"strings"
)

// APIVersion identifies a generation of the connector interface contract.
type APIVersion string

const (
	// V1 is the legacy connector interface. It is loadable only through the
	// compatibility adapter.
	V1 APIVersion = "connector.api/v1"
	// V2 is the current native interface.
	V2 APIVersion = "connector.api/v2"
)

// FieldType enumerates the primitive field types understood by the schema
// compatibility checker.
type FieldType string

const (
	TypeString  FieldType = "string"
	TypeInt64   FieldType = "int64"
	TypeFloat64 FieldType = "float64"
	TypeBool    FieldType = "bool"
)

// Field describes one field of an output schema.
//
// Required=false means the field is optional. Optional fields are NEVER
// silently defaulted: a missing optional value is represented explicitly as
// a null value by the host, and a missing required value is a contract
// violation.
type Field struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
}

// Schema is the declared output data schema of a connector. Field order is
// significant for stable, reproducible compatibility reporting.
type Schema struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

// Capability declares what a connector can do. A connector may be a source,
// a sink, or both.
type Capability string

const (
	CapSource      Capability = "source"
	CapSink        Capability = "sink"
	CapResumable   Capability = "resumable"     // source supports restart from a position
	CapAtLeastOnce Capability = "at-least-once" // sink provides at-least-once delivery
)

// Declaration is the self-describing metadata every connector must present
// at registration time. The host validates it before the connector is ever
// invoked.
type Declaration struct {
	// Name uniquely identifies the connector kind. Non-empty, no whitespace.
	Name string `json:"name"`
	// ConnectorVersion is the version of the connector implementation.
	ConnectorVersion string `json:"connectorVersion"`
	// APIVersion is the interface generation the connector was written
	// against. Must be V1 or V2; anything else is unsupported.
	APIVersion APIVersion `json:"apiVersion"`
	// Capabilities advertised by the connector.
	Capabilities []Capability `json:"capabilities"`
	// OutputSchema is required for source connectors and describes every
	// emitted record.
	OutputSchema *Schema `json:"outputSchema,omitempty"`
	// InputSchema is required for sink connectors and describes every record
	// the sink accepts.
	InputSchema *Schema `json:"inputSchema,omitempty"`
}

// SourceContext is handed to a source on each read call. Read implementations
// MUST return promptly when ctx is cancelled; the host additionally enforces
// ReadTimeout as a watchdog for connectors that ignore cancellation.
type SourceContext interface {
	// Done is closed when the connector should stop (cancellation or
	// timeout).
	Done() <-chan struct{}
	// Err returns the cancellation reason after Done closes.
	Err() error
}

// SinkContext is handed to a sink on write/commit. Cancellation is wired to
// the same cooperative contract as for sources.
type SinkContext interface {
	Done() <-chan struct{}
	Err() error
}

// Record is one datum flowing through the pipeline.
type Record struct {
	// ID is the connector-assigned stable identity of the record. Records
	// replayed after a restart reuse the same ID, which lets the host dedupe
	// records that were delivered but whose checkpoint commit was lost.
	ID string
	// Values maps field name to value. nil value encodes an explicit null;
	// a missing map entry for a required field is a contract violation.
	Values map[string]any
}

// Envelope wraps a record with the position at which it was produced. The
// position is opaque to the host; only the producing source interprets it.
type Envelope struct {
	Record   Record
	Position []byte
}

// Batch is the unit of delivery and of checkpoint commits.
type Batch struct {
	// Records are the records in this batch.
	Records []Envelope
	// Position is the position to commit AFTER the whole batch is confirmed.
	Position []byte
}

// Source is the V2 read side. Read returns up to max records; an empty slice
// with nil error means "no data right now" (poll). Returning io.EOF signals
// that the stream is permanently complete; the host stops the runner
// successfully.
type Source interface {
	Read(ctx SourceContext, max int) (Batch, error)
}

// Sink is the V2 write side. Write delivers one batch; Commit finalises it.
// After Commit returns nil the records must not be delivered twice by the
// sink even if the host later replays them (dedupe by Record.ID).
type Sink interface {
	Write(ctx SinkContext, batch Batch) error
	Commit(ctx SinkContext) error
}

// Connector is a loadable V2 connector instance.
type Connector interface {
	Declare() Declaration
	// Open is called once after declaration validation passes.
	Open() error
	// Close releases resources; it must not panic.
	Close() error
	// Source returns the read implementation when CapSource is advertised.
	Source() Source
	// Sink returns the write implementation when CapSink is advertised.
	Sink() Sink
}

// Sentinel errors used to distinguish failure classes. Callers should use
// errors.Is rather than comparing directly.
var (
	ErrDuplicateConnector    = errors.New("connector name already registered")
	ErrDeclarationMissing    = errors.New("connector declaration is missing required information")
	ErrDeclarationConflict   = errors.New("connector declaration is internally inconsistent")
	ErrSchemaIncompatible    = errors.New("schema change is incompatible")
	ErrUnsupportedAPIVersion = errors.New("unsupported connector API version")
)

// Incompatibility carries a deterministic explanation for a rejected schema
// evolution. Kinds are stable strings so hosts and tests can branch on them.
type Incompatibility struct {
	Kind   string
	Field  string
	Detail string
}

func (i Incompatibility) Error() string {
	if i.Field == "" {
		return fmt.Sprintf("%s: %s", i.Kind, i.Detail)
	}
	return fmt.Sprintf("%s: field %q: %s", i.Kind, i.Field, i.Detail)
}

const (
	IncompatAddedRequired      = "field-added-required"
	IncompatRemoved            = "field-removed"
	IncompatTypeChanged        = "field-type-changed"
	IncompatOptionalToRequired = "optional-to-required"
	IncompatDuplicateField     = "field-duplicated"
	IncompatEmptySchema        = "schema-empty"
	IncompatBadType            = "field-type-unknown"
	IncompatUnnamedField       = "field-unnamed"
)

// ValidateSchema checks a schema declaration in isolation.
func ValidateSchema(s *Schema) []Incompatibility {
	var out []Incompatibility
	if s == nil {
		out = append(out, Incompatibility{Kind: IncompatEmptySchema, Detail: "schema is nil"})
		return out
	}
	if strings.TrimSpace(s.Name) == "" {
		out = append(out, Incompatibility{Kind: IncompatEmptySchema, Detail: "schema name is empty"})
	}
	seen := map[string]bool{}
	for _, f := range s.Fields {
		if f.Name == "" {
			out = append(out, Incompatibility{Kind: IncompatUnnamedField, Detail: "field has no name"})
			continue
		}
		if seen[f.Name] {
			out = append(out, Incompatibility{Kind: IncompatDuplicateField, Field: f.Name, Detail: "field declared more than once"})
		}
		seen[f.Name] = true
		switch f.Type {
		case TypeString, TypeInt64, TypeFloat64, TypeBool:
		default:
			out = append(out, Incompatibility{Kind: IncompatBadType, Field: f.Name, Detail: fmt.Sprintf("unknown type %q", f.Type)})
		}
	}
	if len(s.Fields) == 0 {
		out = append(out, Incompatibility{Kind: IncompatEmptySchema, Detail: "schema declares no fields"})
	}
	return out
}

// DiffSchema determines whether evolving from old to new is a compatible
// change. The rules are deliberately explicit:
//
//   - Adding an OPTIONAL field is compatible. Existing records simply carry
//     null for it; no default value is ever invented.
//   - Adding a REQUIRED field is INCOMPATIBLE (old records cannot satisfy it).
//   - Removing a field is INCOMPATIBLE: downstream consumers may depend on
//     it and silently dropping data is not allowed.
//   - Changing a field's type is INCOMPATIBLE.
//   - Tightening an optional field to required is INCOMPATIBLE; relaxing a
//     required field to optional is compatible.
//   - Renaming is not modelled: it appears as remove+add-required and is
//     rejected.
func DiffSchema(old, neu Schema) []Incompatibility {
	var out []Incompatibility
	oldFields := map[string]Field{}
	for _, f := range old.Fields {
		oldFields[f.Name] = f
	}
	newFields := map[string]bool{}
	for _, f := range neu.Fields {
		newFields[f.Name] = true
		of, existed := oldFields[f.Name]
		if !existed {
			if f.Required {
				out = append(out, Incompatibility{Kind: IncompatAddedRequired, Field: f.Name, Detail: "new field is required; existing records cannot populate it"})
			}
			continue
		}
		if of.Type != f.Type {
			out = append(out, Incompatibility{Kind: IncompatTypeChanged, Field: f.Name, Detail: fmt.Sprintf("type changed from %q to %q", of.Type, f.Type)})
		}
		if !of.Required && f.Required {
			out = append(out, Incompatibility{Kind: IncompatOptionalToRequired, Field: f.Name, Detail: "optional field cannot be tightened to required in a compatible change"})
		}
	}
	for _, f := range old.Fields {
		if !newFields[f.Name] {
			out = append(out, Incompatibility{Kind: IncompatRemoved, Field: f.Name, Detail: "field removed; dropping data silently is not allowed"})
		}
	}
	return out
}
