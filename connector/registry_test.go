package connector

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/connector/v1"
	"github.com/lechandonga/data-connector-sdk-go/contract"
)

func bizLog(t testing.TB, biz, name, format string, args ...any) {
	t.Helper()
	fmt.Fprintf(os.Stdout, "    [biz:%s] case=%s "+format+"\n", append([]any{biz, name}, args...)...)
}

// stubConnector is a minimal valid V2 connector for tests.
type stubConnector struct {
	decl contract.Declaration
}

func (s *stubConnector) Declare() contract.Declaration { return s.decl }
func (s *stubConnector) Open() error                   { return nil }
func (s *stubConnector) Close() error                  { return nil }
func (s *stubConnector) Source() contract.Source       { return nil }
func (s *stubConnector) Sink() contract.Sink           { return nil }

func goodDecl(name string) contract.Declaration {
	s := &contract.Schema{Name: "s", Fields: []contract.Field{{Name: "id", Type: contract.TypeString, Required: true}}}
	return contract.Declaration{
		Name: name, ConnectorVersion: "1.0.0", APIVersion: contract.V2,
		Capabilities: []contract.Capability{contract.CapSource}, OutputSchema: s,
	}
}

func TestRegistrationRejections(t *testing.T) {
	r := NewRegistry(nil)

	t.Run("duplicate registration rejected distinctly", func(t *testing.T) {
		bizLog(t, "load-registration", "duplicate", "same name registered twice -> ErrDuplicateConnector")
		if err := r.Register(func() (contract.Connector, error) { return &stubConnector{decl: goodDecl("dup")}, nil }); err != nil {
			t.Fatal(err)
		}
		err := r.Register(func() (contract.Connector, error) { return &stubConnector{decl: goodDecl("dup")}, nil })
		var le *LoadError
		if !errors.As(err, &le) || le.Kind != LoadKindDuplicate || !errors.Is(err, contract.ErrDuplicateConnector) {
			t.Fatalf("want duplicate LoadError, got %v", err)
		}
	})

	t.Run("missing declaration fields rejected", func(t *testing.T) {
		bizLog(t, "load-registration", "missing-fields", "empty name/version/caps -> ErrDeclarationMissing")
		d := goodDecl("m")
		d.Name = "  "
		err := r.Register(func() (contract.Connector, error) { return &stubConnector{decl: d}, nil })
		var le *LoadError
		if !errors.As(err, &le) || le.Kind != LoadKindMissing {
			t.Fatalf("want missing, got %v", err)
		}
	})

	t.Run("conflicting declaration rejected", func(t *testing.T) {
		bizLog(t, "load-registration", "conflict", "resumable without source -> ErrDeclarationConflict")
		s := &contract.Schema{Name: "s", Fields: []contract.Field{{Name: "id", Type: contract.TypeString, Required: true}}}
		d := contract.Declaration{Name: "c", ConnectorVersion: "1", APIVersion: contract.V2,
			Capabilities: []contract.Capability{contract.CapSink}, InputSchema: s, OutputSchema: s}
		err := r.Register(func() (contract.Connector, error) { return &stubConnector{decl: d}, nil })
		var le *LoadError
		if !errors.As(err, &le) || le.Kind != LoadKindConflict {
			t.Fatalf("want conflict, got %v", err)
		}
	})

	t.Run("unknown api version rejected, not reinterpreted", func(t *testing.T) {
		bizLog(t, "load-registration", "bad-api-version", "future/v3 never runs with v2 semantics")
		d := goodDecl("future")
		d.APIVersion = "connector.api/v99"
		err := r.Register(func() (contract.Connector, error) { return &stubConnector{decl: d}, nil })
		var le *LoadError
		if !errors.As(err, &le) || le.Kind != LoadKindAPIVersion || !errors.Is(err, contract.ErrUnsupportedAPIVersion) {
			t.Fatalf("want unsupported api version, got %v", err)
		}
	})

	t.Run("invalid schema rejected at registration", func(t *testing.T) {
		bizLog(t, "load-registration", "bad-schema", "unknown field type rejected")
		d := goodDecl("badschema")
		d.OutputSchema = &contract.Schema{Name: "x", Fields: []contract.Field{{Name: "a", Type: "nope"}}}
		err := r.Register(func() (contract.Connector, error) { return &stubConnector{decl: d}, nil })
		var le *LoadError
		if !errors.As(err, &le) || le.Kind != LoadKindSchema {
			t.Fatalf("want schema rejection, got %v", err)
		}
	})
}

type panicConnector struct{ decl contract.Declaration }

func (p *panicConnector) Declare() contract.Declaration { return p.decl }
func (p *panicConnector) Open() error                   { panic("boom in open") }
func (p *panicConnector) Close() error                  { return nil }
func (p *panicConnector) Source() contract.Source       { return nil }
func (p *panicConnector) Sink() contract.Sink           { return nil }

func TestOneBadConnectorDoesNotBlockOthers(t *testing.T) {
	bizLog(t, "load-isolation", "bad-open-isolated", "panicking Open is attributed; others still load")
	r := NewRegistry(nil)
	_ = r.Register(func() (contract.Connector, error) { return &panicConnector{decl: goodDecl("bad")}, nil })
	_ = r.Register(func() (contract.Connector, error) { return &stubConnector{decl: goodDecl("good")}, nil })

	loaded, failures := r.Load()
	if _, ok := loaded["good"]; !ok {
		t.Fatalf("good connector must still load: failures=%v", failures)
	}
	le, ok := failures["bad"]
	if !ok || le.Kind != LoadKindPanic {
		t.Fatalf("want panic load failure for bad, got %+v", failures)
	}
}

func TestSchemaEvolutionPersistedAcrossRegistrations(t *testing.T) {
	bizLog(t, "load-registration", "persisted-schema-evolution", "restart with required-added schema is rejected")
	dir := filepath.Join(t.TempDir(), "schemas")
	store := checkpoint.NewFileSchemaStore(dir)
	r1 := NewRegistry(store)
	d1 := goodDecl("evolving")
	if err := r1.Register(func() (contract.Connector, error) { return &stubConnector{decl: d1}, nil }); err != nil {
		t.Fatal(err)
	}
	// New registry instance simulates process restart, same persisted schema.
	r2 := NewRegistry(checkpoint.NewFileSchemaStore(dir))
	d2 := goodDecl("evolving")
	d2.OutputSchema.Fields = append(d2.OutputSchema.Fields, contract.Field{Name: "must", Type: contract.TypeString, Required: true})
	err := r2.Register(func() (contract.Connector, error) { return &stubConnector{decl: d2}, nil })
	var le *LoadError
	if !errors.As(err, &le) || le.Kind != LoadKindSchema {
		t.Fatalf("want persisted schema incompatibility, got %v", err)
	}
	// Compatible evolution (optional field) succeeds after restart.
	r3 := NewRegistry(checkpoint.NewFileSchemaStore(dir))
	d3 := goodDecl("evolving")
	d3.OutputSchema.Fields = append(d3.OutputSchema.Fields, contract.Field{Name: "extra", Type: contract.TypeBool, Required: false})
	if err := r3.Register(func() (contract.Connector, error) { return &stubConnector{decl: d3}, nil }); err != nil {
		t.Fatalf("compatible evolution must register: %v", err)
	}
}

// v1Legacy is a hand-written legacy connector for the adapter test.
type v1Legacy struct {
	records []v1.LegacyRecord
	idx     int
}

func (v *v1Legacy) Pull(cancelled <-chan struct{}) (v1.LegacyRecord, error) {
	if v.idx >= len(v.records) {
		<-cancelled
		return v1.LegacyRecord{}, errEOF
	}
	r := v.records[v.idx]
	v.idx++
	return r, nil
}

var errEOF = fmt.Errorf("EOF")

func TestV1AdapterIsAcceptedAndMapped(t *testing.T) {
	bizLog(t, "load-compat-v1", "v1-adapter", "legacy connector loads via adapter with v1 declaration")
	legacy := &v1Legacy{records: []v1.LegacyRecord{{ID: 1, Message: "hi"}}}
	adapted, err := v1.New(&v1.LegacyConnector{Name: "old", Version: "0.9", Source: legacy})
	if err != nil {
		t.Fatal(err)
	}
	r := NewRegistry(nil)
	if err := r.Register(func() (contract.Connector, error) { return adapted, nil }); err != nil {
		t.Fatalf("v1 connector should register through adapter: %v", err)
	}
	loaded, failures := r.Load()
	if _, ok := loaded["old"]; !ok {
		t.Fatalf("v1 connector failed to load: %v", failures)
	}
	batch, err := loaded["old"].Source().Read(&neverCtx{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if batch.Records[0].Record.Values["message"] != "hi" {
		t.Fatalf("legacy mapping wrong: %+v", batch.Records)
	}
}

type neverCtx struct{}

func (neverCtx) Done() <-chan struct{} { return make(chan struct{}) }
func (neverCtx) Err() error            { return nil }
