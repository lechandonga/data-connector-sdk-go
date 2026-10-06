// Package connector implements the host-side connector registry: declaration
// validation, duplicate/conflict detection, schema-evolution enforcement and
// the V1 compatibility adapter boundary.
package connector

import (
	"fmt"
	"strings"
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// LoadError reports why one connector failed to load. The Kind is a stable
// string; use errors.Is to match the underlying sentinel.
type LoadError struct {
	Connector string
	Kind      string
	Detail    string
	Err       error
	Reasons   []contract.Incompatibility
}

const (
	LoadKindDuplicate  = "duplicate"
	LoadKindMissing    = "declaration-missing"
	LoadKindConflict   = "declaration-conflict"
	LoadKindSchema     = "schema-incompatible"
	LoadKindAPIVersion = "unsupported-api-version"
	LoadKindPanic      = "connector-panic"
	LoadKindOpenFailed = "open-failed"
)

func (e *LoadError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "connector %q: %s", e.Connector, e.Kind)
	if e.Detail != "" {
		fmt.Fprintf(&b, ": %s", e.Detail)
	}
	for _, r := range e.Reasons {
		fmt.Fprintf(&b, "; %s", r.Error())
	}
	if e.Err != nil {
		fmt.Fprintf(&b, " (%v)", e.Err)
	}
	return b.String()
}

func (e *LoadError) Unwrap() error { return e.Err }

// Factory builds a fresh connector instance. The returned connector reports
// its own declaration; the registry never trusts a factory argument.
type Factory func() (contract.Connector, error)

// SchemaStore persists the last accepted schema per connector, so a restart
// with an incompatible new build is deterministically rejected.
type SchemaStore interface {
	LoadSchema(name string) (contract.Schema, bool, error)
	SaveSchema(name string, s contract.Schema) error
}

// Registry holds validated connector factories keyed by connector name.
type Registry struct {
	mu        sync.Mutex
	factories map[string]Factory
	schemas   SchemaStore
}

// NewRegistry creates a registry. schemaStore may be nil to disable
// cross-restart schema-evolution checks.
func NewRegistry(schemaStore SchemaStore) *Registry {
	return &Registry{factories: map[string]Factory{}, schemas: schemaStore}
}

// Registered reports whether name is present.
func (r *Registry) Registered(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.factories[name]
	return ok
}

// Register validates d and stores the factory. A failure leaves the registry
// unchanged. One bad connector never prevents later connectors from
// registering.
func (r *Registry) Register(f Factory) error {
	if f == nil {
		return &LoadError{Connector: "?", Kind: LoadKindMissing, Detail: "nil factory", Err: contract.ErrDeclarationMissing}
	}
	c, err := safeBuild(f)
	if err != nil {
		return err
	}
	d := c.Declare()
	name := d.Name
	if err := validateDeclaration(d); err != nil {
		closeQuietly(c)
		return err
	}
	if d.APIVersion != contract.V1 && d.APIVersion != contract.V2 {
		return &LoadError{Connector: name, Kind: LoadKindAPIVersion, Detail: fmt.Sprintf("declared %q; host supports %q and %q (v1 via adapter only)", d.APIVersion, contract.V2, contract.V1), Err: contract.ErrUnsupportedAPIVersion}
	}

	// Cross-restart schema evolution check before mutating the registry.
	if r.schemas != nil {
		schema := relevantSchema(d)
		if schema != nil {
			old, ok, lerr := r.schemas.LoadSchema(name)
			if lerr != nil {
				return &LoadError{Connector: name, Kind: LoadKindSchema, Detail: "cannot load previously committed schema: " + lerr.Error(), Err: contract.ErrSchemaIncompatible}
			}
			if ok {
				if reasons := contract.DiffSchema(old, *schema); len(reasons) > 0 {
					return &LoadError{Connector: name, Kind: LoadKindSchema, Reasons: reasons, Err: contract.ErrSchemaIncompatible}
				}
			}
		}
	}

	r.mu.Lock()
	if _, exists := r.factories[name]; exists {
		r.mu.Unlock()
		closeQuietly(c)
		return &LoadError{Connector: name, Kind: LoadKindDuplicate, Detail: "a factory with the same name was already registered", Err: contract.ErrDuplicateConnector}
	}
	r.factories[name] = f
	r.mu.Unlock()

	if r.schemas != nil {
		if schema := relevantSchema(d); schema != nil {
			if serr := r.schemas.SaveSchema(name, *schema); serr != nil {
				return &LoadError{Connector: name, Kind: LoadKindSchema, Detail: "failed to persist accepted schema: " + serr.Error(), Err: contract.ErrSchemaIncompatible}
			}
		}
	}
	closeQuietly(c)
	return nil
}

// Load builds and opens every registered connector. A failure for one
// connector is returned in failures but does not abort loading of the
// others; successfully opened connectors are returned keyed by name.
func (r *Registry) Load() (map[string]contract.Connector, map[string]*LoadError) {
	r.mu.Lock()
	names := make([]string, 0, len(r.factories))
	factories := make(map[string]Factory, len(r.factories))
	for n, f := range r.factories {
		names = append(names, n)
		factories[n] = f
	}
	r.mu.Unlock()

	loaded := map[string]contract.Connector{}
	failures := map[string]*LoadError{}
	for _, name := range names {
		c, err := safeBuild(factories[name])
		if err != nil {
			failures[name] = err
			continue
		}
		func() {
			defer func() {
				if rec := recover(); rec != nil {
					failures[name] = &LoadError{Connector: name, Kind: LoadKindPanic, Detail: fmt.Sprintf("panic while opening: %v", rec), Err: contract.ErrDeclarationConflict}
					closeQuietly(c)
				}
			}()
			if oerr := c.Open(); oerr != nil {
				failures[name] = &LoadError{Connector: name, Kind: LoadKindOpenFailed, Detail: oerr.Error(), Err: oerr}
				closeQuietly(c)
				return
			}
			loaded[name] = c
		}()
	}
	return loaded, failures
}

func safeBuild(f Factory) (c contract.Connector, err *LoadError) {
	defer func() {
		if rec := recover(); rec != nil {
			err = &LoadError{Connector: "?", Kind: LoadKindPanic, Detail: fmt.Sprintf("panic while constructing connector: %v", rec)}
		}
	}()
	built, berr := f()
	if berr != nil {
		return nil, &LoadError{Connector: "?", Kind: LoadKindOpenFailed, Detail: berr.Error(), Err: berr}
	}
	var d contract.Declaration
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				err = &LoadError{Connector: "?", Kind: LoadKindPanic, Detail: fmt.Sprintf("panic while reading declaration: %v", rec)}
			}
		}()
		d = built.Declare()
	}()
	if err != nil {
		return nil, err
	}
	err = validateDeclaration(d)
	if err != nil {
		return nil, err
	}
	return built, nil
}

func validateDeclaration(d contract.Declaration) *LoadError {
	name := d.Name
	fail := func(kind, detail string, sentinel error) *LoadError {
		return &LoadError{Connector: name, Kind: kind, Detail: detail, Err: sentinel}
	}
	if strings.TrimSpace(d.Name) == "" || strings.ContainsAny(d.Name, " \t\n") {
		return fail(LoadKindMissing, "connector name must be non-empty and contain no whitespace", contract.ErrDeclarationMissing)
	}
	if strings.TrimSpace(d.ConnectorVersion) == "" {
		return fail(LoadKindMissing, "connectorVersion is required", contract.ErrDeclarationMissing)
	}
	if len(d.Capabilities) == 0 {
		return fail(LoadKindMissing, "connector must advertise at least one capability", contract.ErrDeclarationMissing)
	}
	seen := map[contract.Capability]bool{}
	for _, cap := range d.Capabilities {
		switch cap {
		case contract.CapSource, contract.CapSink, contract.CapResumable, contract.CapAtLeastOnce:
		default:
			return fail(LoadKindConflict, fmt.Sprintf("unknown capability %q", cap), contract.ErrDeclarationConflict)
		}
		if seen[cap] {
			return fail(LoadKindConflict, fmt.Sprintf("capability %q advertised more than once", cap), contract.ErrDeclarationConflict)
		}
		seen[cap] = true
	}
	isSource := seen[contract.CapSource]
	isSink := seen[contract.CapSink]
	if !isSource && !isSink {
		return fail(LoadKindConflict, "connector must be a source, a sink, or both", contract.ErrDeclarationConflict)
	}
	if isSource {
		if reasons := contract.ValidateSchema(d.OutputSchema); len(reasons) > 0 {
			return &LoadError{Connector: name, Kind: LoadKindSchema, Detail: "output schema invalid", Reasons: reasons, Err: contract.ErrSchemaIncompatible}
		}
	}
	if isSink {
		if reasons := contract.ValidateSchema(d.InputSchema); len(reasons) > 0 {
			return &LoadError{Connector: name, Kind: LoadKindSchema, Detail: "input schema invalid", Reasons: reasons, Err: contract.ErrSchemaIncompatible}
		}
	}
	if !isSource && d.OutputSchema != nil {
		return fail(LoadKindConflict, "output schema declared without source capability", contract.ErrDeclarationConflict)
	}
	if !isSink && d.InputSchema != nil {
		return fail(LoadKindConflict, "input schema declared without sink capability", contract.ErrDeclarationConflict)
	}
	if seen[contract.CapResumable] && !isSource {
		return fail(LoadKindConflict, "resumable capability requires source capability", contract.ErrDeclarationConflict)
	}
	if seen[contract.CapAtLeastOnce] && !isSink {
		return fail(LoadKindConflict, "at-least-once capability requires sink capability", contract.ErrDeclarationConflict)
	}
	return nil
}

func relevantSchema(d contract.Declaration) *contract.Schema {
	if d.OutputSchema != nil {
		return d.OutputSchema
	}
	return d.InputSchema
}

func closeQuietly(c contract.Connector) {
	if c == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = c.Close()
}
