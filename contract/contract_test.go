package contract

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func bizLog(t testing.TB, biz, name, format string, args ...any) {
	t.Helper()
	fmt.Fprintf(os.Stdout, "    [biz:%s] case=%s "+format+"\n", append([]any{biz, name}, args...)...)
}

func TestSchemaEvolution(t *testing.T) {
	base := Schema{Name: "ev", Fields: []Field{
		{Name: "id", Type: TypeInt64, Required: true},
		{Name: "note", Type: TypeString, Required: false},
	}}

	t.Run("add optional field is compatible", func(t *testing.T) {
		bizLog(t, "contract-compatibility", "add-optional", "optional fields may be added, null explicit")
		next := Schema{Name: "ev", Fields: append(append([]Field{}, base.Fields...), Field{Name: "tag", Type: TypeString, Required: false})}
		if got := DiffSchema(base, next); len(got) != 0 {
			t.Fatalf("expected compatible, got %v", got)
		}
	})

	t.Run("add required field is rejected", func(t *testing.T) {
		bizLog(t, "contract-compatibility", "add-required", "new required field must be refused")
		next := Schema{Name: "ev", Fields: append(append([]Field{}, base.Fields...), Field{Name: "tag", Type: TypeString, Required: true})}
		got := DiffSchema(base, next)
		if len(got) != 1 || got[0].Kind != IncompatAddedRequired || got[0].Field != "tag" {
			t.Fatalf("want field-added-required(tag), got %+v", got)
		}
	})

	t.Run("remove field is rejected", func(t *testing.T) {
		bizLog(t, "contract-compatibility", "remove-field", "dropping data is never silent")
		next := Schema{Name: "ev", Fields: []Field{base.Fields[0]}}
		got := DiffSchema(base, next)
		if len(got) != 1 || got[0].Kind != IncompatRemoved {
			t.Fatalf("want field-removed, got %+v", got)
		}
	})

	t.Run("type change is rejected", func(t *testing.T) {
		bizLog(t, "contract-compatibility", "type-change", "int64->string rejected")
		next := Schema{Name: "ev", Fields: []Field{
			{Name: "id", Type: TypeString, Required: true},
			{Name: "note", Type: TypeString, Required: false},
		}}
		got := DiffSchema(base, next)
		if len(got) != 1 || got[0].Kind != IncompatTypeChanged {
			t.Fatalf("want type-changed, got %+v", got)
		}
	})

	t.Run("optional tightened to required is rejected", func(t *testing.T) {
		bizLog(t, "contract-compatibility", "tighten-optional", "optional->required rejected")
		next := Schema{Name: "ev", Fields: []Field{
			{Name: "id", Type: TypeInt64, Required: true},
			{Name: "note", Type: TypeString, Required: true},
		}}
		got := DiffSchema(base, next)
		if len(got) != 1 || got[0].Kind != IncompatOptionalToRequired {
			t.Fatalf("want optional-to-required, got %+v", got)
		}
	})

	t.Run("required relaxed to optional is compatible", func(t *testing.T) {
		bizLog(t, "contract-compatibility", "relax-required", "required->optional allowed")
		next := Schema{Name: "ev", Fields: []Field{
			{Name: "id", Type: TypeInt64, Required: false},
			{Name: "note", Type: TypeString, Required: false},
		}}
		if got := DiffSchema(base, next); len(got) != 0 {
			t.Fatalf("expected compatible, got %v", got)
		}
	})
}

func TestRecordContract(t *testing.T) {
	s := Schema{Name: "r", Fields: []Field{
		{Name: "id", Type: TypeInt64, Required: true},
		{Name: "note", Type: TypeString, Required: false},
	}}

	t.Run("missing required rejected without defaulting", func(t *testing.T) {
		bizLog(t, "contract-records", "missing-required", "no silent defaults")
		got := ValidateRecord(s, Record{ID: "x", Values: map[string]any{}})
		if len(got) != 1 || got[0].Kind != ViolationMissingRequired {
			t.Fatalf("want missing-required, got %+v", got)
		}
	})

	t.Run("unknown field rejected", func(t *testing.T) {
		bizLog(t, "contract-records", "unknown-field", "undeclared field rejected")
		got := ValidateRecord(s, Record{ID: "x", Values: map[string]any{"id": int64(1), "boom": true}})
		if len(got) != 1 || got[0].Kind != ViolationUnknownField {
			t.Fatalf("want unknown-field, got %+v", got)
		}
	})

	t.Run("wrong type rejected, no coercion", func(t *testing.T) {
		bizLog(t, "contract-records", "wrong-type", "int passed as int64? exact types only")
		got := ValidateRecord(s, Record{ID: "x", Values: map[string]any{"id": 1}})
		if len(got) != 1 || got[0].Kind != ViolationWrongType {
			t.Fatalf("want wrong-type, got %+v", got)
		}
	})

	t.Run("missing optional becomes explicit null", func(t *testing.T) {
		bizLog(t, "contract-records", "optional-null", "normalized record carries null")
		rec := NormalizeRecord(s, Record{ID: "x", Values: map[string]any{"id": int64(1)}})
		if v, ok := rec.Values["note"]; !ok || v != nil {
			t.Fatalf("expected explicit nil note, got %#v", rec.Values)
		}
		if err := ValidateSchema(&s); len(err) != 0 {
			t.Fatalf("schema invalid: %v", err)
		}
	})
}

func TestValidateSchemaRejectsBadDeclarations(t *testing.T) {
	bizLog(t, "contract-schema", "bad-declarations", "empty name, dup fields, unknown types rejected")
	if got := ValidateSchema(nil); len(got) == 0 {
		t.Fatal("nil schema must be invalid")
	}
	s := Schema{Fields: []Field{
		{Name: "a", Type: FieldType("weird")},
		{Name: "a", Type: TypeString},
	}}
	got := ValidateSchema(&s)
	kinds := map[string]bool{}
	for _, in := range got {
		kinds[in.Kind] = true
	}
	if !kinds[IncompatBadType] || !kinds[IncompatDuplicateField] || !kinds[IncompatEmptySchema] {
		t.Fatalf("missing expected reasons, got %+v", got)
	}
	_ = errors.Is
}
