package contract

import "fmt"

// RecordViolation describes one record that does not match its declared
// schema. Record-level violations are contract violations attributed to the
// connector that produced the record.
type RecordViolation struct {
	RecordID string
	Field    string
	Kind     string
	Detail   string
}

func (r RecordViolation) Error() string {
	return fmt.Sprintf("record %q violates schema: %s field %q: %s", r.RecordID, r.Kind, r.Field, r.Detail)
}

const (
	ViolationMissingRequired = "missing-required"
	ViolationUnknownField    = "unknown-field"
	ViolationWrongType       = "wrong-type"
)

// ValidateRecord checks one record against s. The semantics are fixed:
//
//   - A value for a field not present in the schema is rejected (no silent
//     pass-through).
//   - A required field missing from the map is rejected (no defaulting).
//   - An optional field missing from the map is allowed; the host inserts an
//     explicit null rather than inventing a value.
//   - Present values must match the declared Go type exactly (no implicit
//     numeric coercion).
func ValidateRecord(s Schema, rec Record) []RecordViolation {
	var out []RecordViolation
	known := map[string]Field{}
	for _, f := range s.Fields {
		known[f.Name] = f
	}
	for name, val := range rec.Values {
		f, ok := known[name]
		if !ok {
			out = append(out, RecordViolation{RecordID: rec.ID, Field: name, Kind: ViolationUnknownField, Detail: "field is not declared in schema"})
			continue
		}
		if val == nil {
			continue
		}
		if !valueMatchesType(f.Type, val) {
			out = append(out, RecordViolation{RecordID: rec.ID, Field: name, Kind: ViolationWrongType, Detail: fmt.Sprintf("declared %s but got %T", f.Type, val)})
		}
	}
	for _, f := range s.Fields {
		if !f.Required {
			continue
		}
		if _, present := rec.Values[f.Name]; !present {
			out = append(out, RecordViolation{RecordID: rec.ID, Field: f.Name, Kind: ViolationMissingRequired, Detail: "required field absent; defaults are not applied"})
		}
	}
	return out
}

// NormalizeRecord returns a copy of rec in which every declared optional
// field absent from the map is present with an explicit nil value. Required
// fields are left untouched. Unknown fields are not copied. The caller must
// validate first; this never hides violations.
func NormalizeRecord(s Schema, rec Record) Record {
	normalized := Record{ID: rec.ID, Values: make(map[string]any, len(s.Fields))}
	for _, f := range s.Fields {
		if v, present := rec.Values[f.Name]; present {
			normalized.Values[f.Name] = v
		} else if !f.Required {
			normalized.Values[f.Name] = nil
		}
	}
	return normalized
}

func valueMatchesType(t FieldType, v any) bool {
	switch t {
	case TypeString:
		_, ok := v.(string)
		return ok
	case TypeInt64:
		_, ok := v.(int64)
		return ok
	case TypeFloat64:
		_, ok := v.(float64)
		return ok
	case TypeBool:
		_, ok := v.(bool)
		return ok
	default:
		return false
	}
}
