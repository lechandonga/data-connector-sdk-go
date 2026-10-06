package host

import (
	"errors"
	"testing"

	"github.com/lechandonga/data-connector-sdk-go/connector"
	"github.com/lechandonga/data-connector-sdk-go/localconn"
)

func strptr(s string) *string { return &s }

func validManifest(id string) connector.Manifest {
	return connector.Manifest{
		ID:           id,
		Interface:    connector.InterfaceVersion{Major: 1, Minor: 0},
		Capabilities: []connector.Capability{connector.CapabilityRead, connector.CapabilityResume, connector.CapabilitySchema},
		Schema: &connector.Schema{
			Name: "test",
			Fields: []connector.Field{
				{Name: "line", Type: connector.FieldString, Required: true},
			},
		},
	}
}

func memFactory() connector.Connector { return &localconn.MemoryConnector{} }

// TestContractRejection 覆盖「契约与兼容」业务：重复注册、声明缺失、
// 声明冲突、接口版本不支持、旧版本兼容适配。
func TestContractRejection(t *testing.T) {
	t.Run("重复注册相同声明被拒绝且原因可区分", func(t *testing.T) {
		reg := NewRegistry()
		if err := reg.Register(validManifest("a"), memFactory); err != nil {
			t.Fatalf("first register: %v", err)
		}
		err := reg.Register(validManifest("a"), memFactory)
		var rej *connector.RejectError
		if !errors.As(err, &rej) || rej.Reason != connector.ReasonDuplicateID {
			t.Fatalf("want duplicate_id rejection, got %v", err)
		}
		t.Logf("场景[契约-重复注册]: 拒绝原因=%s", rej.Reason)
	})

	t.Run("声明缺失必填项被拒绝", func(t *testing.T) {
		reg := NewRegistry()
		cases := map[string]connector.Manifest{
			"空ID":   func() connector.Manifest { m := validManifest(""); return m }(),
			"无能力声明": func() connector.Manifest { m := validManifest("x"); m.Capabilities = nil; return m }(),
			"缺read能力": func() connector.Manifest {
				m := validManifest("x")
				m.Capabilities = []connector.Capability{connector.CapabilityResume}
				return m
			}(),
			"声明schema但为nil": func() connector.Manifest { m := validManifest("x"); m.Schema = nil; return m }(),
		}
		for name, m := range cases {
			err := reg.Register(m, memFactory)
			var rej *connector.RejectError
			if !errors.As(err, &rej) || rej.Reason != connector.ReasonMissingField {
				t.Fatalf("%s: want missing_field rejection, got %v", name, err)
			}
			t.Logf("场景[契约-声明缺失/%s]: 拒绝原因=%s 详情=%s", name, rej.Reason, rej.Detail)
		}
	})

	t.Run("同ID不同声明冲突被拒绝", func(t *testing.T) {
		reg := NewRegistry()
		if err := reg.Register(validManifest("a"), memFactory); err != nil {
			t.Fatal(err)
		}
		m := validManifest("a")
		m.Capabilities = []connector.Capability{connector.CapabilityRead}
		err := reg.Register(m, memFactory)
		var rej *connector.RejectError
		if !errors.As(err, &rej) || rej.Reason != connector.ReasonConflictingManifest {
			t.Fatalf("want conflicting_manifest, got %v", err)
		}
		t.Logf("场景[契约-声明冲突]: 拒绝原因=%s", rej.Reason)
	})

	t.Run("接口主版本不支持被明确拒绝", func(t *testing.T) {
		reg := NewRegistry()
		m := validManifest("legacy")
		m.Interface = connector.InterfaceVersion{Major: 0, Minor: 9}
		err := reg.Register(m, memFactory)
		var rej *connector.RejectError
		if !errors.As(err, &rej) || rej.Reason != connector.ReasonUnsupportedInterface {
			t.Fatalf("want unsupported_interface, got %v", err)
		}
		t.Logf("场景[契约-接口不支持]: v0.9 拒绝原因=%s", rej.Reason)
	})

	t.Run("更高Minor版本被拒绝", func(t *testing.T) {
		reg := NewRegistry()
		m := validManifest("future")
		m.Interface = connector.InterfaceVersion{Major: 1, Minor: 1}
		err := reg.Register(m, memFactory)
		var rej *connector.RejectError
		if !errors.As(err, &rej) || rej.Reason != connector.ReasonUnsupportedInterface {
			t.Fatalf("want unsupported_interface, got %v", err)
		}
		t.Logf("场景[契约-未来版本]: v1.1 拒绝原因=%s", rej.Reason)
	})

	t.Run("单个连接器不合契约不影响其它连接器", func(t *testing.T) {
		reg := NewRegistry()
		bad := validManifest("bad")
		bad.Interface = connector.InterfaceVersion{Major: 9, Minor: 9}
		if err := reg.Register(bad, memFactory); err == nil {
			t.Fatal("expected rejection")
		}
		if err := reg.Register(validManifest("good"), memFactory); err != nil {
			t.Fatalf("good connector must load despite bad one: %v", err)
		}
		if _, _, err := reg.Load("good"); err != nil {
			t.Fatalf("load good: %v", err)
		}
		t.Log("场景[契约-故障隔离]: 不合规连接器被拒绝，合规连接器正常装载")
	})
}

// TestSchemaCompatibility 覆盖「模式演进」业务：不兼容变更必须拒绝，
// 新增/缺失字段语义确定。
func TestSchemaCompatibility(t *testing.T) {
	old := connector.Schema{
		Name: "users",
		Fields: []connector.Field{
			{Name: "id", Type: connector.FieldInt, Required: true},
			{Name: "name", Type: connector.FieldString, Required: true},
		},
	}
	t.Run("删除字段拒绝", func(t *testing.T) {
		next := connector.Schema{Name: "users", Fields: old.Fields[:1]}
		if err := CheckSchemaCompatible(old, next); err == nil {
			t.Fatal("want rejection")
		} else {
			t.Logf("场景[模式-删除字段]: %v", err)
		}
	})
	t.Run("字段类型变更拒绝", func(t *testing.T) {
		next := connector.Schema{Name: "users", Fields: []connector.Field{
			{Name: "id", Type: connector.FieldString, Required: true},
			{Name: "name", Type: connector.FieldString, Required: true},
		}}
		if err := CheckSchemaCompatible(old, next); err == nil {
			t.Fatal("want rejection")
		} else {
			t.Logf("场景[模式-类型变更]: %v", err)
		}
	})
	t.Run("新增必需字段拒绝", func(t *testing.T) {
		next := connector.Schema{Name: "users", Fields: append(append([]connector.Field{}, old.Fields...),
			connector.Field{Name: "email", Type: connector.FieldString, Required: true})}
		if err := CheckSchemaCompatible(old, next); err == nil {
			t.Fatal("want rejection")
		} else {
			t.Logf("场景[模式-新增必需字段]: %v", err)
		}
	})
	t.Run("新增可选字段缺省值未声明则拒绝", func(t *testing.T) {
		next := connector.Schema{Name: "users", Fields: append(append([]connector.Field{}, old.Fields...),
			connector.Field{Name: "email", Type: connector.FieldString})}
		if err := CheckSchemaCompatible(old, next); err == nil {
			t.Fatal("want rejection: 不允许静默按零值补默认")
		} else {
			t.Logf("场景[模式-新增可选字段无默认值]: %v", err)
		}
	})
	t.Run("新增带显式默认值的可选字段兼容", func(t *testing.T) {
		next := connector.Schema{Name: "users", Fields: append(append([]connector.Field{}, old.Fields...),
			connector.Field{Name: "email", Type: connector.FieldString, Default: strptr("")})}
		if err := CheckSchemaCompatible(old, next); err != nil {
			t.Fatalf("want compatible, got %v", err)
		}
		t.Log("场景[模式-兼容演进]: 新增带默认值的可选字段被接受")
	})
}
