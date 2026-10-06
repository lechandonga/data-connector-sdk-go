package host

import (
	"fmt"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// CheckSchemaCompatible 判定 newSchema 相对 oldSchema 是否为兼容演进。
// 规则是确定的，不满足即拒绝，不做任何静默降级：
//  1. 已有字段不得删除、改名或改类型；
//  2. 已有字段的 Required 不得由可选变必需；
//  3. 新增字段必须是可选字段，且必须显式声明 Default，
//     宿主不会用类型零值冒充默认值；
//  4. 模式名必须一致。
func CheckSchemaCompatible(oldSchema, newSchema connector.Schema) error {
	if oldSchema.Name != newSchema.Name {
		return fmt.Errorf("schema name changed: %q -> %q", oldSchema.Name, newSchema.Name)
	}
	newFields := make(map[string]connector.Field, len(newSchema.Fields))
	for _, f := range newSchema.Fields {
		newFields[f.Name] = f
	}
	oldFields := make(map[string]connector.Field, len(oldSchema.Fields))
	for _, f := range oldSchema.Fields {
		oldFields[f.Name] = f
		nf, ok := newFields[f.Name]
		if !ok {
			return fmt.Errorf("field %q removed", f.Name)
		}
		if nf.Type != f.Type {
			return fmt.Errorf("field %q type changed: %s -> %s", f.Name, f.Type, nf.Type)
		}
		if !f.Required && nf.Required {
			return fmt.Errorf("field %q changed from optional to required", f.Name)
		}
	}
	for _, f := range newSchema.Fields {
		if _, ok := oldFields[f.Name]; ok {
			continue
		}
		if f.Required {
			return fmt.Errorf("new field %q is required; only optional fields may be added", f.Name)
		}
		if f.Default == nil {
			return fmt.Errorf("new optional field %q must declare an explicit default", f.Name)
		}
	}
	return nil
}
