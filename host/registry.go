package host

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// HostInterface 是宿主当前实现的契约接口版本。
// Major 相同的连接器被接受；Minor 低于宿主的按旧语义兼容适配（见适配规则），
// Minor 高于宿主或 Major 不同的被明确拒绝。
var HostInterface = connector.InterfaceVersion{Major: 1, Minor: 0}

// Factory 由第三方提供，每次运行创建一个连接器实例，保证实例间状态隔离。
type Factory func() connector.Connector

type registration struct {
	manifest connector.Manifest
	factory  Factory
	// adapted 为 true 表示该连接器按旧 Minor 版本被兼容适配。
	adapted bool
}

// Registry 校验并登记连接器声明。单个连接器不符合契约只会使其自身被拒绝，
// 不影响其它连接器的注册与装载。
type Registry struct {
	mu   sync.Mutex
	regs map[string]*registration
}

func NewRegistry() *Registry {
	return &Registry{regs: make(map[string]*registration)}
}

// Register 校验声明并注册连接器。失败时返回 *connector.RejectError，
// 可通过 Reason 字段区分重复注册、声明缺失、声明冲突、接口不支持等原因。
func (r *Registry) Register(m connector.Manifest, f Factory) error {
	if err := validateManifest(m); err != nil {
		return err
	}
	if f == nil {
		return &connector.RejectError{Reason: connector.ReasonMissingField, ConnectorID: m.ID, Detail: "factory is nil"}
	}
	adapted, err := checkInterface(m)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.regs[m.ID]; ok {
		if manifestsEqual(prev.manifest, m) {
			return &connector.RejectError{Reason: connector.ReasonDuplicateID, ConnectorID: m.ID,
				Detail: "identical manifest registered twice"}
		}
		return &connector.RejectError{Reason: connector.ReasonConflictingManifest, ConnectorID: m.ID,
			Detail: "a different manifest is already registered under this ID"}
	}
	r.regs[m.ID] = &registration{manifest: m, factory: f, adapted: adapted}
	return nil
}

func validateManifest(m connector.Manifest) error {
	if m.ID == "" {
		return &connector.RejectError{Reason: connector.ReasonMissingField, Detail: "manifest id is empty"}
	}
	if len(m.Capabilities) == 0 {
		return &connector.RejectError{Reason: connector.ReasonMissingField, ConnectorID: m.ID, Detail: "no capabilities declared"}
	}
	if !m.HasCapability(connector.CapabilityRead) {
		return &connector.RejectError{Reason: connector.ReasonMissingField, ConnectorID: m.ID, Detail: "read capability is required"}
	}
	if m.HasCapability(connector.CapabilitySchema) {
		if m.Schema == nil {
			return &connector.RejectError{Reason: connector.ReasonMissingField, ConnectorID: m.ID,
				Detail: "schema capability declared but schema is nil"}
		}
		seen := map[string]bool{}
		for _, f := range m.Schema.Fields {
			if f.Name == "" {
				return &connector.RejectError{Reason: connector.ReasonMissingField, ConnectorID: m.ID,
					Detail: "schema field with empty name"}
			}
			if seen[f.Name] {
				return &connector.RejectError{Reason: connector.ReasonConflictingManifest, ConnectorID: m.ID,
					Detail: fmt.Sprintf("duplicate schema field %q", f.Name)}
			}
			seen[f.Name] = true
		}
	}
	return nil
}

// checkInterface 判定接口版本是否受支持。
// 返回 adapted=true 表示连接器按旧 Minor 版本被兼容适配：
// 旧 Minor 缺失的可选行为（目前无）一律按宿主默认语义确定性地补齐，
// 绝不按新语义含糊执行旧连接器不认识的特性。
func checkInterface(m connector.Manifest) (adapted bool, err error) {
	v := m.Interface
	if v.Major != HostInterface.Major {
		return false, &connector.RejectError{Reason: connector.ReasonUnsupportedInterface, ConnectorID: m.ID,
			Detail: fmt.Sprintf("interface %s not supported by host %s", v, HostInterface)}
	}
	if v.Minor > HostInterface.Minor {
		return false, &connector.RejectError{Reason: connector.ReasonUnsupportedInterface, ConnectorID: m.ID,
			Detail: fmt.Sprintf("interface %s is newer than host %s", v, HostInterface)}
	}
	return v.Minor < HostInterface.Minor, nil
}

func manifestsEqual(a, b connector.Manifest) bool {
	ab, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(ab) == string(bb)
}

// Load 返回指定连接器的注册信息，供 Runner 使用。
func (r *Registry) Load(id string) (connector.Manifest, Factory, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	reg, ok := r.regs[id]
	if !ok {
		return connector.Manifest{}, nil, &connector.RejectError{Reason: connector.ReasonMissingField, ConnectorID: id,
			Detail: "connector not registered"}
	}
	return reg.manifest, reg.factory, nil
}

// IDs 返回当前已注册连接器的 ID 列表。
func (r *Registry) IDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, 0, len(r.regs))
	for id := range r.regs {
		ids = append(ids, id)
	}
	return ids
}
