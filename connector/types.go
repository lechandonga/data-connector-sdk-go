// Package connector 定义第三方数据连接器需要实现的统一契约：
// 接口版本、能力声明、数据模式（Schema）以及记录与检查点类型。
package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
)

// InterfaceVersion 描述连接器实现所遵循的契约接口版本。
// 宿主按 Major 判定是否支持，按 Minor 判定是否需要兼容适配。
type InterfaceVersion struct {
	Major int `json:"major"`
	Minor int `json:"minor"`
}

func (v InterfaceVersion) String() string { return fmt.Sprintf("v%d.%d", v.Major, v.Minor) }

// Capability 表示连接器声明的能力范围。
type Capability string

const (
	CapabilityRead       Capability = "read"
	CapabilityResume     Capability = "resume"     // 支持从检查点断点续读
	CapabilitySchema     Capability = "schema"     // 产出带固定模式的数据
	CapabilityCancelable Capability = "cancelable" // 声明会响应 ctx 取消
)

// FieldType 是模式字段的类型枚举。
type FieldType string

const (
	FieldString FieldType = "string"
	FieldInt    FieldType = "int"
	FieldFloat  FieldType = "float"
	FieldBool   FieldType = "bool"
	FieldBytes  FieldType = "bytes"
)

// Field 描述模式中的一个字段。
type Field struct {
	Name     string    `json:"name"`
	Type     FieldType `json:"type"`
	Required bool      `json:"required"`
	// Default 仅对可选字段有意义；兼容演进时新增可选字段必须显式给出默认值，
	// 宿主不会静默用零值填充。
	Default *string `json:"default,omitempty"`
}

// Schema 描述连接器产出记录的数据模式。
type Schema struct {
	Name   string  `json:"name"`
	Fields []Field `json:"fields"`
}

// Hash 返回模式的确定性哈希，字段顺序不影响结果。
func (s Schema) Hash() string {
	fields := make([]Field, len(s.Fields))
	copy(fields, s.Fields)
	sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
	norm := struct {
		Name   string  `json:"name"`
		Fields []Field `json:"fields"`
	}{Name: s.Name, Fields: fields}
	b, _ := json.Marshal(norm)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// Manifest 是连接器向宿主提交的声明信息，装载时由宿主校验。
type Manifest struct {
	// ID 是连接器唯一标识，注册表内不可重复。
	ID string `json:"id"`
	// Interface 是连接器实现的契约接口版本。
	Interface InterfaceVersion `json:"interface"`
	// Capabilities 是连接器声明的能力集合。
	Capabilities []Capability `json:"capabilities"`
	// Schema 是产出数据的模式；声明了 CapabilitySchema 时必须提供。
	Schema *Schema `json:"schema,omitempty"`
}

// HasCapability 报告声明中是否包含指定能力。
func (m Manifest) HasCapability(c Capability) bool {
	for _, x := range m.Capabilities {
		if x == c {
			return true
		}
	}
	return false
}

// Record 是连接器产出的一条数据记录。
type Record struct {
	// Seq 是连接器内单调递增的序号：
	// 检查点按 Seq 提交，恢复时 Seq <= 已提交序号的记录会被去重跳过。
	Seq uint64 `json:"seq"`
	// Offset 是连接器自定义的不透明游标，随检查点持久化，恢复时回传。
	Offset string `json:"offset,omitempty"`
	// Data 是记录负载，键应对应 Schema 字段。
	Data map[string]any `json:"data"`
}

// Checkpoint 表示一次已提交的消费进度。
type Checkpoint struct {
	// Seq 是已确认消费的最大记录序号，必须单调递增。
	Seq uint64 `json:"seq"`
	// Offset 是对应记录的连接器游标。
	Offset string `json:"offset,omitempty"`
	// SchemaHash 是提交时连接器声明模式的哈希，用于恢复时检测不兼容演进。
	SchemaHash string `json:"schema_hash,omitempty"`
}

// Config 是宿主在 Open 时传给连接器的配置。
type Config struct {
	// ConnectorID 与 Manifest.ID 一致，便于连接器打日志。
	ConnectorID string
	// Properties 是连接器的自定义配置项。
	Properties map[string]string
}
