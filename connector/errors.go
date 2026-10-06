package connector

import "fmt"

// RejectReason 区分宿主拒绝注册/装载连接器的具体原因。
type RejectReason string

const (
	// ReasonDuplicateID 同一 ID 被重复注册。
	ReasonDuplicateID RejectReason = "duplicate_id"
	// ReasonMissingField 声明缺失必填信息（如 ID、接口版本、能力、模式）。
	ReasonMissingField RejectReason = "missing_field"
	// ReasonConflictingManifest 同一 ID 再次注册但声明内容不一致。
	ReasonConflictingManifest RejectReason = "conflicting_manifest"
	// ReasonUnsupportedInterface 接口主版本不受宿主支持，明确拒绝而非按新语义运行。
	ReasonUnsupportedInterface RejectReason = "unsupported_interface"
	// ReasonIncompatibleSchema 数据模式发生不兼容变化。
	ReasonIncompatibleSchema RejectReason = "incompatible_schema"
)

// RejectError 是宿主拒绝连接器时返回的错误，携带可区分的原因与连接器标识。
type RejectError struct {
	Reason      RejectReason
	ConnectorID string
	Detail      string
}

func (e *RejectError) Error() string {
	return fmt.Sprintf("connector %q rejected (%s): %s", e.ConnectorID, e.Reason, e.Detail)
}

// Is 支持 errors.Is 按原因匹配。
func (e *RejectError) Is(target error) bool {
	t, ok := target.(*RejectError)
	return ok && (t.Reason == "" || t.Reason == e.Reason)
}
