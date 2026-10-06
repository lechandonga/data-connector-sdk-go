package host

import "fmt"

// ErrorKind 区分一次运行失败的来源，便于定位与告警。
type ErrorKind string

const (
	// KindConnector 连接器自身返回的错误（Open/Read/Close）。
	KindConnector ErrorKind = "connector_error"
	// KindContract 连接器违反契约（如记录序号回退、缺少必需字段）。
	KindContract ErrorKind = "contract_violation"
	// KindTimeout 连接器超过截止时间仍未完成。
	KindTimeout ErrorKind = "timeout"
	// KindPanic 连接器执行中 panic，已被宿主收敛。
	KindPanic ErrorKind = "panic"
	// KindHost 宿主自身动作导致的终止（如主动取消、存储失败）。
	KindHost ErrorKind = "host_action"
)

// RunError 是一次连接器运行失败的结构化错误。
type RunError struct {
	Kind        ErrorKind
	ConnectorID string
	// RunID 是本次运行的唯一标识，用于日志关联。
	RunID string
	// PanicValue 仅在 Kind == KindPanic 时有值。
	PanicValue any
	Err        error
}

func (e *RunError) Error() string {
	base := fmt.Sprintf("run %s connector %q failed (%s)", e.RunID, e.ConnectorID, e.Kind)
	if e.Kind == KindPanic {
		return fmt.Sprintf("%s: panic: %v", base, e.PanicValue)
	}
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", base, e.Err)
	}
	return base
}

func (e *RunError) Unwrap() error { return e.Err }

// Is 支持 errors.Is 按 ErrorKind 匹配。
func (e *RunError) Is(target error) bool {
	t, ok := target.(*RunError)
	return ok && (t.Kind == "" || t.Kind == e.Kind)
}
