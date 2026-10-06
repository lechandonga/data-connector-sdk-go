package host

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// CheckpointRejectReason 区分检查点被拒绝的原因。
type CheckpointRejectReason string

const (
	// ReasonRegression 提交的序号不大于已提交序号（回退或重复）。
	ReasonRegression CheckpointRejectReason = "regression"
	// ReasonGap 提交跳过了尚未确认消费的记录（乱序/越级提交）。
	ReasonGap CheckpointRejectReason = "gap"
	// ReasonCorrupt 持久化的检查点文件损坏（JSON 或校验和不匹配）。
	ReasonCorrupt CheckpointRejectReason = "corrupt"
	// ReasonSchemaDrift 恢复时模式哈希与已提交检查点不一致。
	ReasonSchemaDrift CheckpointRejectReason = "schema_drift"
)

// CheckpointError 描述一次检查点提交/加载失败。
type CheckpointError struct {
	Reason      CheckpointRejectReason
	ConnectorID string
	Detail      string
}

func (e *CheckpointError) Error() string {
	return fmt.Sprintf("checkpoint for connector %q rejected (%s): %s", e.ConnectorID, e.Reason, e.Detail)
}

func (e *CheckpointError) Is(target error) bool {
	t, ok := target.(*CheckpointError)
	return ok && (t.Reason == "" || t.Reason == e.Reason)
}

// checkpointFile 是磁盘上的持久化格式，带校验和防止半写与损坏。
type checkpointFile struct {
	Checkpoint connector.Checkpoint `json:"checkpoint"`
	Checksum   string               `json:"checksum"`
}

// CheckpointStore 在本地目录按连接器分别持久化检查点。
// 提交经互斥锁串行化并采用 tmp+rename 原子落盘，
// 并发提交、取消与恢复不会观察到半提交状态。
type CheckpointStore struct {
	dir string
	mu  sync.Mutex
}

func NewCheckpointStore(dir string) (*CheckpointStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &CheckpointStore{dir: dir}, nil
}

func (s *CheckpointStore) path(id string) string {
	sum := sha256.Sum256([]byte(id))
	return filepath.Join(s.dir, hex.EncodeToString(sum[:8])+"-"+filepath.Base(id)+".json")
}

// Load 读取连接器最近一次已提交的检查点；不存在时返回零值 Checkpoint。
// 文件损坏时返回 Reason 为 ReasonCorrupt 的 *CheckpointError，绝不静默从头开始。
func (s *CheckpointStore) Load(id string) (connector.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	b, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return connector.Checkpoint{}, nil
	}
	if err != nil {
		return connector.Checkpoint{}, err
	}
	var f checkpointFile
	if err := json.Unmarshal(b, &f); err != nil {
		return connector.Checkpoint{}, &CheckpointError{Reason: ReasonCorrupt, ConnectorID: id,
			Detail: "file is not valid JSON: " + err.Error()}
	}
	if f.Checksum != checksumOf(f.Checkpoint) {
		return connector.Checkpoint{}, &CheckpointError{Reason: ReasonCorrupt, ConnectorID: id,
			Detail: "checksum mismatch"}
	}
	return f.Checkpoint, nil
}

// Commit 原子地提交检查点。规则：
//   - Seq 必须严格大于已提交 Seq，否则按 ReasonRegression 拒绝（回退/重复）；
//   - expect 非 nil 时，Seq 必须恰好等于 expect.Seq+1 的连续推进语义由 Runner 保证，
//     这里额外防御乱序：若 Seq 相对已提交值出现跳号，按 ReasonGap 拒绝。
//
// 跳号判定：当 committed.Seq > 0 且 cp.Seq > committed.Seq+1 时视为越过了未确认记录。
func (s *CheckpointStore) Commit(id string, cp connector.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	committed, err := s.loadLocked(id)
	if err != nil {
		return err
	}
	if cp.Seq <= committed.Seq {
		return &CheckpointError{Reason: ReasonRegression, ConnectorID: id,
			Detail: fmt.Sprintf("seq %d is not ahead of committed seq %d", cp.Seq, committed.Seq)}
	}
	if committed.Seq > 0 && cp.Seq > committed.Seq+1 {
		return &CheckpointError{Reason: ReasonGap, ConnectorID: id,
			Detail: fmt.Sprintf("seq %d skips unacked records after committed seq %d", cp.Seq, committed.Seq)}
	}
	return s.writeLocked(id, cp)
}

// Force 用于测试或运维显式写入检查点，仍保持原子落盘。
func (s *CheckpointStore) Force(id string, cp connector.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(id, cp)
}

func (s *CheckpointStore) loadLocked(id string) (connector.Checkpoint, error) {
	b, err := os.ReadFile(s.path(id))
	if errors.Is(err, os.ErrNotExist) {
		return connector.Checkpoint{}, nil
	}
	if err != nil {
		return connector.Checkpoint{}, err
	}
	var f checkpointFile
	if err := json.Unmarshal(b, &f); err != nil {
		return connector.Checkpoint{}, &CheckpointError{Reason: ReasonCorrupt, ConnectorID: id,
			Detail: "file is not valid JSON: " + err.Error()}
	}
	if f.Checksum != checksumOf(f.Checkpoint) {
		return connector.Checkpoint{}, &CheckpointError{Reason: ReasonCorrupt, ConnectorID: id,
			Detail: "checksum mismatch"}
	}
	return f.Checkpoint, nil
}

func (s *CheckpointStore) writeLocked(id string, cp connector.Checkpoint) error {
	f := checkpointFile{Checkpoint: cp, Checksum: checksumOf(cp)}
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	p := s.path(id)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}

func checksumOf(cp connector.Checkpoint) string {
	b, _ := json.Marshal(cp)
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
