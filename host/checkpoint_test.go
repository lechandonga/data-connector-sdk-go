package host

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// TestCheckpointStoreRejection 覆盖「检查点一致性」业务：
// 回退、跳号、损坏均被明确拒绝并给出原因。
func TestCheckpointStoreRejection(t *testing.T) {
	dir := t.TempDir()
	store, err := NewCheckpointStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Run("序号回退被拒绝", func(t *testing.T) {
		if err := store.Commit("c", connector.Checkpoint{Seq: 5}); err != nil {
			t.Fatal(err)
		}
		err := store.Commit("c", connector.Checkpoint{Seq: 3})
		var ce *CheckpointError
		if !errors.As(err, &ce) || ce.Reason != ReasonRegression {
			t.Fatalf("want regression rejection, got %v", err)
		}
		t.Logf("场景[检查点-回退]: 拒绝原因=%s", ce.Reason)
	})
	t.Run("重复提交同序号被拒绝", func(t *testing.T) {
		err := store.Commit("c", connector.Checkpoint{Seq: 5})
		var ce *CheckpointError
		if !errors.As(err, &ce) || ce.Reason != ReasonRegression {
			t.Fatalf("want regression rejection, got %v", err)
		}
		t.Logf("场景[检查点-重复提交]: 拒绝原因=%s", ce.Reason)
	})
	t.Run("越过未确认记录的跳号被拒绝", func(t *testing.T) {
		err := store.Commit("c", connector.Checkpoint{Seq: 9})
		var ce *CheckpointError
		if !errors.As(err, &ce) || ce.Reason != ReasonGap {
			t.Fatalf("want gap rejection, got %v", err)
		}
		t.Logf("场景[检查点-跳号]: 拒绝原因=%s", ce.Reason)
	})
	t.Run("损坏文件被拒绝且说明原因", func(t *testing.T) {
		if err := store.Commit("d", connector.Checkpoint{Seq: 1}); err != nil {
			t.Fatal(err)
		}
		// 找到持久化文件并破坏内容。
		matches, _ := filepath.Glob(filepath.Join(dir, "*-d.json"))
		if len(matches) != 1 {
			t.Fatalf("expect 1 checkpoint file, got %v", matches)
		}
		if err := os.WriteFile(matches[0], []byte("{not-json"), 0o644); err != nil {
			t.Fatal(err)
		}
		_, err := store.Load("d")
		var ce *CheckpointError
		if !errors.As(err, &ce) || ce.Reason != ReasonCorrupt {
			t.Fatalf("want corrupt rejection, got %v", err)
		}
		t.Logf("场景[检查点-损坏]: 拒绝原因=%s", ce.Reason)
	})
}

// TestCheckpointConcurrentCommit 覆盖并发提交不出现半提交状态。
func TestCheckpointConcurrentCommit(t *testing.T) {
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const n = 50
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 所有 goroutine 都尝试提交各自序号；串行化后只有按序者成功。
			errs[i] = store.Commit("c", connector.Checkpoint{Seq: uint64(i + 1)})
		}(i)
	}
	wg.Wait()
	cp, err := store.Load("c")
	if err != nil {
		t.Fatalf("load after concurrent commits: %v", err)
	}
	if cp.Seq < 1 || cp.Seq > n {
		t.Fatalf("committed seq %d out of range", cp.Seq)
	}
	t.Logf("场景[检查点-并发提交]: %d 个并发提交后状态一致 seq=%d", n, cp.Seq)
}
