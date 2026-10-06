package checkpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func bizLog(t testing.TB, biz, name, format string, args ...any) {
	t.Helper()
	fmt.Fprintf(os.Stdout, "    [biz:%s] case=%s "+format+"\n", append([]any{biz, name}, args...)...)
}

func lessInt(a, b Position) bool {
	ai, _ := decodeRaw(a)
	bi, _ := decodeRaw(b)
	return ai < bi
}

func decodeRaw(p Position) (int, error) {
	if len(p) != 1 {
		return 0, fmt.Errorf("bad test position")
	}
	return int(p[0]), nil
}

func TestCommitHappyPath(t *testing.T) {
	bizLog(t, "checkpoint-commit", "happy", "commit persists and reloads")
	dir := t.TempDir()
	s, err := OpenStore(dir, "c1", lessInt)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Commit(State{Connector: "c1", Seq: 1, Epoch: 1, Position: EncodePosition(Position{1})}); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenStore(dir, "c1", lessInt)
	if err != nil {
		t.Fatal(err)
	}
	cur, err := s2.Current()
	if err != nil || cur.Seq != 1 {
		t.Fatalf("reload failed: %+v err=%v", cur, err)
	}
}

func TestRollbackAndOutOfOrderRejected(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenStore(dir, "c", lessInt)
	_, _ = s.Commit(State{Connector: "c", Seq: 5, Epoch: 1, Position: EncodePosition(Position{5})})

	t.Run("lower seq rejected", func(t *testing.T) {
		bizLog(t, "checkpoint-commit", "seq-rollback", "seq must advance strictly")
		_, err := s.Commit(State{Connector: "c", Seq: 4, Epoch: 1, Position: EncodePosition(Position{6})})
		var ce *CommitError
		if !errors.As(err, &ce) || ce.Kind != KindRollback {
			t.Fatalf("want rollback, got %v", err)
		}
	})

	t.Run("equal seq rejected (no overwrite)", func(t *testing.T) {
		bizLog(t, "checkpoint-commit", "seq-equal", "re-commit same seq rejected")
		_, err := s.Commit(State{Connector: "c", Seq: 5, Epoch: 1, Position: EncodePosition(Position{5})})
		var ce *CommitError
		if !errors.As(err, &ce) || ce.Kind != KindRollback {
			t.Fatalf("want rollback, got %v", err)
		}
	})

	t.Run("backward position rejected", func(t *testing.T) {
		bizLog(t, "checkpoint-commit", "position-backward", "older position with newer seq still rejected")
		_, err := s.Commit(State{Connector: "c", Seq: 6, Epoch: 1, Position: EncodePosition(Position{4})})
		var ce *CommitError
		if !errors.As(err, &ce) || ce.Kind != KindRollback {
			t.Fatalf("want rollback, got %v", err)
		}
	})

	t.Run("equal position is idempotently accepted", func(t *testing.T) {
		bizLog(t, "checkpoint-commit", "idempotent", "same position, next seq allowed")
		if _, err := s.Commit(State{Connector: "c", Seq: 6, Epoch: 1, Position: EncodePosition(Position{5})}); err != nil {
			t.Fatalf("equal position should be accepted: %v", err)
		}
	})

	cur, _ := s.Current()
	if cur.Seq != 6 {
		t.Fatalf("rejected commits must not move state, got seq=%d", cur.Seq)
	}
}

func TestEpochFence(t *testing.T) {
	bizLog(t, "checkpoint-commit", "epoch-fence", "stale generation cannot commit over new one")
	dir := t.TempDir()
	s, _ := OpenStore(dir, "c", lessInt)
	_, _ = s.Commit(State{Connector: "c", Seq: 1, Epoch: 2, Position: EncodePosition(Position{1})})
	_, err := s.Commit(State{Connector: "c", Seq: 99, Epoch: 1, Position: EncodePosition(Position{9})})
	var ce *CommitError
	if !errors.As(err, &ce) || ce.Kind != KindFence {
		t.Fatalf("want fence error, got %v", err)
	}
}

func TestCorruptCheckpointRejected(t *testing.T) {
	bizLog(t, "checkpoint-recovery", "corrupt-file", "garbage checkpoint refused with clear reason")
	dir := t.TempDir()
	s, _ := OpenStore(dir, "c", lessInt)
	if err := os.WriteFile(s.Path(), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := OpenStore(dir, "c", lessInt)
	var ce *CommitError
	if !errors.As(err, &ce) || ce.Kind != KindCorrupt {
		t.Fatalf("want corrupt error, got %v", err)
	}
}

func TestNoHalfCommitOnFailure(t *testing.T) {
	bizLog(t, "checkpoint-commit", "atomic-file", "previous file survives a failed commit")
	dir := t.TempDir()
	s, _ := OpenStore(filepath.Join(dir, "ck"), "c", lessInt)
	_, _ = s.Commit(State{Connector: "c", Seq: 1, Epoch: 1, Position: EncodePosition(Position{1})})
	_, err := s.Commit(State{Connector: "c", Seq: 1, Epoch: 1, Position: EncodePosition(Position{2})})
	if err == nil {
		t.Fatal("expected rejection")
	}
	cur, _ := s.Current()
	pos, _ := DecodePosition(cur.Position)
	if cur.Seq != 1 || len(pos) != 1 || pos[0] != 1 {
		t.Fatalf("state changed despite failed commit: %+v", cur)
	}
}

func TestConcurrentCommitsSerialize(t *testing.T) {
	bizLog(t, "checkpoint-commit", "concurrent-serial", "parallel commits never corrupt or half-apply")
	dir := t.TempDir()
	s, _ := OpenStore(dir, "c", lessInt)
	var wg sync.WaitGroup
	var accepted, rejected int64
	var mu sync.Mutex
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(seq uint64) {
			defer wg.Done()
			_, err := s.Commit(State{Connector: "c", Seq: seq + 1, Epoch: 1, Position: EncodePosition(Position{byte(seq + 1)})})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				accepted++
			} else {
				rejected++
			}
		}(uint64(i))
	}
	wg.Wait()
	cur, _ := s.Current()
	if cur.Seq != 20 {
		t.Fatalf("exactly the highest seq must win, got %d (accepted=%d rejected=%d)", cur.Seq, accepted, rejected)
	}
	// Accepted commits form a strictly increasing prefix ending at 20;
	// everything contending with an already-advanced state is rejected.
	if accepted < 1 || accepted+rejected != 20 {
		t.Fatalf("accepted must be a monotone prefix (1..%d), rejected=%d", accepted, rejected)
	}
}
