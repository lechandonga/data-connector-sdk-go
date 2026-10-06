package std

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

func bizLog(t testing.TB, biz, name, format string, args ...any) {
	t.Helper()
	fmt.Printf("    [biz:%s] case=%s "+format+"\n", append([]any{biz, name}, args...)...)
}

func writeNDJSON(t *testing.T, path string, n int) {
	t.Helper()
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	for i := 0; i < n; i++ {
		if _, err := fmt.Fprintf(f, "{\"id\":\"r%d\",\"payload\":\"m%d\"}\n", i, i); err != nil {
			t.Fatal(err)
		}
	}
}

type doneCtx struct{}

func (doneCtx) Done() <-chan struct{} { return make(chan struct{}) }
func (doneCtx) Err() error            { return nil }

func TestFileSourceResumeSkipsConsumed(t *testing.T) {
	bizLog(t, "builtin-source", "file-resume", "Resume(n) starts at record n exactly")
	dir := t.TempDir()
	path := filepath.Join(dir, "in.ndjson")
	writeNDJSON(t, path, 5)

	s := NewFileSource("f", "1", path)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	}
	if err := s.Resume(encodePosition(3)); err != nil {
		t.Fatal(err)
	}
	batch, err := s.Read(doneCtx{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch.Records) != 2 || batch.Records[0].Record.ID != "r3" || batch.Records[1].Record.ID != "r4" {
		t.Fatalf("resume read wrong records: %+v", batch.Records)
	}
	if _, err := s.Read(doneCtx{}, 10); err == nil {
		t.Fatal("exhausted file source must return EOF")
	}
	_ = s.Close()
}

func TestMemorySourceResume(t *testing.T) {
	bizLog(t, "builtin-source", "memory-resume", "in-memory source honors resume and EOFs")
	m := NewMemorySource("m", "1", []map[string]any{
		{"payload": "a"}, {"payload": "b"}, {"payload": "c"},
	})
	if err := m.Resume(encodePosition(2)); err != nil {
		t.Fatal(err)
	}
	b, err := m.Read(doneCtx{}, 10)
	if err != nil || len(b.Records) != 1 || b.Records[0].Record.ID != "2" {
		t.Fatalf("resume wrong: %+v err=%v", b, err)
	}
	if _, err := m.Read(doneCtx{}, 10); err == nil {
		t.Fatal("expected EOF")
	}
}

type sinkCtx struct{}

func (sinkCtx) Done() <-chan struct{} { return make(chan struct{}) }
func (sinkCtx) Err() error            { return nil }

func TestMemorySinkDedupesCommittedReplays(t *testing.T) {
	bizLog(t, "builtin-sink", "memory-dedupe", "replayed ids after commit are applied once")
	s := NewMemorySink("sink", "1")
	mkBatch := func(ids ...string) contract.Batch {
		var b contract.Batch
		for _, id := range ids {
			b.Records = append(b.Records, contract.Envelope{Record: contract.Record{ID: id, Values: map[string]any{"id": id}}})
		}
		return b
	}
	if err := s.Write(sinkCtx{}, mkBatch("a", "b")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(sinkCtx{}); err != nil {
		t.Fatal(err)
	}
	if err := s.Write(sinkCtx{}, mkBatch("b", "c")); err != nil {
		t.Fatal(err)
	}
	if err := s.Commit(sinkCtx{}); err != nil {
		t.Fatal(err)
	}
	got := s.Records()
	if len(got) != 3 || got[0].ID != "a" || got[1].ID != "b" || got[2].ID != "c" {
		t.Fatalf("dedupe result wrong: %+v", got)
	}
}
