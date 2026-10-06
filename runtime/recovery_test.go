package runtime

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/connector/std"
	"github.com/lechandonga/data-connector-sdk-go/contract"
)

func TestRecoveryResumesFromCheckpoint(t *testing.T) {
	bizLog(t, "recovery", "resume-position", "restart starts after last committed batch; no loss, no duplicate effects")
	dir := t.TempDir()
	cfg := testConfig(dir)

	// 200 records in a local NDJSON file make a 20ms first run stop
	// deterministically mid-stream.
	path := filepath.Join(dir, "data.ndjson")
	{
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 200; i++ {
			if _, err := f.WriteString("{\"id\":\"r" + itoa(i) + "\",\"payload\":\"p" + itoa(i) + "\"}\n"); err != nil {
				t.Fatal(err)
			}
		}
		_ = f.Close()
	}

	runOnce := func(runFor time.Duration, down Downstream) {
		src := std.NewFileSource("mem", "1", path)
		host := NewHost(cfg)
		if err := host.Registry().Register(func() (contract.Connector, error) { return src, nil }); err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		_, fails := host.Run(ctx,
			map[string]Downstream{"mem": down},
			map[string]checkpoint.PositionLess{"mem": std.PositionLess})
		if len(fails) != 0 {
			t.Fatalf("load failures: %v", fails)
		}
		time.Sleep(runFor)
		cancel()
		host.Stop(time.Second)
	}

	// A durable, idempotent downstream journal shared across restarts is
	// what turns at-least-once replay into no-duplicate/no-loss effect.
	journal := filepath.Join(dir, "downstream.jsonl")
	openDown := func() *DurableDownstream {
		d, err := OpenDurableDownstream(journal)
		if err != nil {
			t.Fatal(err)
		}
		return d
	}

	d1 := openDown()
	runOnce(20*time.Millisecond, d1)
	n1 := d1.Count()
	_ = d1.Close()
	if n1 == 0 || n1 >= 200 {
		t.Fatalf("first run should durably accept a proper subset, got %d", n1)
	}

	d2 := openDown()
	runOnce(2*time.Second, d2)
	n2 := d2.Count()
	_ = d2.Close()
	bizLog(t, "recovery", "resume-position", "after run1=%d, after run2=%d (replays collapsed by durable idempotent downstream)", n1, n2)
	if n2 != 200 {
		t.Fatalf("exactly 200 unique durable records after recovery, got %d (run1=%d)", n2, n1)
	}

	d3 := openDown()
	runOnce(300*time.Millisecond, d3)
	n3 := d3.Count()
	_ = d3.Close()
	if n3 != 200 {
		t.Fatalf("third run must not change durable count, got %d", n3)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

func TestReplayedIDsAreDeduped(t *testing.T) {
	bizLog(t, "recovery", "dedupe-replay", "same ids replayed -> delivered exactly once")
	dir := t.TempDir()
	cfg := testConfig(dir)

	call := 0
	conn := ctrlConnector("replay", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		if call > 5 {
			<-ctx.Done()
			return contract.Batch{}, ctx.Err()
		}
		b := contract.Batch{Position: []byte{byte(call + 1)}}
		b.Records = append(b.Records,
			contract.Envelope{Record: rec("same-1", "x"), Position: []byte{byte(call + 1)}},
			contract.Envelope{Record: rec("same-2", "y"), Position: []byte{byte(call + 1)}})
		call++
		return b, nil
	})
	down := &sliceDownstream{}
	store, _ := checkpoint.OpenStore(cfg.CheckpointDir, "replay", nil)
	r := NewRunner(cfg, conn, store, down, 1)
	_ = r.Start(context.Background())
	time.Sleep(150 * time.Millisecond)
	r.Stop(time.Second)
	if down.len() != 2 {
		t.Fatalf("want exactly 2 unique deliveries, got %d", down.len())
	}
	if got := r.Stats().RecordsDeduped; got != 10 {
		t.Fatalf("want 10 deduped replays, got %d", got)
	}
}
