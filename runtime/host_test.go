package runtime

import (
	"context"
	"testing"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/connector/std"
	"github.com/lechandonga/data-connector-sdk-go/contract"
)

func TestMultiConnectorIsolationAndConcurrency(t *testing.T) {
	bizLog(t, "multi-connector", "concurrent-isolated", "good + stuck + panicking connectors run independently")
	dir := t.TempDir()
	cfg := testConfig(dir)

	good := std.NewMemorySource("good", "1", []map[string]any{
		{"payload": "g1"}, {"payload": "g2"}, {"payload": "g3"},
	})
	stuck := ctrlConnector("stuck", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		select {} // never responds
	})
	panicker := ctrlConnector("panicker", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		panic("die")
	})

	goodDown := &sliceDownstream{}
	host := NewHost(cfg)
	mustRegister(t, host, func() (contract.Connector, error) { return good, nil })
	mustRegister(t, host, func() (contract.Connector, error) { return stuck, nil })
	mustRegister(t, host, func() (contract.Connector, error) { return panicker, nil })

	ctx, cancel := context.WithCancel(context.Background())
	runners, fails := host.Run(ctx,
		map[string]Downstream{"good": goodDown, "stuck": &sliceDownstream{}, "panicker": &sliceDownstream{}},
		map[string]checkpoint.PositionLess{"good": std.PositionLess})
	if len(fails) != 0 {
		t.Fatalf("unexpected load failures: %v", fails)
	}

	// Good connector drains fully despite its two neighbours failing.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if goodDown.len() == 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if goodDown.len() != 3 {
		t.Fatalf("good connector affected by neighbours, delivered %d", goodDown.len())
	}

	waitFailure(t, runners["stuck"], 3*time.Second)
	waitFailure(t, runners["panicker"], 3*time.Second)
	if runners["stuck"].Failure().Class != ClassTimeout {
		t.Fatalf("stuck should time out: %+v", runners["stuck"].Failure())
	}
	if runners["panicker"].Failure().Class != ClassPanic {
		t.Fatalf("panicker should panic: %+v", runners["panicker"].Failure())
	}
	if runners["good"].Failure() != nil {
		t.Fatalf("good connector should complete cleanly: %+v", runners["good"].Failure())
	}

	stats := host.Stats()
	bizLog(t, "multi-connector", "stats", "good delivered=%d; stuck timeouts=%d; panicker panics=%d",
		stats["good"].RecordsDelivered, stats["stuck"].Timeouts, stats["panicker"].Panics)
	if stats["good"].RecordsDelivered != 3 || stats["stuck"].Timeouts == 0 || stats["panicker"].Panics == 0 {
		t.Fatalf("isolated stats wrong: %+v", stats)
	}

	cancel()
	host.Stop(time.Second)
}

func TestCancellationPropagatesToRead(t *testing.T) {
	bizLog(t, "multi-connector", "cancellation", "host cancel reaches connector read chain")
	dir := t.TempDir()
	cfg := testConfig(dir)

	cooperative := ctrlConnector("coop", func(ctx contract.SourceContext, max int) (contract.Batch, error) {
		<-ctx.Done()
		return contract.Batch{}, ctx.Err()
	})
	host := NewHost(cfg)
	mustRegister(t, host, func() (contract.Connector, error) { return cooperative, nil })
	ctx, cancel := context.WithCancel(context.Background())
	runners, _ := host.Run(ctx, map[string]Downstream{"coop": &sliceDownstream{}}, nil)

	cancel()
	select {
	case <-runners["coop"].Done():
	case <-time.After(2 * time.Second):
		t.Fatal("cooperative connector did not stop after host cancellation")
	}
	// Cancellation is the terminal reason, but Stop does not report it as an
	// error failure (it is a host action).
	out := host.Stop(time.Second)
	if _, ok := out["coop"]; ok {
		t.Fatalf("clean cancellation must not be reported as failure: %+v", out)
	}
}

func mustRegister(t testing.TB, host *Host, f func() (contract.Connector, error)) {
	t.Helper()
	if err := host.Registry().Register(f); err != nil {
		t.Fatalf("register: %v", err)
	}
}
