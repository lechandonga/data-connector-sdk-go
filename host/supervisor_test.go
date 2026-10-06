package host

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// TestConcurrentConnectors 覆盖「多连接器并发」业务：并发运行时
// 进度、统计互不干扰；一个连接器 panic 不影响其它连接器完成。
func TestConcurrentConnectors(t *testing.T) {
	reg := NewRegistry()
	store, err := NewCheckpointStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const perConn = 20
	ids := []string{"c1", "c2", "c3"}
	for _, id := range ids {
		n := perConn
		if err := reg.Register(validManifest(id), func() connector.Connector {
			return &replayConnector{total: n}
		}); err != nil {
			t.Fatal(err)
		}
	}
	// 一个会 panic 的连接器与正常连接器混跑。
	if err := reg.Register(validManifest("bad"), func() connector.Connector {
		return &panicConnector{msg: "isolated boom"}
	}); err != nil {
		t.Fatal(err)
	}
	all := append(append([]string{}, ids...), "bad")

	sup := NewSupervisor(reg, store, Options{BufferSize: 2, ReadTimeout: 10 * time.Second, CancelGrace: time.Second})
	sinks := map[string]*collectSink{}
	var mu sync.Mutex
	results := sup.RunAll(context.Background(), all, func(id string) Sink {
		s := &collectSink{}
		mu.Lock()
		sinks[id] = s
		mu.Unlock()
		return s
	})

	byID := map[string]Result{}
	for _, r := range results {
		byID[r.ConnectorID] = r
	}
	for _, id := range ids {
		r := byID[id]
		if r.Err != nil {
			t.Fatalf("connector %s: %v", id, r.Err)
		}
		if r.Stats.RecordsDelivered != perConn {
			t.Fatalf("connector %s delivered %d, want %d", id, r.Stats.RecordsDelivered, perConn)
		}
		cp, err := store.Load(id)
		if err != nil || cp.Seq != perConn {
			t.Fatalf("connector %s checkpoint = %+v, err=%v", id, cp, err)
		}
		if got := len(sinks[id].seqs()); got != perConn {
			t.Fatalf("connector %s sink got %d records", id, got)
		}
		t.Logf("场景[并发-独立进度]: %s 投递=%d 检查点=%d 运行ID=%s",
			id, r.Stats.RecordsDelivered, cp.Seq, r.Stats.RunID)
	}
	var re *RunError
	if r := byID["bad"]; !errors.As(r.Err, &re) || re.Kind != KindPanic {
		t.Fatalf("bad connector should fail with KindPanic, got %v", r.Err)
	} else {
		t.Logf("场景[并发-故障隔离]: bad 以 %s 失败且未波及其它连接器", re.Kind)
	}
}

// TestConcurrentSameSourceDistinctState 验证同一工厂产生的实例状态互不影响。
func TestConcurrentSameSourceDistinctState(t *testing.T) {
	reg := NewRegistry()
	store, _ := NewCheckpointStore(t.TempDir())
	for _, id := range []string{"s1", "s2"} {
		if err := reg.Register(validManifest(id), func() connector.Connector {
			return &replayConnector{total: 5}
		}); err != nil {
			t.Fatal(err)
		}
	}
	sup := NewSupervisor(reg, store, Options{BufferSize: 1, ReadTimeout: 5 * time.Second, CancelGrace: time.Second})
	results := sup.RunAll(context.Background(), []string{"s1", "s2"}, func(string) Sink { return &collectSink{} })
	for _, r := range results {
		if r.Err != nil || r.Stats.RecordsDelivered != 5 {
			t.Fatalf("%s: stats=%+v err=%v", r.ConnectorID, r.Stats, r.Err)
		}
	}
	t.Log("场景[并发-状态隔离]: 同工厂两实例各自完整投递 5 条, 统计互不干扰")
}

func ExampleSupervisor_RunAll() {
	reg := NewRegistry()
	dir, _ := os.MkdirTemp("", "ckpt-example")
	defer os.RemoveAll(dir)
	store, _ := NewCheckpointStore(dir)
	_ = reg.Register(validManifest("example"), func() connector.Connector { return &replayConnector{total: 3} })
	sup := NewSupervisor(reg, store, Options{})
	results := sup.RunAll(context.Background(), []string{"example"}, func(string) Sink {
		return SinkFunc(func(_ context.Context, rec connector.Record) error {
			fmt.Println("got", rec.Seq)
			return nil
		})
	})
	fmt.Println("err:", results[0].Err)
	// Output:
	// got 1
	// got 2
	// got 3
	// err: <nil>
}
