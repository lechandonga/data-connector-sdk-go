// Command demo runs two fully local connectors concurrently under the host:
// a file source pumping NDJSON records and an in-memory source, each into an
// idempotent JSONL durable downstream. Stop it with Ctrl-C: checkpoints are
// committed per batch, so restarting resumes from the last committed batch.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/connector/std"
	"github.com/lechandonga/data-connector-sdk-go/contract"
	"github.com/lechandonga/data-connector-sdk-go/runtime"
)

func main() {
	var (
		workDir = flag.String("work", ".host-demo", "host working directory (checkpoints, schemas)")
		input   = flag.String("input", "", "NDJSON input file (generated sample if empty)")
	)
	flag.Parse()

	if err := os.MkdirAll(*workDir, 0o755); err != nil {
		log.Fatal(err)
	}
	inFile := *input
	if inFile == "" {
		inFile = filepath.Join(*workDir, "sample.ndjson")
		if err := os.WriteFile(inFile, []byte(sampleData), 0o644); err != nil {
			log.Fatal(err)
		}
	}

	cfg := runtime.DefaultConfig()
	cfg.CheckpointDir = filepath.Join(*workDir, "checkpoints")
	cfg.SchemaDir = filepath.Join(*workDir, "schemas")

	host := runtime.NewHost(cfg)
	fileSrc := std.NewFileSource("file-source", "1.0.0", inFile)
	memSrc := std.NewMemorySource("memory-source", "1.0.0", []map[string]any{
		{"id": "mem-1", "payload": "hello from memory"},
		{"id": "mem-2", "payload": "second memory record"},
	})
	must(host.Registry().Register(func() (contract.Connector, error) { return fileSrc, nil }))
	must(host.Registry().Register(func() (contract.Connector, error) { return memSrc, nil }))

	fileDown, err := runtime.OpenDurableDownstream(filepath.Join(*workDir, "file-output.jsonl"))
	must(err)
	memDown, err := runtime.OpenDurableDownstream(filepath.Join(*workDir, "memory-output.jsonl"))
	must(err)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runners, failures := host.Run(ctx,
		map[string]runtime.Downstream{
			"file-source":   fileDown,
			"memory-source": memDown,
		},
		map[string]checkpoint.PositionLess{
			"file-source":   std.PositionLess,
			"memory-source": std.PositionLess,
		})
	for name, lf := range failures {
		log.Printf("connector %q failed to load: %v", name, lf)
	}

	log.Printf("host running %d connector(s); Ctrl-C stops and checkpoints", len(runners))
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Printf("shutting down; draining in-flight batches...")
			failed := host.Stop(5 * time.Second)
			_ = fileDown.Close()
			_ = memDown.Close()
			for name, f := range failed {
				log.Printf("connector %q terminated: class=%s op=%s detail=%s", name, f.Class, f.Op, f.Detail)
			}
			log.Printf("file records durably accepted: %d; memory records: %d", fileDown.Count(), memDown.Count())
			return
		case <-ticker.C:
			for name, st := range host.Stats() {
				log.Printf("[%s] read=%d delivered=%d deduped=%d committed=%d timeouts=%d panics=%d",
					name, st.Reads, st.RecordsDelivered, st.RecordsDeduped, st.Commits, st.Timeouts, st.Panics)
			}
		}
	}
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

const sampleData = `{"id":"f-1","payload":"first file record"}
{"id":"f-2","payload":"second file record"}
{"id":"f-3","payload":"third file record"}
{"id":"f-4","payload":"fourth file record"}
{"id":"f-5","payload":"fifth file record"}
`
