package std

import (
	"encoding/json"
	"os"
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// FileSink appends committed records to a local newline-delimited JSON file.
// Writes are buffered per batch; Commit flushes and fsyncs. Replayed record
// ids are skipped on commit (best-effort at-least-once dedupe keyed by id).
type FileSink struct {
	Name    string
	Version string
	Path    string

	mu      sync.Mutex
	file    *os.File
	encoder *json.Encoder
	pending []contract.Record
	seen    map[string]struct{}
}

// NewFileSink constructs a file sink. The file is created or appended.
func NewFileSink(name, version, path string) *FileSink {
	return &FileSink{Name: name, Version: version, Path: path, seen: map[string]struct{}{}}
}

func (f *FileSink) Declare() contract.Declaration {
	s := recordSchema()
	return contract.Declaration{
		Name:             f.Name,
		ConnectorVersion: f.Version,
		APIVersion:       contract.V2,
		Capabilities:     []contract.Capability{contract.CapSink, contract.CapAtLeastOnce},
		InputSchema:      s,
	}
}

func (f *FileSink) Open() error {
	file, err := os.OpenFile(f.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	f.file = file
	f.encoder = json.NewEncoder(file)
	return nil
}

func (f *FileSink) Close() error {
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}

func (f *FileSink) Source() contract.Source { return nil }
func (f *FileSink) Sink() contract.Sink     { return f }

func (f *FileSink) Write(_ contract.SinkContext, batch contract.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, env := range batch.Records {
		f.pending = append(f.pending, env.Record)
	}
	return nil
}

func (f *FileSink) Commit(_ contract.SinkContext) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, rec := range f.pending {
		if _, ok := f.seen[rec.ID]; ok {
			continue
		}
		if err := f.encoder.Encode(rec.Values); err != nil {
			return err
		}
		f.seen[rec.ID] = struct{}{}
	}
	f.pending = nil
	return f.file.Sync()
}
