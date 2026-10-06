// Package std provides built-in, fully local connectors: an in-memory
// source/sink and a newline-delimited JSON file source/sink. No network or
// external service is involved.
package std

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/checkpoint"
	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// Position is a big-endian uint64 record index (zero-based next-to-read).
type Position uint64

func encodePosition(idx uint64) []byte {
	buf := make([]byte, 8)
	for i := 0; i < 8; i++ {
		buf[7-i] = byte(idx >> (8 * i))
	}
	return buf
}

// DecodePosition parses a position produced by the built-in sources.
func DecodePosition(p []byte) (uint64, error) {
	if len(p) != 8 {
		return 0, fmt.Errorf("std position must be 8 bytes, got %d", len(p))
	}
	var idx uint64
	for i := 0; i < 8; i++ {
		idx = idx<<8 | uint64(p[i])
	}
	return idx, nil
}

// PositionLess orders built-in positions by record index.
func PositionLess(a, b checkpoint.Position) bool {
	ai, _ := DecodePosition(a)
	bi, _ := DecodePosition(b)
	return ai < bi
}

func recordSchema() *contract.Schema {
	return &contract.Schema{
		Name: "std.record",
		Fields: []contract.Field{
			{Name: "id", Type: contract.TypeString, Required: true},
			{Name: "payload", Type: contract.TypeString, Required: false},
		},
	}
}

func toRecord(idx uint64, values map[string]any) contract.Envelope {
	id := fmt.Sprintf("%d", idx)
	if v, ok := values["id"]; ok {
		id = fmt.Sprint(v)
	} else {
		values["id"] = id
	}
	return contract.Envelope{
		Record:   contract.Record{ID: id, Values: values},
		Position: encodePosition(idx + 1),
	}
}

// MemorySource replays an in-memory slice of records. It is resumable.
type MemorySource struct {
	Name    string
	Version string
	Records []map[string]any

	mu     sync.Mutex
	next   uint64
	closed bool
}

// NewMemorySource constructs a named memory source.
func NewMemorySource(name, version string, records []map[string]any) *MemorySource {
	cp := make([]map[string]any, len(records))
	for i, r := range records {
		cp[i] = r
	}
	return &MemorySource{Name: name, Version: version, Records: cp}
}

func (m *MemorySource) Declare() contract.Declaration {
	return contract.Declaration{
		Name:             m.Name,
		ConnectorVersion: m.Version,
		APIVersion:       contract.V2,
		Capabilities:     []contract.Capability{contract.CapSource, contract.CapResumable},
		OutputSchema:     recordSchema(),
	}
}

func (m *MemorySource) Open() error { return nil }

func (m *MemorySource) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *MemorySource) Source() contract.Source { return m }
func (m *MemorySource) Sink() contract.Sink     { return nil }

func (m *MemorySource) Resume(position []byte) error {
	idx, err := DecodePosition(position)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if idx > uint64(len(m.Records)) {
		return fmt.Errorf("resume position %d beyond record count %d", idx, len(m.Records))
	}
	m.next = idx
	return nil
}

func (m *MemorySource) Read(ctx contract.SourceContext, max int) (contract.Batch, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if max <= 0 {
		max = 32
	}
	if int(m.next) >= len(m.Records) {
		return contract.Batch{}, io.EOF
	}
	batch := contract.Batch{}
	for len(batch.Records) < max && int(m.next) < len(m.Records) {
		select {
		case <-ctx.Done():
			return contract.Batch{}, ctx.Err()
		default:
		}
		idx := m.next
		values := map[string]any{}
		for k, v := range m.Records[idx] {
			values[k] = v
		}
		env := toRecord(idx, values)
		batch.Records = append(batch.Records, env)
		batch.Position = env.Position
		m.next++
	}
	return batch, nil
}

// FileSource reads newline-delimited JSON objects from a local file. Each
// line is one record; the position is the count of records consumed.
type FileSource struct {
	Name    string
	Version string
	Path    string

	mu     sync.Mutex
	next   uint64
	file   *os.File
	reader *bufio.Reader
}

// NewFileSource constructs a file source.
func NewFileSource(name, version, path string) *FileSource {
	return &FileSource{Name: name, Version: version, Path: path}
}

func (f *FileSource) Declare() contract.Declaration {
	return contract.Declaration{
		Name:             f.Name,
		ConnectorVersion: f.Version,
		APIVersion:       contract.V2,
		Capabilities:     []contract.Capability{contract.CapSource, contract.CapResumable},
		OutputSchema:     recordSchema(),
	}
}

func (f *FileSource) Open() error {
	file, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	f.file = file
	f.reader = bufio.NewReader(file)
	return nil
}

func (f *FileSource) Close() error {
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}

func (f *FileSource) Source() contract.Source { return f }
func (f *FileSource) Sink() contract.Sink     { return nil }

func (f *FileSource) Resume(position []byte) error {
	idx, err := DecodePosition(position)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.file == nil {
		return fmt.Errorf("resume before open")
	}
	f.next = idx
	// Position n means "n records consumed, next index is n", i.e. a
	// zero-based next-to-read. Drain n lines, leaving the reader on record n.
	for i := uint64(0); i < idx; i++ {
		if _, err := f.reader.ReadBytes('\n'); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
	return nil
}

func (f *FileSource) Read(ctx contract.SourceContext, max int) (contract.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if max <= 0 {
		max = 32
	}
	batch := contract.Batch{}
	for len(batch.Records) < max {
		select {
		case <-ctx.Done():
			return contract.Batch{}, ctx.Err()
		default:
		}
		line, err := f.reader.ReadBytes('\n')
		if len(line) > 0 {
			trimmed := trimNL(line)
			if len(trimmed) > 0 {
				values := map[string]any{}
				if jerr := json.Unmarshal(trimmed, &values); jerr != nil {
					return contract.Batch{}, fmt.Errorf("invalid JSON on record %d: %w", f.next, jerr)
				}
				env := toRecord(f.next, values)
				batch.Records = append(batch.Records, env)
				batch.Position = env.Position
				f.next++
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if len(batch.Records) > 0 {
					return batch, nil
				}
				return contract.Batch{}, io.EOF
			}
			return contract.Batch{}, err
		}
	}
	return batch, nil
}

func trimNL(b []byte) []byte {
	for len(b) > 0 && (b[len(b)-1] == '\n' || b[len(b)-1] == '\r') {
		b = b[:len(b)-1]
	}
	return b
}

// MemorySink collects records in memory and dedupes by record id.
type MemorySink struct {
	Name    string
	Version string

	mu      sync.Mutex
	records []contract.Record
	seen    map[string]struct{}
	pending []contract.Record
}

// NewMemorySink constructs a named memory sink.
func NewMemorySink(name, version string) *MemorySink {
	return &MemorySink{Name: name, Version: version, seen: map[string]struct{}{}}
}

func (m *MemorySink) Declare() contract.Declaration {
	s := recordSchema()
	return contract.Declaration{
		Name:             m.Name,
		ConnectorVersion: m.Version,
		APIVersion:       contract.V2,
		Capabilities:     []contract.Capability{contract.CapSink, contract.CapAtLeastOnce},
		InputSchema:      s,
	}
}

func (m *MemorySink) Open() error             { return nil }
func (m *MemorySink) Close() error            { return nil }
func (m *MemorySink) Source() contract.Source { return nil }
func (m *MemorySink) Sink() contract.Sink     { return m }

func (m *MemorySink) Write(_ contract.SinkContext, batch contract.Batch) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, env := range batch.Records {
		m.pending = append(m.pending, env.Record)
	}
	return nil
}

// Commit makes pending records durable (in-memory) and dedupes replays.
func (m *MemorySink) Commit(_ contract.SinkContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, rec := range m.pending {
		if _, ok := m.seen[rec.ID]; ok {
			continue
		}
		m.seen[rec.ID] = struct{}{}
		m.records = append(m.records, rec)
	}
	m.pending = nil
	return nil
}

// Records returns a copy of committed records.
func (m *MemorySink) Records() []contract.Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]contract.Record, len(m.records))
	copy(out, m.records)
	return out
}
