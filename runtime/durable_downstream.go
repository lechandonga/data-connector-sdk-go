package runtime

import (
	"bufio"
	"bytes"
	"encoding/json"
	"os"
	"sync"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// DurableDownstream is a local-file downstream used by the host itself and
// by tests. It appends delivered records to a JSONL journal and suppresses
// duplicate record ids across process restarts by reading the journal on
// construction. The journal is fsynced on every Deliver, which models the
// "accepted" durability point that the checkpoint commit follows.
//
// This is what makes the overall pipeline effectively-once in effect: the
// source+checkpoint layer is at-least-once; idempotent persistence here
// collapses replays. A non-durable downstream instead observes replays, as
// the contract documents.
type DurableDownstream struct {
	path string

	mu   sync.Mutex
	file *os.File
	w    *bufio.Writer
	seen map[string]struct{}
}

// OpenDurableDownstream opens (creating if needed) the journal at path and
// indexes all record ids already present.
func OpenDurableDownstream(path string) (*DurableDownstream, error) {
	d := &DurableDownstream{path: path, seen: map[string]struct{}{}}
	if existing, err := os.ReadFile(path); err == nil {
		scanner := bufio.NewScanner(bytes.NewReader(existing))
		scanner.Buffer(make([]byte, 1024*1024), 16*1024*1024)
		for scanner.Scan() {
			var rec contract.Record
			if err := json.Unmarshal(scanner.Bytes(), &rec); err == nil {
				d.seen[rec.ID] = struct{}{}
			}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	d.file = f
	d.w = bufio.NewWriter(f)
	return d, nil
}

// Deliver appends rec if its id has not been durably seen before.
func (d *DurableDownstream) Deliver(rec contract.Record) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.seen[rec.ID]; ok {
		return nil
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	if _, err := d.w.Write(raw); err != nil {
		return err
	}
	if err := d.w.WriteByte('\n'); err != nil {
		return err
	}
	if err := d.w.Flush(); err != nil {
		return err
	}
	if err := d.file.Sync(); err != nil {
		return err
	}
	d.seen[rec.ID] = struct{}{}
	return nil
}

// Close releases the journal file.
func (d *DurableDownstream) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.file == nil {
		return nil
	}
	err := d.file.Close()
	d.file = nil
	return err
}

// Count returns the number of distinct persisted records.
func (d *DurableDownstream) Count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}
