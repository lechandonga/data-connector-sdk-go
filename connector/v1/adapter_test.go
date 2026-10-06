package v1

import (
	"fmt"
	"testing"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

type seekLegacy struct {
	records []LegacyRecord
	idx     int64
}

func (s *seekLegacy) Pull(cancelled <-chan struct{}) (LegacyRecord, error) {
	if int(s.idx) >= len(s.records) {
		<-cancelled
		return LegacyRecord{}, fmt.Errorf("EOF")
	}
	r := s.records[s.idx]
	s.idx++
	return r, nil
}

func (s *seekLegacy) Seek(position []byte) error {
	id, err := DecodePosition(position)
	if err != nil {
		return err
	}
	s.idx = int64(id)
	return nil
}

type ctxStub struct{}

func (ctxStub) Done() <-chan struct{} { return make(chan struct{}) }
func (ctxStub) Err() error            { return nil }

func TestSeekableV1Resume(t *testing.T) {
	fmt.Printf("    [biz:v1-compat] case=seek-resume legacy Seek is wired to ResumableSource\n")
	legacy := &seekLegacy{records: []LegacyRecord{{ID: 1, Message: "a"}, {ID: 2, Message: "b"}, {ID: 3, Message: "c"}}}
	a, err := New(&LegacyConnector{Name: "seek", Version: "0.1", Source: legacy})
	if err != nil {
		t.Fatal(err)
	}
	if !containsCap(a.Declare().Capabilities, contract.CapResumable) {
		t.Fatalf("seekable v1 connector must advertise resumable: %+v", a.Declare().Capabilities)
	}
	src := a.Source().(interface {
		Resume(position []byte) error
	})
	if err := src.Resume(encodePosition(2)); err != nil {
		t.Fatal(err)
	}
	b, err := a.Source().Read(ctxStub{}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := b.Records[0].Record.Values["message"]; got != "c" {
		t.Fatalf("resume must land on record c, got %v", got)
	}
}

func containsCap(caps []contract.Capability, want contract.Capability) bool {
	for _, c := range caps {
		if c == want {
			return true
		}
	}
	return false
}
