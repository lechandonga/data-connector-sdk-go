package checkpoint

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"

	"github.com/lechandonga/data-connector-sdk-go/contract"
)

// FileSchemaStore persists the last accepted schema per connector as a JSON
// file in dir. It implements connector.SchemaStore.
type FileSchemaStore struct {
	dir string
}

// NewFileSchemaStore creates a schema store backed by dir.
func NewFileSchemaStore(dir string) *FileSchemaStore {
	return &FileSchemaStore{dir: dir}
}

func (f *FileSchemaStore) path(name string) string {
	return filepath.Join(f.dir, "schema-"+sanitize(name)+".json")
}

// LoadSchema returns the stored schema.
func (f *FileSchemaStore) LoadSchema(name string) (contract.Schema, bool, error) {
	raw, err := os.ReadFile(f.path(name))
	if errors.Is(err, os.ErrNotExist) {
		return contract.Schema{}, false, nil
	}
	if err != nil {
		return contract.Schema{}, false, err
	}
	var s contract.Schema
	if err := json.Unmarshal(raw, &s); err != nil {
		return contract.Schema{}, false, err
	}
	return s, true, nil
}

// SaveSchema writes the schema atomically enough for registration-time use
// (temp file + rename).
func (f *FileSchemaStore) SaveSchema(name string, s contract.Schema) error {
	if err := os.MkdirAll(f.dir, 0o755); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(f.dir, ".schema-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, f.path(name))
}
