package runtime

import "time"

// Config holds host runtime configuration. All zero/negative values are
// replaced by defaults when a Host is constructed, so callers may pass a
// partially filled Config.
type Config struct {
	// CheckpointDir is where per-connector progress files live.
	CheckpointDir string
	// SchemaDir is where accepted connector schemas are persisted.
	SchemaDir string

	// ReadTimeout bounds every Source.Read call, including connectors that
	// ignore cancellation.
	ReadTimeout time.Duration
	// WriteTimeout bounds every Sink.Write call.
	CommitTimeout time.Duration
	// OpenTimeout bounds Connector.Open.
	OpenTimeout time.Duration

	// BufferCapacity bounds the number of in-flight batches between a
	// connector and its downstream. A slow downstream applies back-pressure:
	// reads stall rather than buffering without limit.
	BufferCapacity int
	// BatchSize is the maximum records requested per Read.
	BatchSize int
	// IdlePollInterval is the wait before re-reading after an empty batch.
	IdlePollInterval time.Duration
	// DedupeWindow bounds how many recently delivered-but-uncommitted record
	// ids are retained to suppress replays after recovery.
	DedupeWindow int
}

// DefaultConfig returns the built-in defaults.
func DefaultConfig() Config {
	return Config{
		CheckpointDir:    "./.host/checkpoints",
		SchemaDir:        "./.host/schemas",
		ReadTimeout:      5 * time.Second,
		CommitTimeout:    5 * time.Second,
		OpenTimeout:      3 * time.Second,
		BufferCapacity:   4,
		BatchSize:        32,
		IdlePollInterval: 50 * time.Millisecond,
		DedupeWindow:     4096,
	}
}

func (c *Config) withDefaults() Config {
	d := DefaultConfig()
	if c.CheckpointDir != "" {
		d.CheckpointDir = c.CheckpointDir
	}
	if c.SchemaDir != "" {
		d.SchemaDir = c.SchemaDir
	}
	if c.ReadTimeout > 0 {
		d.ReadTimeout = c.ReadTimeout
	}
	if c.CommitTimeout > 0 {
		d.CommitTimeout = c.CommitTimeout
	}
	if c.OpenTimeout > 0 {
		d.OpenTimeout = c.OpenTimeout
	}
	if c.BufferCapacity > 0 {
		d.BufferCapacity = c.BufferCapacity
	}
	if c.BatchSize > 0 {
		d.BatchSize = c.BatchSize
	}
	if c.IdlePollInterval > 0 {
		d.IdlePollInterval = c.IdlePollInterval
	}
	if c.DedupeWindow > 0 {
		d.DedupeWindow = c.DedupeWindow
	}
	return d
}
