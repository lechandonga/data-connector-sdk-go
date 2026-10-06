// Package localconn 提供两个参考连接器实现：从内存切片读取的
// MemoryConnector 和从本地文件按行读取的 FileConnector。
// 它们不访问任何外部服务，可用于本地开发与测试。
package localconn

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"time"

	"github.com/lechandonga/data-connector-sdk-go/connector"
)

// MemoryConnector 从内存中的记录切片产出数据，支持断点续读。
type MemoryConnector struct {
	// Records 是数据源；Seq 由连接器按 1..N 重新分配。
	Records []map[string]any
	// Delay 是每条记录之间的间隔，用于模拟慢速源。
	Delay time.Duration
}

func (m *MemoryConnector) Open(context.Context, connector.Config) error { return nil }

func (m *MemoryConnector) Read(ctx context.Context, resume connector.Checkpoint, emit func(connector.Record) error) error {
	for i, data := range m.Records {
		seq := uint64(i + 1)
		if seq <= resume.Seq {
			continue
		}
		if m.Delay > 0 {
			select {
			case <-time.After(m.Delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := emit(connector.Record{Seq: seq, Offset: fmt.Sprintf("mem:%d", seq), Data: data}); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryConnector) Close() error { return nil }

// FileConnector 逐行读取本地文件，Offset 记录已读行号，支持断点续读。
type FileConnector struct {
	Path string
}

func (f *FileConnector) Open(context.Context, connector.Config) error { return nil }

func (f *FileConnector) Read(ctx context.Context, resume connector.Checkpoint, emit func(connector.Record) error) error {
	file, err := os.Open(f.Path)
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	var seq uint64
	for scanner.Scan() {
		seq++
		if seq <= resume.Seq {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		rec := connector.Record{Seq: seq, Offset: fmt.Sprintf("line:%d", seq),
			Data: map[string]any{"line": scanner.Text()}}
		if err := emit(rec); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func (f *FileConnector) Close() error { return nil }
