package tstorage

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"os"
	"sync"
)

var (
	// Magic sequence to check for valid data.
	walMagic = uint32(0x11141993)
)

type fileWAL struct {
	filename string
	w        *bufio.Writer
	mu       sync.Mutex
}

func newFileWal(filename string) (wal, error) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	return &fileWAL{
		filename: filename,
		w:        bufio.NewWriter(f),
	}, nil
}

// append appends the given entry to the end of a file via the file descriptor it has.
func (w fileWAL) append(entry walEntry) error {
	// TODO: Implement appending to wal correctly.

	w.mu.Lock()
	defer w.mu.Unlock()

	// Write the operation type
	if err := w.w.WriteByte(byte(entry.operation)); err != nil {
		return err
	}

	for _, row := range entry.rows {
		// Write metric name
		name := marshalMetricName(row.Metric, row.Labels)
		if _, err := w.w.WriteString(name); err != nil {
			return err
		}
		if err := binary.Write(w.w, binary.LittleEndian, row.DataPoint.Timestamp); err != nil {
			return err
		}
		if err := binary.Write(w.w, binary.LittleEndian, row.DataPoint.Value); err != nil {
			return err
		}
	}

	return w.w.Flush()
}
