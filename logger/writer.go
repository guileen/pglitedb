package logger

import (
	"io"
	"sync"
)

// BufferedWriter wraps an io.Writer with buffering capabilities
type BufferedWriter struct {
	writer io.Writer
	buffer []byte
	mutex  sync.Mutex
}

// NewBufferedWriter creates a new BufferedWriter
func NewBufferedWriter(writer io.Writer, bufferSize int) *BufferedWriter {
	if bufferSize <= 0 {
		bufferSize = 4096 // Default buffer size
	}
	
	return &BufferedWriter{
		writer: writer,
		buffer: make([]byte, 0, bufferSize),
	}
}

// Write implements io.Writer
func (bw *BufferedWriter) Write(p []byte) (n int, err error) {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	
	// If the data is larger than our buffer, write directly
	if len(p) >= cap(bw.buffer) {
		// Flush any existing buffered data first
		if len(bw.buffer) > 0 {
			_, err = bw.writer.Write(bw.buffer)
			if err != nil {
				return 0, err
			}
			bw.buffer = bw.buffer[:0]
		}
		
		// Write the large data directly
		return bw.writer.Write(p)
	}
	
	// If adding this data would exceed buffer capacity, flush first
	if len(bw.buffer)+len(p) > cap(bw.buffer) {
		_, err = bw.writer.Write(bw.buffer)
		if err != nil {
			return 0, err
		}
		bw.buffer = bw.buffer[:0]
	}
	
	// Add data to buffer
	bw.buffer = append(bw.buffer, p...)
	return len(p), nil
}

// Flush writes any buffered data
func (bw *BufferedWriter) Flush() error {
	bw.mutex.Lock()
	defer bw.mutex.Unlock()
	
	if len(bw.buffer) > 0 {
		_, err := bw.writer.Write(bw.buffer)
		bw.buffer = bw.buffer[:0]
		return err
	}
	
	return nil
}