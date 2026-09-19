package resources

import (
	"bytes"
	"io/fs"
	"sync"

	"fabric-workspace-fs/internal/fserrors"
)

// Snapshot pins one bounded resource download to a handle. Its lifetime is
// independent of the shared TTL cache and cannot trigger further HTTP reads.
type Snapshot struct {
	mu      sync.Mutex
	data    []byte
	size    int64
	version string
	closed  bool
}

func NewSnapshot(data []byte, version string) *Snapshot {
	return &Snapshot{data: append([]byte(nil), data...), size: int64(len(data)), version: version}
}
func (s *Snapshot) ReadAt(p []byte, offset int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, fserrors.ErrClosed
	}
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	if len(p) == 0 {
		return 0, nil
	}
	return bytes.NewReader(s.data).ReadAt(p, offset)
}
func (s *Snapshot) Size() int64     { return s.size }
func (s *Snapshot) Version() string { return s.version }
func (s *Snapshot) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.data = nil
	return nil
}
