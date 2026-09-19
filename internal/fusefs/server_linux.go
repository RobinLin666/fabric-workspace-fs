//go:build linux

package fusefs

import (
	"fmt"
	"sync"
	"time"
)

type fuseLifecycle interface {
	WaitMount() error
	Unmount() error
}

type shutdown struct {
	done chan struct{}
	err  error
}

type mountedServer struct {
	server        fuseLifecycle
	notifications *invalidator
	done          chan struct{}
	timeout       time.Duration
	mu            sync.Mutex
	pending       *shutdown
}

func (s *mountedServer) WaitMount() error { return s.server.WaitMount() }
func (s *mountedServer) Wait()            { <-s.done }

func (s *mountedServer) Unmount() error {
	s.mu.Lock()
	pending := s.pending
	if pending == nil {
		pending = &shutdown{done: make(chan struct{})}
		s.pending = pending
		go func() {
			pending.err = s.server.Unmount()
			close(pending.done)
		}()
	}
	s.mu.Unlock()
	timer := time.NewTimer(s.timeout)
	defer timer.Stop()
	select {
	case <-pending.done:
		if pending.err != nil {
			s.mu.Lock()
			if s.pending == pending {
				s.pending = nil
			}
			s.mu.Unlock()
			return pending.err
		}
	case <-timer.C:
		return fmt.Errorf("FUSE unmount did not finish within %s; retain the mountpoint until server shutdown is confirmed", s.timeout)
	}
	select {
	case <-s.done:
		return nil
	case <-timer.C:
		return fmt.Errorf("FUSE serving goroutine did not stop within %s", s.timeout)
	}
}
