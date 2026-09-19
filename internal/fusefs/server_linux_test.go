//go:build linux

package fusefs

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeLifecycle struct {
	unmount func() error
}

func (s fakeLifecycle) WaitMount() error { return nil }
func (s fakeLifecycle) Unmount() error   { return s.unmount() }

func TestUnmountWaitsForCompleteServeLifetime(t *testing.T) {
	unmounted := make(chan struct{})
	serving := make(chan struct{})
	server := &mountedServer{
		server: fakeLifecycle{unmount: func() error { close(unmounted); return nil }},
		done:   serving, timeout: time.Second,
	}
	result := make(chan error, 1)
	go func() { result <- server.Unmount() }()
	<-unmounted
	select {
	case err := <-result:
		t.Fatal("unmount reported success before serving goroutine stopped", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(serving)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	server.Wait()
}

func TestUnmountTimeoutFailsInsteadOfPretendingShutdown(t *testing.T) {
	for _, stage := range []string{"unmount", "serve"} {
		t.Run(stage, func(t *testing.T) {
			blocked := make(chan struct{})
			defer close(blocked)
			done := make(chan struct{})
			server := &mountedServer{
				server: fakeLifecycle{unmount: func() error {
					if stage == "unmount" {
						<-blocked
					}
					return nil
				}},
				done: done, timeout: 10 * time.Millisecond,
			}
			if err := server.Unmount(); err == nil || !strings.Contains(err.Error(), "did not") {
				t.Fatalf("blocked shutdown was silently accepted: %v", err)
			}
			close(done)
		})
	}
}

func TestUnmountSerializesConcurrentCallsAndAllowsRetryAfterError(t *testing.T) {
	var calls atomic.Int32
	failure := errors.New("mount is busy")
	done := make(chan struct{})
	close(done)
	server := &mountedServer{
		server: fakeLifecycle{unmount: func() error {
			if calls.Add(1) == 1 {
				return failure
			}

			return nil
		}},
		done: done, timeout: time.Second,
	}
	if err := server.Unmount(); !errors.Is(err, failure) {
		t.Fatal("unmount error hidden", err)
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := server.Unmount(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if calls.Load() != 2 {
		t.Fatalf("unmount ran concurrently/repeatedly: %d", calls.Load())
	}
}

type startupServer struct{ err error }

func (s *startupServer) Unmount() error   { return s.err }
func (s *startupServer) WaitMount() error { return nil }
func (s *startupServer) Wait()            {}

func TestFailedStartupRetainsHandleWhenCleanupFails(t *testing.T) {
	startupErr := errors.New("unsupported kernel capability")
	cleanupErr := errors.New("shutdown timed out")
	server := &startupServer{err: cleanupErr}
	got, err := failedMount(server, "/tmp/exact-private-mount", startupErr)
	if got != server || !errors.Is(err, startupErr) || !errors.Is(err, cleanupErr) ||
		!strings.Contains(err.Error(), "/tmp/exact-private-mount") {
		t.Fatal("startup failure lost a potentially mounted server or its exact path", got, err)
	}
	server.err = nil
	if got, err := failedMount(server, "/tmp/exact-private-mount", startupErr); got != nil || !errors.Is(err, startupErr) {
		t.Fatal("completed cleanup returned a live server", got, err)
	}
}

func TestCleanupRefusesAnyMountedPrivateSubtree(t *testing.T) {
	for _, test := range []struct {
		mount, directory string
		mounted          bool
	}{
		{"/tmp/owned/mount", "/tmp/owned", true},
		{"/tmp/owned/spool/nested", "/tmp/owned", true},
		{"/tmp/owned", "/tmp/owned", true},
		{"/tmp/owned-other/mount", "/tmp/owned", false},
		{`/tmp/own\040space/mount`, "/tmp/own space", true},
	} {
		input := "23 22 0:123 / " + test.mount + " rw - fuse.fabric-workspace-fs fabric-workspace-fs rw\n"
		got, err := hasMountedTree(strings.NewReader(input), test.directory)
		if err != nil || got != test.mounted {
			t.Fatalf("cleanup guard %q => %v,%v", test.mount, got, err)
		}
	}
	if _, err := hasMountedTree(strings.NewReader("malformed\n"), "/tmp/owned"); err == nil {
		t.Fatal("malformed mount table treated as clean unmount")
	}
}
