//go:build linux

package main

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"fabric-workspace-fs/internal/workspacefs"
)

func TestBoundAPIContextHonorsBothCancellationSources(t *testing.T) {
	for _, cancelRun := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			run, stopRun := context.WithCancel(context.Background())
			defer stopRun()
			request, stopRequest := context.WithCancel(context.Background())
			defer stopRequest()
			done := make(chan error, 1)
			go func() {
				_, err := boundCall(run, request, func(ctx context.Context) (int, error) {
					<-ctx.Done()
					return 0, ctx.Err()
				})
				done <- err
			}()
			synctest.Wait()
			if cancelRun {
				stopRun()
			} else {
				stopRequest()
			}
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatal("API request did not inherit cancellation")
			}
		})
	}
}

type fakeOwnServer struct {
	calls   []string
	err     error
	release <-chan struct{}
}

func (s *fakeOwnServer) WaitMount() error { return nil }

func (s *fakeOwnServer) Unmount() error {
	s.calls = append(s.calls, "unmount")
	return s.err
}

func (s *fakeOwnServer) Wait() {
	s.calls = append(s.calls, "wait")
	if s.release != nil {
		<-s.release
	}
}

func TestShutdownUnmountsOnlyPassedServerThenWaits(t *testing.T) {
	for _, failure := range []error{nil, errors.New("offline unmount error")} {
		server := &fakeOwnServer{err: failure}
		untouched := &fakeOwnServer{}
		err := stopOwnedServer(context.Background(), server)
		if !errors.Is(err, failure) || !slices.Equal(server.calls, []string{"unmount", "wait"}) || len(untouched.calls) != 0 {
			t.Fatal("shutdown did not exclusively unmount then wait for the passed server")
		}
	}
}

func TestShutdownWaitIsBoundedWithoutRemovingAnything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		defer close(release)
		server := &fakeOwnServer{release: release}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := stopOwnedServer(ctx, server); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal("shutdown did not return its bounded failure")
		}
	})
}

func TestDiscoveredPathsMustUseSafeBackendComponents(t *testing.T) {
	parent := location{path: "/private-test-mount"}
	for _, name := range []string{"../escape", "a/b", `a\b`, ".", ""} {
		if _, err := childLocation(parent, workspacefs.Entry{Name: name}); err == nil {
			t.Fatal("unsafe backend name escaped the private mount")
		}
	}
	child, err := childLocation(parent, workspacefs.Entry{Name: "display name (duplicate)"})
	if err != nil || child.path != "/private-test-mount/display name (duplicate)" || child.parent != parent.path {
		t.Fatal("safe discovered name was not used verbatim")
	}
}
