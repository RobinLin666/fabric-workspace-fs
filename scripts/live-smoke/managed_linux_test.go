//go:build linux

package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPostUnmountCleanupContextKeepsAnExplicitDeadline(t *testing.T) {
	run, stop := context.WithCancel(context.Background())
	stop()
	called := false
	call := func(ctx context.Context) (bool, error) {
		called = true
		return ctx.Err() == nil, ctx.Err()
	}
	if _, err := boundCall(run, backendCleanupContext(context.Background()), call); err == nil || called {
		t.Fatal("cleanup escaped the main cancellation without a bounded cleanup deadline")
	}
	deadlineCtx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	ok, err := boundCall(run, backendCleanupContext(deadlineCtx), call)
	if err != nil || !ok || !called {
		t.Fatal("owned post-unmount cleanup could not use its fresh short deadline")
	}
	called = false
	if _, err := boundCall(run, context.Background(), call); !errors.Is(err, context.Canceled) {
		t.Fatal("ordinary FUSE API calls bypassed the canceled run")
	}
}

func TestManagedKindsDoNotMapUnknownTypesToRoot(t *testing.T) {
	for _, kind := range []string{"Workspace", "ArbitraryType", ""} {
		if _, err := managedEntryKind(kind); err == nil {
			t.Fatal("unplanned managed type received a backend removal kind")
		}

	}
}
