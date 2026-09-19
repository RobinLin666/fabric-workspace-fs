//go:build linux

package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestBackendCloseWaitsForOwnedServerStop(t *testing.T) {
	calls := 0
	closeBackend := func() error {
		calls++
		return nil
	}
	status, err := closeOwnedBackend(false, closeBackend)
	if err != nil || calls != 0 || status != "retained-until-own-server-stops" {
		t.Fatal("backend was closed while its own server might still use it")
	}
	status, err = closeOwnedBackend(true, closeBackend)
	if err != nil || calls != 1 || status != "closed-after-owned-server-and-managed-cleanup" {
		t.Fatal("stopped backend was not closed exactly once")
	}
}

func TestBackendCloseFailureRequiresParentRetention(t *testing.T) {
	cause := errors.New("offline close failure")
	status, err := closeOwnedBackend(true, func() error { return cause })
	if !errors.Is(err, cause) || !strings.Contains(status, "retained") {
		t.Fatal("backend close failure was hidden or allowed native parent removal")
	}
	status, err = closeOwnedBackend(true, nil)
	if err != nil || status != "not-created" {
		t.Fatal("cleanup attempted to close an unconstructed backend")
	}
}

func TestLegacyOverlayStopsManagedCleanupBeforeBackendOrHTTPAccess(t *testing.T) {
	for _, runCleanup := range []func(*runner) error{
		func(r *runner) error { return r.cleanupManagedFixtures(context.Background()) },
		func(r *runner) error { return r.deleteManagedFixtures() },
	} {
		doc := &report{
			OverlayDirectory:    "unopened-legacy-store",
			CreatedOverlayPaths: map[string]bool{".agents": true},
		}
		// Nil clients/guard/backend are intentional: legacy handling must
		// return before any local store, backend, or remote resource access.
		if err := runCleanup(&runner{doc: doc}); err == nil ||
			doc.Cleanup.Overlay != "unsupported-legacy-overlay-preserved" {
			t.Fatal("legacy cleanup reached a backend or discarded its receipt")
		}
		if doc.OverlayDirectory != "unopened-legacy-store" || !doc.CreatedOverlayPaths[".agents"] {
			t.Fatal("retained legacy cleanup evidence was lost")
		}
	}
}
