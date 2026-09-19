//go:build linux

package main

import (
	"context"
	"testing"
)

func TestNotebookAndEnvironmentCleanupNeverRoutesThroughProductRmdir(t *testing.T) {
	var r runner
	for _, kind := range []string{"Notebook", "Environment"} {
		// A zero runner would panic if the route reached identity lookup,
		// HTTP, FUSE, or the backend. Rejection must precede all of them.
		if err := r.removeManaged(context.Background(), kind, true); err == nil {
			t.Fatal("protected managed item type was routed through product rmdir")
		}
	}
}
