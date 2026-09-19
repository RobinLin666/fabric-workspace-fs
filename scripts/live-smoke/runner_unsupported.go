//go:build !linux

package main

import "io"

func prepareSignals() {}

func run(options, io.Writer) error {
	return fail("live-smoke is unsupported on this OS; build and run it on native Linux with FUSE")
}

// Non-Linux builds only exercise the offline journal helpers, never a live run.
func syncEvidenceDirectory(string) error { return nil }
