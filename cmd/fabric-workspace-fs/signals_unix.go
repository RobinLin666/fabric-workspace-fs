//go:build !windows

package main

import (
	"os/signal"
	"syscall"
)

func ignoreBrokenLogPipes() {
	// Losing a terminal/log consumer must not terminate a mounted filesystem.
	// File operations still return their real errors to the calling process.
	signal.Ignore(syscall.SIGPIPE)
}
