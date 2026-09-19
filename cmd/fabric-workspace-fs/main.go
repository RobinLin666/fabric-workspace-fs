package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"fabric-workspace-fs/internal/cli"
)

var version = "0.3.0-dev"

func main() {
	ignoreBrokenLogPipes()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Run(ctx, os.Args[1:], os.Stdout, os.Stderr, version)
	stop()
	os.Exit(code)
}
