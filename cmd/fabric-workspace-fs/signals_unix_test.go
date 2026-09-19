//go:build !windows

package main

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

func TestClosedLogConsumerDoesNotTerminateProcess(t *testing.T) {
	if mode := os.Getenv("FABRICFS_TEST_CLOSED_LOG"); mode != "" {
		if mode == "protected" {
			ignoreBrokenLogPipes()
		}
		_, err := os.Stderr.WriteString("observable filesystem error\n")
		if !errors.Is(err, syscall.EPIPE) {
			os.Exit(3)
		}
		os.Exit(0)
	}
	for _, mode := range []string{"original-behavior", "protected"} {
		t.Run(mode, func(t *testing.T) {
			reader, writer, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			if err := reader.Close(); err != nil {
				t.Fatal(err)
			}
			defer writer.Close()
			command := exec.Command(os.Args[0], "-test.run=^TestClosedLogConsumerDoesNotTerminateProcess$")
			command.Env = append(os.Environ(), "FABRICFS_TEST_CLOSED_LOG="+mode)
			command.Stderr = writer
			err = command.Run()
			if mode == "protected" && err != nil {
				t.Fatalf("closed error-log consumer killed the protected process: %v", err)
			}
			if mode == "original-behavior" {
				status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
				if err == nil || !ok || !status.Signaled() || status.Signal() != syscall.SIGPIPE {
					t.Fatalf("original SIGPIPE termination not reproduced: status=%v err=%v", status, err)
				}
			}
		})
	}
}
