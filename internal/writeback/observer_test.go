package writeback

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestFlushObserverOutsideIOLock(t *testing.T) {
	type event struct {
		stage string
		err   error
	}
	var events []event
	var f *File
	var err error
	f, err = Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 16,
		Commit: func(context.Context, io.ReaderAt, int64) error { return nil },
		Observe: func(stage string, elapsed time.Duration, err error) {
			if !f.mu.TryLock() {
				t.Error("observer called under I/O lock")
				return
			}
			f.mu.Unlock()
			if elapsed < 0 {
				t.Errorf("negative elapsed time: %v", elapsed)
			}
			events = append(events, event{stage, err})
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	if err := f.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].stage != "flush_lock" || events[1].stage != "spool_sync" ||
		events[0].err != nil || events[1].err != nil {
		t.Fatalf("successful flush events = %+v", events)
	}
	events = nil
	if err := f.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].stage != "flush_lock" {
		t.Fatalf("clean flush events = %+v", events)
	}
	if _, err := f.WriteAt([]byte("x"), 4); err != nil {
		t.Fatal(err)
	}
	if err := f.file.Close(); err != nil {
		t.Fatal(err)
	}
	events = nil
	if err := f.Flush(context.Background()); err == nil {
		t.Fatal("sync unexpectedly succeeded")
	}
	if len(events) != 2 || events[1].stage != "spool_sync" || events[1].err == nil {
		t.Fatalf("failed sync events = %+v", events)
	}
}

func TestFlushObserverSurvivesConcurrentClose(t *testing.T) {
	events := make(chan string, 2)
	release := make(chan struct{})
	defer close(release)
	f, err := Open(context.Background(), Options{
		Directory: t.TempDir(), MaxSize: 16,
		Commit: func(context.Context, io.ReaderAt, int64) error { return nil },
		Observe: func(stage string, _ time.Duration, _ error) {
			events <- stage
			if stage == "flush_lock" {
				<-release
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	if _, err := f.WriteAt([]byte("data"), 0); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	go func() { flushed <- f.Flush(context.Background()) }()
	select {
	case stage := <-events:
		if stage != "flush_lock" {
			t.Fatalf("first stage = %q", stage)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("flush observer did not start")
	}
	closed := make(chan error, 1)
	go func() { closed <- f.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close blocked behind observer")
	}
	if f.observe != nil || f.commit != nil || f.onClose != nil {
		t.Fatal("Close retained callbacks")
	}
	// A deferred release also unblocks Flush if an earlier assertion fails.
	release <- struct{}{}
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Flush did not finish after concurrent Close")
	}
	select {
	case stage := <-events:
		if stage != "spool_sync" {
			t.Fatalf("second stage = %q", stage)
		}
	default:
		t.Fatal("captured observer lost sync event after Close")
	}
}
