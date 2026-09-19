//go:build linux

package fusefs

import (
	"bytes"
	"io"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
)

type notifyFunctions struct {
	attrFn  func(*fs.Inode) syscall.Errno
	entryFn func(*fs.Inode, string) syscall.Errno
}

func (f notifyFunctions) attr(inode *fs.Inode) syscall.Errno {
	if f.attrFn != nil {
		return f.attrFn(inode)
	}
	return 0
}

func (f notifyFunctions) entry(inode *fs.Inode, name string) syscall.Errno {
	if f.entryFn != nil {
		return f.entryFn(inode, name)
	}
	return 0
}

func waitInvalidations(t *testing.T, i *invalidator) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		i.mu.Lock()
		pending, err := len(i.jobs), i.err
		i.mu.Unlock()
		if err != 0 {
			t.Fatalf("kernel notification failed: %v", err)
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d invalidations did not finish before the deadline", pending)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestInvalidationIsBoundedAndNeverWaitsForOwnRequest(t *testing.T) {
	started, unblock := make(chan struct{}, 1), make(chan struct{})
	var unblockOnce sync.Once
	var calls atomic.Int32
	i := newInvalidator(log.New(io.Discard, "", 0), notifyFunctions{
		attrFn: func(*fs.Inode) syscall.Errno {
			calls.Add(1)
			select {
			case started <- struct{}{}:
			default:
			}
			<-unblock
			return 0
		},
	})
	t.Cleanup(func() { unblockOnce.Do(func() { close(unblock) }); i.close() })
	i.mu.Lock()
	i.limit = 2
	i.mu.Unlock()
	writer, code := i.reserve([]invalidationTarget{{inode: &fs.Inode{}}})
	if code != 0 {
		t.Fatal(code)
	}
	writer.changed(false)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("notification worker did not start")
	}
	for range 10000 {
		writer.changed(false)
	}
	other, code := i.reserve([]invalidationTarget{{inode: &fs.Inode{}}})
	if code != 0 {
		t.Fatal("blocked notifier retained a coordinator lock", code)
	}
	if overflow, code := i.reserve(nil); overflow != nil || code != syscall.EAGAIN {
		t.Fatalf("full invalidator silently dropped work or grew: %v %v", overflow, code)
	}
	// FLUSH/FSYNC only mark the reusable writer ticket; RELEASE retires it.
	writer.changed(true)
	other.changed(true)
	unblockOnce.Do(func() { close(unblock) })
	waitInvalidations(t, i)
	if got := calls.Load(); got != 3 {
		t.Fatalf("writes were not coalesced with a final post-release invalidation: %d", got)
	}
}

func TestInvalidationFailureIsVisibleAndNotASuccessFallback(t *testing.T) {
	for _, code := range []syscall.Errno{syscall.ENOSYS, syscall.EIO, syscall.ENOENT} {
		t.Run(code.Error(), func(t *testing.T) {
			var logged bytes.Buffer
			i := newInvalidator(log.New(&logged, "", 0), notifyFunctions{
				entryFn: func(*fs.Inode, string) syscall.Errno { return code },
			})
			t.Cleanup(i.close)
			change, err := i.reserve([]invalidationTarget{{inode: &fs.Inode{}, name: "previously-missing"}})
			if err != 0 {
				t.Fatal(err)
			}
			change.changed(true)
			deadline := time.Now().Add(time.Second)
			for {
				i.mu.Lock()
				done := len(i.jobs) == 0
				i.mu.Unlock()
				if done {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("notification did not complete")
				}
				time.Sleep(time.Millisecond)
			}
			next, err := i.reserve(nil)
			if code == syscall.ENOENT {
				if err != 0 || next == nil || logged.Len() != 0 {
					t.Fatal("already-forgotten entry was treated as notification failure", err, logged.String())
				}
				next.changed(true)
				waitInvalidations(t, i)
				return
			}
			if err != code || next != nil || change.check() != code || !strings.Contains(logged.String(), "further mutations will fail") {
				t.Fatal("notification failure was hidden", err, logged.String())
			}
		})
	}
}

func TestInvalidationCoversAttributesBeforePositiveAndNegativeDentries(t *testing.T) {
	parent, source, target := &fs.Inode{}, &fs.Inode{}, &fs.Inode{}
	var mu sync.Mutex
	var attrs []*fs.Inode
	var names []string
	i := newInvalidator(log.New(io.Discard, "", 0), notifyFunctions{
		attrFn: func(inode *fs.Inode) syscall.Errno {
			mu.Lock()
			defer mu.Unlock()
			if len(names) != 0 {
				t.Error("attribute invalidation waited behind a potentially blocking dentry notification")
			}
			attrs = append(attrs, inode)
			return 0
		},
		entryFn: func(inode *fs.Inode, name string) syscall.Errno {
			mu.Lock()
			defer mu.Unlock()
			if inode != parent {
				t.Error("wrong dentry parent")
			}
			names = append(names, name)
			return 0
		},
	})
	t.Cleanup(i.close)
	change, code := i.reserve([]invalidationTarget{
		{inode: parent}, {inode: parent, name: "positive"}, {inode: source},
		{inode: parent, name: "negative"}, {inode: target},
	})
	if code != 0 {
		t.Fatal(code)
	}
	change.changed(true)
	waitInvalidations(t, i)
	mu.Lock()
	defer mu.Unlock()
	if len(attrs) != 3 || attrs[0] != parent || attrs[1] != source || attrs[2] != target ||
		len(names) != 2 || names[0] != "positive" || names[1] != "negative" {
		t.Fatal("missing parent/live-inode/dentry invalidation", attrs, names)
	}
}
