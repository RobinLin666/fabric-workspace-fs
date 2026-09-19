//go:build linux

package fusefs

import (
	"log"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

const maxInvalidations = 256

type kernelNotifier interface {
	attr(*fs.Inode) syscall.Errno
	entry(*fs.Inode, string) syscall.Errno
}

type inodeNotifier struct{}

func (inodeNotifier) attr(inode *fs.Inode) syscall.Errno {
	// Negative offset invalidates attributes only. Every open is DIRECT_IO;
	// invalidating data pages is unnecessary and can block on pending IO.
	return inode.NotifyContent(-1, 0)
}

func (inodeNotifier) entry(inode *fs.Inode, name string) syscall.Errno {
	return inode.NotifyEntry(name)
}

type invalidationTarget struct {
	inode *fs.Inode
	name  string
}

type invalidation struct {
	owner   *invalidator
	targets []invalidationTarget
	dirty   bool
	retired bool
}

type invalidator struct {
	mu       sync.Mutex
	jobs     map[*invalidation]struct{}
	limit    int
	err      syscall.Errno
	closed   bool
	wake     chan struct{}
	stop     chan struct{}
	done     chan struct{}
	notifier kernelNotifier
	logger   *log.Logger
}

func newInvalidator(logger *log.Logger, notifier kernelNotifier) *invalidator {
	i := &invalidator{
		jobs: make(map[*invalidation]struct{}), limit: maxInvalidations,
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
		notifier: notifier, logger: logger,
	}
	go i.run()
	return i
}

// Reserve before touching the backend. A full coordinator fails the operation
// rather than dropping an invalidation or waiting for a notification which may
// itself be waiting for this FUSE request to return. Writers reuse their slot
// until RELEASE, including across duplicate-descriptor FLUSH requests.
func (i *invalidator) reserve(targets []invalidationTarget) (*invalidation, syscall.Errno) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.closed {
		return nil, syscall.ENODEV
	}
	if i.err != 0 {
		return nil, i.err
	}
	if len(i.jobs) >= i.limit {
		return nil, syscall.EAGAIN
	}
	change := &invalidation{owner: i, targets: targets}
	i.jobs[change] = struct{}{}
	return change, 0
}

func (c *invalidation) check() syscall.Errno {
	if c == nil {
		return 0
	}
	c.owner.mu.Lock()
	defer c.owner.mu.Unlock()
	if c.owner.closed {
		return syscall.ENODEV
	}
	return c.owner.err
}

func (c *invalidation) add(inode *fs.Inode) {
	c.owner.mu.Lock()
	c.targets = append(c.targets, invalidationTarget{inode: inode})
	c.owner.mu.Unlock()
}

func (c *invalidation) changed(retire bool) {
	if c == nil {
		return
	}
	i := c.owner
	i.mu.Lock()
	if !i.closed {
		c.dirty = true
		c.retired = c.retired || retire
		select {
		case i.wake <- struct{}{}:
		default:
		}
	}
	i.mu.Unlock()
}

func (i *invalidator) run() {
	defer func() {
		i.mu.Lock()
		i.jobs = nil
		i.mu.Unlock()
		close(i.done)
	}()
	for {
		select {
		case <-i.stop:
			return
		case <-i.wake:
		}
		i.mu.Lock()
		batch := make([]*invalidation, 0, len(i.jobs))
		for change := range i.jobs {
			if change.dirty {
				change.dirty = false
				batch = append(batch, change)
			}
		}
		i.mu.Unlock()
		for _, change := range batch {
			i.mu.Lock()
			targets := append([]invalidationTarget(nil), change.targets...)
			i.mu.Unlock()
			// No adapter, inode, handle, or coordinator lock is held here.
			// EntryNotify may wait for LOOKUP/RELEASE or the originating
			// namespace request, so callbacks must never wait for this loop.
			for _, entries := range []bool{false, true} {
				for _, target := range targets {
					if (target.name != "") != entries {
						continue
					}
					var code syscall.Errno
					if entries {
						code = i.notifier.entry(target.inode, target.name)
					} else {
						code = i.notifier.attr(target.inode)
					}
					i.failure(code, target.name)
				}
			}
			i.mu.Lock()
			if change.retired && !change.dirty {
				delete(i.jobs, change)
			}
			i.mu.Unlock()
		}
	}
}

func (i *invalidator) failure(code syscall.Errno, name string) {
	// ENOENT means the kernel already forgot the inode/dentry, including
	// newly created entries which have not reached the kernel yet.
	if code == 0 || code == syscall.ENOENT {
		return
	}
	i.mu.Lock()
	first := !i.closed && i.err == 0
	if first {
		i.err = code
	}
	i.mu.Unlock()
	if first {
		i.logger.Printf("kernel cache invalidation failed (entry %q): %v; further mutations will fail", name, code)
	}
}

func (i *invalidator) close() {
	i.mu.Lock()
	if !i.closed {
		i.closed = true
		close(i.stop)
	}
	i.mu.Unlock()
	<-i.done
}
