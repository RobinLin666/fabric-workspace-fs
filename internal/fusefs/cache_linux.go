//go:build linux

package fusefs

import (
	"errors"
	"syscall"
	"time"

	"fabric-workspace-fs/internal/workspacefs"

	"github.com/hanwen/go-fuse/v2/fuse"
)

type cacheRawFS struct {
	fuse.RawFileSystem
}

func (f *cacheRawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	status := f.RawFileSystem.Lookup(cancel, header, name, out)
	// go-fuse v2.11 only translates ENOENT to a negative EntryOut when its
	// global fallback supplies the timeout. A per-directory timeout needs
	// the same successful, zero-node-ID wire response here.
	if status == fuse.ENOENT && out.NodeId == 0 && out.EntryTimeout() > 0 {
		return fuse.OK
	}
	return status
}

func (n *node) attrOut(e workspacefs.Entry, out *fuse.AttrOut) {
	n.attr(e, &out.Attr)
	attr, _, _ := n.adapter.backend.CacheTimeouts(e)
	out.SetTimeout(max(attr, 0))
}

func (n *node) entryOut(e workspacefs.Entry, out *fuse.EntryOut) {
	n.attr(e, &out.Attr)
	attr, entry, _ := n.adapter.backend.CacheTimeouts(e)
	out.SetAttrTimeout(max(attr, 0))
	out.SetEntryTimeout(max(entry, 0))
}

func (n *node) negativeOut(e workspacefs.Entry, cause error, out *fuse.EntryOut) {
	out.NodeId = 0
	var source interface{ CacheDeadline() time.Time }
	if errors.As(cause, &source) {
		deadline := source.CacheDeadline()
		// A fresh parent stat is not fresh proof that its child is absent.
		// Keep the miss's original deadline, including explicit unknown/due
		// proofs, without changing the parent inode's attribute deadline.
		if deadline.IsZero() {
			out.SetEntryTimeout(0)
			return
		}
		if e.ValidUntil.IsZero() || deadline.Before(e.ValidUntil) {
			e.ValidUntil = deadline
		}
	}
	_, _, negative := n.adapter.backend.CacheTimeouts(e)
	out.SetEntryTimeout(max(negative, 0))
}

func notificationSupport(settings *fuse.InitIn) syscall.Errno {
	if !settings.SupportsNotify(fuse.NOTIFY_INVAL_INODE) || !settings.SupportsNotify(fuse.NOTIFY_INVAL_ENTRY) {
		return syscall.ENOSYS
	}
	return 0
}
