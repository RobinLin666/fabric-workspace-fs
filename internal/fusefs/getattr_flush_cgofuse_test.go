//go:build windows || (darwin && cgo)

package fusefs

import (
	"io"
	"log"
	"testing"

	"github.com/winfsp/cgofuse/fuse"
)

func TestPortableGetattrDuringBlockedFlush(t *testing.T) {
	backend, entry, handle, blocked := getattrFlushFixture(t)
	adapter := newPortableFS(backend, log.New(io.Discard, "", 0))
	fh := adapter.alloc(entry, handle, fuse.O_RDWR)
	_, _, files, _, _ := portablePaths(t)
	name := files + "/demo.txt"
	checkGetattrDuringFlush(t, blocked, func() int { return adapter.Flush(name, fh) }, map[string]func() (int64, int){
		"handle": func() (int64, int) {
			var stat fuse.Stat_t
			code := adapter.Getattr(name, &stat, fh)
			return stat.Size, code
		},
		"path": func() (int64, int) {
			var stat fuse.Stat_t
			code := adapter.Getattr(name, &stat, ^uint64(0))
			return stat.Size, code
		},
	})
	if code := adapter.Release(name, fh); code != 0 {
		t.Fatalf("release status = %d", code)
	}
}
