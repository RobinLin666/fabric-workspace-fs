//go:build linux

package fusefs

import (
	"context"
	"io"
	"log"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestLinuxGetattrDuringBlockedFlush(t *testing.T) {
	backend, entry, handle, blocked := getattrFlushFixture(t)
	n := &node{adapter: &adapter{backend: backend, logger: log.New(io.Discard, "", 0)}, entry: entry}
	fh := &fileHandle{node: n, handle: handle}
	ctx := context.Background()
	checkGetattrDuringFlush(t, blocked, func() int { return int(fh.Flush(ctx)) }, map[string]func() (int64, int){
		"handle": func() (int64, int) {
			var stat fuse.AttrOut
			code := fh.Getattr(ctx, &stat)
			return int64(stat.Size), int(code)
		},
		"inode": func() (int64, int) {
			var stat fuse.AttrOut
			code := n.Getattr(ctx, nil, &stat)
			return int64(stat.Size), int(code)
		},
	})
	if code := fh.Release(ctx); code != 0 {
		t.Fatalf("release status = %d", code)
	}
}
