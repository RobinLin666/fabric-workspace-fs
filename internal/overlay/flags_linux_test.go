package overlay

import (
	"context"
	"os"
	"syscall"
	"testing"
)

func TestLinuxSyncFlagsReachCreatedAndOpenedDescriptors(t *testing.T) {
	for _, tc := range []struct {
		name string
		flag int
	}{{"sync", os.O_SYNC}, {"data sync", syscall.O_DSYNC}} {
		t.Run(tc.name, func(t *testing.T) {
			store, _ := newTestStore(t, Options{})
			ctx := context.Background()
			mkdir(t, store, ".agents")
			flags := os.O_RDWR | tc.flag
			file, err := store.Create(ctx, at(".agents/file"), flags|os.O_CREATE|os.O_EXCL)
			if err != nil {
				t.Fatal(err)
			}
			checkLinuxDescriptorFlags(t, file, tc.flag)
			if n, err := file.WriteAt(ctx, []byte("data"), 0); n != 4 || err != nil {
				t.Fatalf("synchronous write = %d, %v", n, err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			file, err = store.Open(ctx, at(".agents/file"), flags|os.O_APPEND)
			if err != nil {
				t.Fatal(err)
			}
			checkLinuxDescriptorFlags(t, file, tc.flag|os.O_APPEND)
			if n, err := file.WriteAt(ctx, []byte("!"), 0); n != 1 || err != nil {
				t.Fatalf("synchronous append = %d, %v", n, err)
			}
			if err := file.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := file.Close(); err != nil {
				t.Fatal(err)
			}
			if got := read(t, store, ".agents/file"); got != "data!" {
				t.Fatalf("synchronous append data = %q", got)
			}
		})
	}
}

func checkLinuxDescriptorFlags(t *testing.T, file *File, want int) {
	t.Helper()
	flags, _, errno := syscall.Syscall(syscall.SYS_FCNTL, file.file.Fd(), syscall.F_GETFL, 0)
	if errno != 0 {
		t.Fatal(errno)
	}
	if int(flags)&want != want {
		t.Fatalf("descriptor flags = %#x, missing %#x", flags, want)
	}
}
