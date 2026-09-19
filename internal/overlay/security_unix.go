//go:build unix

package overlay

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

func isReparsePoint(fs.FileInfo) bool { return false }

func checkPrivate(path string, info fs.FileInfo) error {
	if err := checkType(path, info); err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(stat.Uid) != uint64(os.Geteuid()) {
		return unsafeEntry(path, "not owned by the current user")
	}
	want := fs.FileMode(0o600)
	if info.IsDir() {
		want = 0o700
	}
	if info.Mode().Perm() != want || info.Mode()&(fs.ModeSetuid|fs.ModeSetgid|fs.ModeSticky) != 0 {
		return unsafeEntry(path, "storage permissions must be 0700 for directories and 0600 for files")
	}
	if info.Mode().IsRegular() && stat.Nlink != 1 {
		return unsafeEntry(path, "hard-linked file")
	}
	return nil
}

func checkHandle(path string, _ *os.File, info fs.FileInfo) error {
	return checkPrivate(path, info)
}

func isNotEmpty(err error) bool {
	return errors.Is(err, syscall.ENOTEMPTY) || errors.Is(err, syscall.EEXIST)
}
