//go:build !unix && !windows

package overlay

import (
	"io/fs"
	"os"

	"fabric-workspace-fs/internal/fserrors"
)

func isReparsePoint(fs.FileInfo) bool { return false }

func checkPrivate(string, fs.FileInfo) error { return fserrors.ErrUnsupported }

func checkHandle(string, *os.File, fs.FileInfo) error { return fserrors.ErrUnsupported }

func isNotEmpty(error) bool { return false }
