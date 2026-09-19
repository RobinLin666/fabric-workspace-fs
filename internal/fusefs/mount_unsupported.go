//go:build !linux && !windows && !darwin

package fusefs

import (
	"fmt"
	"runtime"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/workspacefs"
)

func Supported() bool { return false }

func Mount(string, *workspacefs.FS, Options) (Server, error) {
	return nil, fmt.Errorf("mounting is not supported on %s: %w", runtime.GOOS, fserrors.ErrUnsupported)
}
