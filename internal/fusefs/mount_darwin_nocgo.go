//go:build darwin && !cgo

package fusefs

import (
	"fmt"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/workspacefs"
)

func Supported() bool { return false }

func Mount(string, *workspacefs.FS, Options) (Server, error) {
	return nil, fmt.Errorf("macOS mounting requires a CGO-enabled build and macFUSE: %w", fserrors.ErrUnsupported)
}
