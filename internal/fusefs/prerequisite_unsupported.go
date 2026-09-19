//go:build (!linux && !windows && !darwin) || (darwin && !cgo)

package fusefs

import (
	"fmt"
	"runtime"

	"fabric-workspace-fs/internal/fserrors"
)

func CheckPrerequisites() error {
	return fmt.Errorf("mounting is unavailable in this %s build: %w", runtime.GOOS, fserrors.ErrUnsupported)
}
