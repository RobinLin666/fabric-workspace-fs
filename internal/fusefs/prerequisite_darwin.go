//go:build darwin && cgo

package fusefs

import (
	"errors"
	"os"
)

func CheckPrerequisites() error {
	info, err := os.Stat("/Library/Filesystems/macfuse.fs")
	if err != nil || !info.IsDir() {
		return errors.New("macFUSE is required; install it from https://macfuse.github.io/ and retry")
	}
	return nil
}
