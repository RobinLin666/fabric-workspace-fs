package fusefs

import (
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"runtime"
	"strings"
)

type Options struct {
	ReadOnly bool
	Logger   *log.Logger
}

type Server interface {
	WaitMount() error
	Unmount() error
	Wait()
}

func failedMount(server Server, mountpoint string, cause error) (Server, error) {
	if err := server.Unmount(); err != nil {
		return server, fmt.Errorf("mount startup failed; shutdown is not confirmed at %q, retain this mountpoint: %w", mountpoint, errors.Join(cause, err))
	}
	return nil, cause
}

func NormalizeMountpoint(raw string) (string, error) {
	if runtime.GOOS == "windows" && len(raw) == 2 && raw[1] == ':' &&
		((raw[0] >= 'a' && raw[0] <= 'z') || (raw[0] >= 'A' && raw[0] <= 'Z')) {
		return strings.ToUpper(raw), nil
	}
	return filepath.Abs(raw)
}

func UnmountHelp(mountpoint string) string {
	switch runtime.GOOS {
	case "windows":
		return fmt.Sprintf("close users of %q and retry; WinFsp also supports `fsptool-x64.exe unmount %s`", mountpoint, mountpoint)
	case "darwin":
		return fmt.Sprintf("close users of %q and retry; then use `diskutil unmount %s` or `umount %s`", mountpoint, mountpoint, mountpoint)
	default:
		return fmt.Sprintf("close users of %q and retry; then use `fusermount3 -u %s`", mountpoint, mountpoint)
	}
}
