//go:build windows

package fusefs

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"golang.org/x/sys/windows/registry"
)

func CheckPrerequisites() error {
	name := map[string]string{
		"386": "winfsp-x86.dll", "amd64": "winfsp-x64.dll", "arm64": "winfsp-a64.dll",
	}[runtime.GOARCH]
	if name == "" {
		return fmt.Errorf("WinFsp does not support Windows architecture %s", runtime.GOARCH)
	}
	key, err := registry.OpenKey(registry.LOCAL_MACHINE, `SOFTWARE\WinFsp`, registry.QUERY_VALUE|registry.WOW64_32KEY)
	if err == nil {
		defer key.Close()
		if directory, _, valueErr := key.GetStringValue("InstallDir"); valueErr == nil {
			if info, statErr := os.Stat(filepath.Join(directory, "bin", name)); statErr == nil && info.Mode().IsRegular() {
				return nil
			}
		}
	}
	return errors.New("WinFsp 2.1 runtime is required; install it from https://github.com/winfsp/winfsp/releases/tag/v2.1")
}
