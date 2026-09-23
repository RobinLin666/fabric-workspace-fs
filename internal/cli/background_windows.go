//go:build windows

package cli

import (
	"os/exec"
	"syscall"
)

const detachedProcess = 0x00000008

func configureBackgroundProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{
		CreationFlags: syscall.CREATE_NEW_PROCESS_GROUP | detachedProcess,
		HideWindow:    true,
	}
}
