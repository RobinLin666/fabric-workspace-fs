package overlay

import (
	"errors"
	"io/fs"
	"os"
	"syscall"
)

func isReparsePoint(info fs.FileInfo) bool {
	data, ok := info.Sys().(*syscall.Win32FileAttributeData)
	return ok && data.FileAttributes&syscall.FILE_ATTRIBUTE_REPARSE_POINT != 0
}

func checkPrivate(path string, info fs.FileInfo) error {
	return checkType(path, info)
}

func checkHandle(path string, file *os.File, info fs.FileInfo) error {
	if err := checkPrivate(path, info); err != nil {
		return err
	}
	if info.Mode().IsRegular() {
		var data syscall.ByHandleFileInformation
		if err := syscall.GetFileInformationByHandle(syscall.Handle(file.Fd()), &data); err != nil {
			return err
		}
		if data.NumberOfLinks != 1 {
			return unsafeEntry(path, "hard-linked file")
		}
	}
	return nil
}

func isNotEmpty(err error) bool {
	return errors.Is(err, syscall.ERROR_DIR_NOT_EMPTY) || errors.Is(err, syscall.ENOTEMPTY)
}
