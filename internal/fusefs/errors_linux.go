//go:build linux

package fusefs

import (
	"context"
	"errors"
	"io/fs"
	"net"
	"strings"
	"syscall"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

func errno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	for _, mapping := range []struct {
		err  error
		code syscall.Errno
	}{
		{fserrors.ErrReadOnly, syscall.EROFS},
		{fserrors.ErrConflict, syscall.ESTALE},
		{fserrors.ErrBusy, syscall.EBUSY},
		{fserrors.ErrTooLarge, syscall.EFBIG},
		{fserrors.ErrCrossDevice, syscall.EXDEV},
		{fserrors.ErrNotEmpty, syscall.ENOTEMPTY},
		{fserrors.ErrUnsupported, syscall.ENOTSUP},
		{fserrors.ErrIsDir, syscall.EISDIR},
		{fserrors.ErrNotDir, syscall.ENOTDIR},
		{fserrors.ErrClosed, syscall.EBADF},
		{fs.ErrNotExist, syscall.ENOENT},
		{fs.ErrExist, syscall.EEXIST},
		{fs.ErrPermission, syscall.EACCES},
		{fs.ErrInvalid, syscall.EINVAL},
		{context.Canceled, syscall.EINTR},
		{context.DeadlineExceeded, syscall.ETIMEDOUT},
	} {
		if errors.Is(err, mapping.err) {
			return mapping.code
		}
	}
	var response *transport.HTTPError
	if errors.As(err, &response) {
		switch response.StatusCode {
		case 400, 416:
			return syscall.EINVAL
		case 401, 403:
			return syscall.EACCES
		case 404:
			return syscall.ENOENT
		case 408, 504:
			return syscall.ETIMEDOUT
		case 409:
			if strings.EqualFold(response.Code, "DirectoryNotEmpty") {
				return syscall.ENOTEMPTY
			}
			if strings.Contains(strings.ToLower(response.Code), "alreadyexists") {
				return syscall.EEXIST
			}
			return syscall.EBUSY
		case 412:
			return syscall.ESTALE
		case 429:
			return syscall.EAGAIN
		case 507:
			return syscall.ENOSPC
		}
	}
	var system syscall.Errno
	if errors.As(err, &system) {
		return system
	}
	var network net.Error
	if errors.As(err, &network) && network.Timeout() {
		return syscall.ETIMEDOUT
	}
	return syscall.EIO
}
