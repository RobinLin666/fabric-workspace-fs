package fserrors

import "errors"

var (
	ErrReadOnly    = errors.New("read-only Fabric object")
	ErrConflict    = errors.New("remote object changed since it was opened")
	ErrBusy        = errors.New("object has an active writer")
	ErrTooLarge    = errors.New("configured file size limit exceeded")
	ErrCrossDevice = errors.New("cross-workspace or cross-item rename is not supported")
	ErrNotEmpty    = errors.New("directory is not empty")
	ErrUnsupported = errors.New("operation is not supported")
	ErrIsDir       = errors.New("object is a directory")
	ErrNotDir      = errors.New("object is not a directory")
	ErrClosed      = errors.New("file handle is closed")
)
