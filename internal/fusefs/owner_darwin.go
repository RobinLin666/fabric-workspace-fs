//go:build darwin && cgo

package fusefs

import "os"

func portableOwner() (uint32, uint32) {
	return uint32(os.Getuid()), uint32(os.Getgid())
}
