//go:build windows

package fusefs

func portableOwner() (uint32, uint32) { return 0, 0 }
