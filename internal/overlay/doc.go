// Package overlay stores local dot-directories without contacting Fabric.
//
// A Store exclusively manages an explicitly supplied, absolute storage directory.
// Its layout is <workspace UUID>/<parent UUID or root>/<dot-directory>/... .
// UUIDs are normalized to lowercase; display names never identify storage.
// Info.Path is relative to the scoped container, using slash separators.
//
// Storage must be owned by the current user and private on Unix: directories
// have mode 0700 and regular files 0600. Symlinks, reparse points, hard-linked
// files, special files, reserved metadata names, and nonportable names are
// rejected. All filesystem operations use os.Root or already-open file handles.
// Rooted operations provide confinement even if a path changes after validation.
//
// Quotas count logical file sizes (including holes) and user-visible entries,
// including dot-directories. Internal workspace and parent containers each have
// a separate MaxEntries bound, so even malformed or empty namespaces are scanned
// in bounded space and time. New validates and accounts for existing storage.
// Out-of-band namespace modifications while a Store is open are unsupported;
// detected replacements fail rather than silently invalidating accounting.
//
// Handles may share a file for reading, with at most one writable handle per
// file. Removing or renaming a path with any open handle, including descendants,
// returns fserrors.ErrBusy. Quota failures use syscall.ENOSPC; the per-file size
// limit uses fserrors.ErrTooLarge. Close is idempotent and closes owned handles.
package overlay
