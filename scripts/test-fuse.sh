#!/bin/sh
set -eu
cd "$(dirname "$0")/.."
if [ "$(uname -s)" != Linux ] || [ ! -c /dev/fuse ]; then
    echo "Linux with an accessible /dev/fuse is required." >&2
    exit 1
fi
if ! command -v fusermount3 >/dev/null 2>&1 && ! command -v fusermount >/dev/null 2>&1; then
    echo "Install the distribution's fuse3 package before running this test." >&2
    exit 1
fi
export FABRICFS_FUSE_TEST=1
exec go test ./internal/fusefs -run TestMounted -count=1 -v -timeout=5m "$@"
