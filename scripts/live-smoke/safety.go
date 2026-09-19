package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"syscall"
)

func safeSystemError(err error) string {
	switch {
	case errors.Is(err, context.Canceled):
		return "operation canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "operation deadline exceeded"
	case errors.Is(err, fs.ErrNotExist):
		return "required path was not found"
	case errors.Is(err, fs.ErrPermission):
		return "permission denied"
	case errors.Is(err, fs.ErrExist):
		return "path already exists"
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno.Error()
	}
	return "operation failed (untrusted error details omitted)"
}

func below(parent, candidate string) bool {
	return candidate == parent || strings.HasPrefix(candidate, parent+"/")
}

func validComponent(name string) bool {
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\\x00") {
		return false
	}
	for _, r := range name {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

func validNativeParent(parent string) bool {
	const prefix = "fabric-workspace-fs-test-live-"
	return path.Clean(parent) == parent && path.Dir(parent) == "/tmp" &&
		strings.HasPrefix(path.Base(parent), prefix) && len(path.Base(parent)) > len(prefix)
}

func unescapeMountPath(value string) (string, error) {
	var result strings.Builder
	for i := 0; i < len(value); i++ {
		if value[i] != '\\' {
			result.WriteByte(value[i])
			continue
		}
		if i+3 >= len(value) {
			return "", fail("malformed mountinfo escape")
		}
		octal := value[i+1 : i+4]
		if octal != "040" && octal != "011" && octal != "012" && octal != "134" {
			return "", fail("unexpected mountinfo escape")
		}
		n, err := strconv.ParseUint(octal, 8, 8)
		if err != nil {
			return "", fail("invalid mountinfo escape")
		}
		result.WriteByte(byte(n))
		i += 3
	}
	return result.String(), nil
}

// Malformed or unreadable mountinfo is never treated as evidence of absence.
func subtreeMounted(reader io.Reader, parent string) (bool, error) {
	if !validNativeParent(parent) {
		return false, fail("refusing cleanup of a non-native or unrecognized private parent")
	}
	mounts, err := readMounts(reader)
	if err != nil {
		return false, err
	}
	for point := range mounts {
		if below(parent, point) {
			return true, nil
		}
	}
	return false, nil
}

func readMounts(reader io.Reader) (map[string]string, error) {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	mounts := make(map[string]string)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		separator := -1
		for i, field := range fields {
			if field == "-" {
				separator = i
				break
			}
		}
		if len(fields) < 10 || separator < 6 || len(fields)-separator < 4 {
			return nil, fail("malformed mountinfo; native-path safety could not be established")
		}
		point, err := unescapeMountPath(fields[4])
		if err != nil || !strings.HasPrefix(point, "/") || path.Clean(point) != point {
			return nil, fail("invalid mountpoint in mountinfo; native-path safety could not be established")
		}
		kind := fields[separator+1]
		if previous := mounts[point]; !strings.HasPrefix(previous, "fuse") {
			mounts[point] = kind
		}
	}
	if scanner.Err() != nil {
		return nil, fail("cannot read complete mountinfo; native-path safety could not be established")
	}
	if len(mounts) == 0 {
		return nil, fail("empty mountinfo is not proof that a path is native or unmounted")
	}
	return mounts, nil
}

func insideFuseMount(candidate string, mounts map[string]string) bool {
	for point, kind := range mounts {
		if strings.HasPrefix(kind, "fuse") && (point == "/" || below(point, candidate)) {
			return true
		}
	}
	return false
}

func decodeSmall(reader io.Reader, limit int64, destination any) error {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return fail("bounded response read failed")
	}
	if int64(len(data)) > limit {
		return fail("response exceeded the fixture helper size limit")
	}
	if json.Unmarshal(data, destination) != nil {
		return fail("response was not one valid JSON value")
	}
	return nil
}

func notebookDocument(data []byte) (map[string]json.RawMessage, map[string]json.RawMessage, error) {
	if len(data) > maxNotebookBytes {
		return nil, nil, fail("fixture notebook exceeded the small smoke-test bound")
	}
	var doc map[string]json.RawMessage
	if json.Unmarshal(data, &doc) != nil || doc == nil {
		return nil, nil, fail("fixture notebook is not a JSON object")
	}
	var version int
	if json.Unmarshal(doc["nbformat"], &version) != nil || version != 4 {
		return nil, nil, fail("fixture notebook is not nbformat 4")
	}
	var cells []json.RawMessage
	rawCells := bytes.TrimSpace(doc["cells"])
	if len(rawCells) != 0 && !bytes.Equal(rawCells, []byte("null")) && (json.Unmarshal(rawCells, &cells) != nil || cells == nil) {
		return nil, nil, fail("fixture notebook has no valid cells array")
	}
	metadata := make(map[string]json.RawMessage)
	if raw, exists := doc["metadata"]; exists {
		if json.Unmarshal(raw, &metadata) != nil || metadata == nil {
			return nil, nil, fail("fixture notebook metadata is not an object")
		}
	}
	return doc, metadata, nil
}

func setNotebookMarker(data []byte, marker string) ([]byte, error) {
	doc, metadata, err := notebookDocument(data)
	if err != nil {
		return nil, err
	}
	metadata["fabricfs_test"], err = json.Marshal(marker)
	if err != nil {
		return nil, fail("fixture marker encoding failed")
	}
	doc["metadata"], err = json.Marshal(metadata)
	if err != nil {
		return nil, fail("fixture metadata encoding failed")
	}
	result, err := json.Marshal(doc)
	if err != nil || len(result) > maxNotebookBytes {
		return nil, fail("updated fixture notebook exceeds its bound")
	}
	return append(result, '\n'), nil
}

func hasNotebookMarker(data []byte, marker string) error {
	_, metadata, err := notebookDocument(data)
	if err != nil {
		return err
	}
	var actual string
	if json.Unmarshal(metadata["fabricfs_test"], &actual) != nil || actual != marker {
		return fail("saved notebook marker did not round-trip semantically")
	}
	return nil
}

func initialNotebook() []byte {
	return []byte(`{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"kernelspec":{"display_name":"Python 3","language":"python","name":"python3"},"language_info":{"name":"python"}}}`)
}
