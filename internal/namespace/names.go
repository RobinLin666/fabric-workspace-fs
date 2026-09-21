package namespace

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"net/url"
	"strings"
	"unicode"
	"unicode/utf8"

	"fabric-workspace-fs/internal/fserrors"
)

const (
	maxNameBytes        = 255
	NotebookContentName = "content.ipynb"
)

// NotebookContentFileName returns the local Notebook content filename.
// Empty display names are retained for synthetic test entries and legacy
// callers; real Fabric Notebook items always have a display name.
func NotebookContentFileName(displayName string) string {
	return NotebookContentFileNameWithExtension(displayName, ".ipynb")
}

func NotebookContentFileNameWithExtension(displayName, extension string) string {
	if extension != ".ipynb" && extension != ".py" {
		extension = ".ipynb"
	}
	if displayName == "" {
		if extension == ".ipynb" {
			return NotebookContentName
		}
		return strings.TrimSuffix(NotebookContentName, ".ipynb") + extension
	}
	encoded, err := Encode(displayName)
	if err != nil {
		if extension == ".ipynb" {
			return NotebookContentName
		}
		return strings.TrimSuffix(NotebookContentName, ".ipynb") + extension
	}
	if len(encoded) > maxNameBytes-len(extension) {
		encoded = shorten(encoded, maxNameBytes-len(extension))
	}
	return encoded + extension
}

// Encode escapes bytes rather than normalizing Unicode, so distinct remote names
// never become aliases. Only catalog labels may contain path separators.
func Encode(name string) (string, error) {
	if name == "" || !utf8.ValidString(name) || strings.ContainsRune(name, 0) {
		return "", fmt.Errorf("invalid name: %w", fs.ErrInvalid)
	}
	var out strings.Builder
	for i, r := range name {
		escape := unicode.IsControl(r) || strings.ContainsRune(`/\%<>:"|?*`, r)
		escape = escape || ((r == '.' || r == ' ') && i+utf8.RuneLen(r) == len(name))
		escape = escape || ((name == "." || name == "..") && r == '.')
		if escape {
			for _, b := range []byte(string(r)) {
				fmt.Fprintf(&out, "%%%02X", b)
			}
		} else {
			out.WriteRune(r)
		}
	}
	encoded := out.String()
	if windowsDeviceName(name) {
		encoded = fmt.Sprintf("%%%02X%s", encoded[0], encoded[1:])
	}
	return encoded, nil
}

func windowsDeviceName(name string) bool {
	stem := name
	if dot := strings.IndexByte(stem, '.'); dot >= 0 {
		stem = stem[:dot]
	}
	stem = strings.ToUpper(stem)
	switch stem {
	case "CON", "PRN", "AUX", "NUL", "CLOCK$":
		return true
	}
	if len(stem) == 4 && stem[3] >= '1' && stem[3] <= '9' {
		return stem[:3] == "COM" || stem[:3] == "LPT"
	}
	return false
}

func FileName(remote string) (string, error) {
	if err := Component(remote); err != nil {
		return "", err
	}
	name, err := Encode(remote)
	if err != nil {
		return "", err
	}
	if len(name) > maxNameBytes {
		return "", fmt.Errorf("escaped filename exceeds 255 bytes: %w", fserrors.ErrTooLarge)
	}
	return name, nil
}

func ParseFileName(local string) (string, error) {
	if local == "" || len(local) > maxNameBytes {
		return "", fmt.Errorf("invalid filename length: %w", fs.ErrInvalid)
	}
	remote, err := url.PathUnescape(local)
	if err != nil {
		return "", fmt.Errorf("invalid filename escape: %w", fs.ErrInvalid)
	}
	canonical, err := FileName(remote)
	if err != nil {
		return "", err
	}
	if canonical != local {
		return "", fmt.Errorf("noncanonical filename (use %q): %w", canonical, fs.ErrInvalid)
	}
	return remote, nil
}

func Component(name string) error {
	if name == "" || name == "." || name == ".." || !utf8.ValidString(name) ||
		strings.ContainsAny(name, "/\\\x00") {
		return fmt.Errorf("invalid path component: %w", fs.ErrInvalid)
	}
	return nil
}

func PartPath(path string) error {
	for _, part := range strings.Split(path, "/") {
		if err := Component(part); err != nil {
			return fmt.Errorf("invalid definition part path: %w", err)
		}
	}
	return nil
}

func CatalogName(label, id string) (string, error) {
	if !validID(id) {
		return "", fmt.Errorf("invalid catalog ID: %w", fs.ErrInvalid)
	}
	if label == "" {
		label = "unnamed"
	}
	encoded, err := Encode(label)
	if err != nil {
		return "", err
	}
	if encoded == ".fabric.json" || encoded == ".platform" {
		encoded = "%2E" + encoded[1:]
	}
	return shorten(encoded, 220), nil
}

func validID(id string) bool {
	if len(id) != 36 {
		return false
	}
	for i, c := range id {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
		} else if !strings.ContainsRune("0123456789abcdefABCDEF", c) {
			return false
		}
	}
	return true
}

func shorten(name string, limit int) string {
	if len(name) <= limit {
		return name
	}
	sum := sha256.Sum256([]byte(name))
	end := limit - 17
	for end > 0 && !utf8.RuneStart(name[end]) {
		end--
	}
	if i := strings.LastIndexByte(name[:end], '%'); i >= 0 && end-i < 3 {
		end = i
	}
	return name[:end] + "-" + hex.EncodeToString(sum[:8])
}
