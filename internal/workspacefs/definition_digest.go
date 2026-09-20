package workspacefs

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"hash"
	"io/fs"
	"sort"

	"fabric-workspace-fs/internal/fabric"
)

// digest streams the same JSON representation as json.Marshal(def), without
// materializing the base64 payload repeatedly inside nested JSON envelopes.
// Keep this private: Fabric's wire marshalers remain the serialization authority.
func digest(def fabric.Definition) ([32]byte, error) {
	w := definitionDigestWriter{hash: sha256.New()}
	if err := w.definition(def); err != nil {
		return [32]byte{}, err
	}
	var sum [32]byte
	w.hash.Sum(sum[:0])
	return sum, nil
}

type definitionDigestWriter struct {
	hash    hash.Hash
	scratch [4096]byte
}

func (w *definitionDigestWriter) text(s string) {
	for len(s) != 0 {
		n := copy(w.scratch[:], s)
		_, _ = w.hash.Write(w.scratch[:n])
		s = s[n:]
	}
}

func (w *definitionDigestWriter) quoted(s string) {
	// Normal definition payloads are base64, requiring no JSON escaping.
	// Delegate unusual strings to encoding/json so invalid UTF-8, HTML, control
	// characters, and U+2028/U+2029 retain exactly its normalization behavior.
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < 0x20 || c >= 0x80 || c == '"' || c == '\\' || c == '<' || c == '>' || c == '&' {
			data, _ := json.Marshal(s)
			_, _ = w.hash.Write(data)
			return
		}
	}
	w.text(`"`)
	w.text(s)
	w.text(`"`)
}

func (w *definitionDigestWriter) raw(value json.RawMessage, enclosingDepth int) error {
	// RawMessage preserves number spelling, object order, and string escapes.
	// Unmarshaling it into interface{} would silently change digest equality.
	// Marshal also provides JSON validation, compaction, HTML escaping, and
	// the distinction between nil RawMessage (null) and an invalid empty one.
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("invalid definition metadata: %w", fs.ErrInvalid)
	}
	// The outer Definition.MarshalJSON result is also validated by
	// encoding/json. Include its enclosing containers in the standard
	// library's 10,000-level nesting limit, not just the raw value's depth.
	depth, quoted := enclosingDepth, false
	for i := 0; i < len(data); i++ {
		c := data[i]
		if quoted {
			if c == '\\' {
				i++
			} else if c == '"' {
				quoted = false
			}
			continue
		}
		switch c {
		case '"':
			quoted = true
		case '[', '{':
			depth++
			if depth > 10000 {
				return fmt.Errorf("definition metadata exceeds JSON nesting limit: %w", fs.ErrInvalid)
			}
		case ']', '}':
			depth--
		}
	}
	_, _ = w.hash.Write(data)
	return nil
}

func (w *definitionDigestWriter) definition(def fabric.Definition) error {
	keys := make([]string, 0, len(def.Extra)+2)
	for key := range def.Extra {
		if key != "format" && key != "parts" {
			keys = append(keys, key)
		}
	}
	if def.Format != "" {
		keys = append(keys, "format")
	}
	keys = append(keys, "parts")
	sort.Strings(keys)
	w.text("{")
	for i, key := range keys {
		if i != 0 {
			w.text(",")
		}
		w.quoted(key)
		w.text(":")
		switch key {
		case "format":
			w.quoted(def.Format)
		case "parts":
			if def.Parts == nil {
				w.text("null")
				continue
			}
			w.text("[")
			for i, part := range def.Parts {
				if i != 0 {
					w.text(",")
				}
				if err := w.part(part); err != nil {
					return err
				}
			}
			w.text("]")
		default:
			if err := w.raw(def.Extra[key], 1); err != nil {
				return err
			}
		}
	}
	w.text("}")
	return nil
}

func (w *definitionDigestWriter) part(part fabric.Part) error {
	keys := make([]string, 0, len(part.Extra)+3)
	for key := range part.Extra {
		if key != "path" && key != "payload" && key != "payloadType" {
			keys = append(keys, key)
		}
	}
	keys = append(keys, "path", "payload", "payloadType")
	sort.Strings(keys)
	w.text("{")
	for i, key := range keys {
		if i != 0 {
			w.text(",")
		}
		w.quoted(key)
		w.text(":")
		switch key {
		case "path":
			w.quoted(part.Path)
		case "payload":
			w.quoted(part.Payload)
		case "payloadType":
			w.quoted(part.PayloadType)
		default:
			if err := w.raw(part.Extra[key], 3); err != nil {
				return err
			}
		}
	}
	w.text("}")
	return nil
}
