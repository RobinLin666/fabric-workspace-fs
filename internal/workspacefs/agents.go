package workspacefs

import (
	"io/fs"
	"sort"
	"strings"

	"fabric-workspace-fs/internal/namespace"
)

func (s *FS) agentRoot() Entry {
	return Entry{Name: namespace.AgentRootName, Kind: AgentDirectory, Directory: true, Fixed: true, Modified: s.start}
}

func (s *FS) agentChildren(parent Entry) ([]Entry, error) {
	prefix := parent.Part
	if prefix != "" {
		prefix += "/"
	}
	children := make(map[string]Entry)
	for path, data := range s.agentFiles {
		if path == namespace.AgentGuideName {
			continue
		}
		if !strings.HasPrefix(path, prefix) {
			continue
		}
		name, _, directory := strings.Cut(strings.TrimPrefix(path, prefix), "/")
		entry := Entry{Name: name, Kind: AgentFile, Part: prefix + name, Size: int64(len(data)), Modified: s.start}
		if directory {
			entry.Kind, entry.Directory, entry.Fixed, entry.Size = AgentDirectory, true, true, 0
		}
		children[name] = entry
	}
	if len(children) == 0 {
		return nil, fs.ErrNotExist
	}
	out := make([]Entry, 0, len(children))
	for _, entry := range children {
		out = append(out, entry)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}
