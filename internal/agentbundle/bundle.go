// Package agentbundle contains the mount's versioned, read-only agent guidance.
package agentbundle

import (
	_ "embed"
	"fmt"
	"io/fs"
	"strings"
	"unicode"
)

//go:embed AGENT.md
var agentGuide string

//go:embed fabric-fuse-skill.md
var fabricFuseSkill string

// Files returns a newly rendered bundle. It contains no per-user credentials,
// tenant state, remote content, or mutable agent session files.
func Files(version, fntkExecutable string) (map[string][]byte, error) {
	if version == "" {
		version = "dev"
	}
	if len(version) > 128 || len(fntkExecutable) > 4096 || strings.Contains(version, "`") {
		return nil, fmt.Errorf("invalid agent bundle version or fntk path: %w", fs.ErrInvalid)
	}
	for _, value := range []string{version, fntkExecutable} {
		for _, r := range value {
			if unicode.IsControl(r) {
				return nil, fmt.Errorf("agent bundle fields must be single-line values: %w", fs.ErrInvalid)
			}
		}
	}
	if fntkExecutable == "" {
		fntkExecutable = "fntk"
	}
	replace := strings.NewReplacer(
		"{{VERSION}}", version,
		"{{FNTK}}", "'"+strings.ReplaceAll(fntkExecutable, "'", "'\\''")+"'",
	)
	return map[string][]byte{
		"AGENT.md":                    []byte(replace.Replace(agentGuide)),
		"skills/fabric-fuse/SKILL.md": []byte(replace.Replace(fabricFuseSkill)),
	}, nil
}
