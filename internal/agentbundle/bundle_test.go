package agentbundle

import (
	"bytes"
	"testing"
)

func TestBundleContentsAndReturnedStorageArePrivate(t *testing.T) {
	files, err := Files("0.3.0+example", "/home/example/.local/bin/fntk")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatal("bundle must contain root AGENTS.md and the notebook workflow skill")
	}
	if _, ok := files["AGENTS.md"]; !ok {
		t.Fatal("root AGENTS.md is missing")
	}
	if _, ok := files["skills/fabric-notebook-workflow/SKILL.md"]; !ok {
		t.Fatal("fabric-notebook-workflow skill is missing")
	}
	if _, ok := files["skills/fabric-fuse/SKILL.md"]; ok {
		t.Fatal("retired fabric-fuse skill is still bundled")
	}
	if bytes.Contains(files["AGENTS.md"], []byte("0.3.0+example")) ||
		bytes.Contains(files["AGENTS.md"], []byte("/home/example/.local/bin/fntk")) ||
		bytes.Contains(files["AGENTS.md"], []byte("{{")) {
		t.Fatal("root guide contains runtime-specific substitutions")
	}
	if !bytes.Contains(files["skills/fabric-notebook-workflow/SKILL.md"], []byte("name: fabric-notebook-workflow")) {
		t.Fatal("workflow skill metadata is missing")
	}
	files["AGENTS.md"][0] = 'x'
	fresh, _ := Files("0.3.0+example", "")
	if fresh["AGENTS.md"][0] == 'x' {
		t.Fatal("caller mutated shared bundle template")
	}
	for _, bad := range []string{"version\ninjected", "version`injected"} {
		if _, err := Files(bad, ""); err == nil {
			t.Fatal("unsafe template version accepted")
		}
	}
}
