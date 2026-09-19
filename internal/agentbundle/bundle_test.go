package agentbundle

import (
	"bytes"
	"testing"
)

func TestBundleIsVersionedAndReturnedStorageIsPrivate(t *testing.T) {
	files, err := Files("0.3.0+example", "/home/example/.local/bin/fntk")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 {
		t.Fatal("bundle must contain one singular AGENT.md and one consolidated skill")
	}
	for _, path := range []string{"AGENT.md", "skills/fabric-fuse/SKILL.md"} {
		if !bytes.Contains(files[path], []byte("0.3.0+example")) || bytes.Contains(files[path], []byte("{{")) {
			t.Fatal("unbound bundle template", path)
		}
		if !bytes.Contains(files[path], []byte("/home/example/.local/bin/fntk")) {
			t.Fatal("external fntk executable missing from bundle", path)
		}
	}
	files["AGENT.md"][0] = 'x'
	fresh, _ := Files("0.3.0+example", "")
	if fresh["AGENT.md"][0] == 'x' {
		t.Fatal("caller mutated shared bundle template")
	}
	for _, bad := range []string{"version\ninjected", "version`injected"} {
		if _, err := Files(bad, ""); err == nil {
			t.Fatal("unsafe template version accepted")
		}
	}
}
