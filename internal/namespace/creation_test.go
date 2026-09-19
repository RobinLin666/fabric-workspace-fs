package namespace

import (
	"errors"
	"io/fs"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
)

func TestCanonicalCreationRouting(t *testing.T) {
	for _, test := range []struct {
		name, display, kind string
		local               bool
	}{
		{"Demo.Notebook", "Demo", "Notebook", false},
		{"Demo.Lakehouse", "Demo", "Lakehouse", false},
		{"Demo.Environment", "Demo", "Environment", false},
		{"ordinary.Foo", "ordinary.Foo", "Folder", false},
		{"ordinary.notebook", "ordinary.notebook", "Folder", false},
		{"A%2FB.Notebook", "A/B", "Notebook", false},
		{"My Folder", "My Folder", "Folder", false},
		{".agents", ".agents", "Folder", false},
		{".agents.Environment", ".agents", "Environment", false},
	} {
		got, err := ParseCreation(test.name)
		if err != nil || got.DisplayName != test.display || got.Type != test.kind || got.Local != test.local {
			t.Fatalf("%q => %+v %v", test.name, got, err)
		}
	}
	for _, name := range []string{"", ".", "..", ".Notebook", "a/b", `a\b`, "%2Eagents", "%61.Notebook", "x%", "x%00.Notebook"} {
		got, err := ParseCreation(name)
		if !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("invalid creation accepted %q: %+v %v", name, got, err)
		}
	}
	for _, name := range []string{".fabric.json", ".platform"} {
		if _, err := ParseCreation(name); !errors.Is(err, fserrors.ErrReadOnly) {
			t.Error("reserved creation accepted", name, err)
		}
	}
}
