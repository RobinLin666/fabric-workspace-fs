package resources

import (
	"errors"
	"io/fs"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
)

func TestResourceIdentityAndEnvironmentAliasStayReadonly(t *testing.T) {
	notebook := Target{WorkspaceID: "11111111-1111-1111-1111-111111111111", ItemID: "22222222-2222-2222-2222-222222222222", Kind: "Notebook"}
	environment := Target{WorkspaceID: notebook.WorkspaceID, ItemID: "33333333-3333-3333-3333-333333333333", Kind: "Environment"}
	roots := []Root{{Name: "builtin", Target: notebook}, {Name: "env", Target: environment}}
	if err := ValidateRoots(notebook, roots); err != nil {
		t.Fatal(err)
	}
	if err := ValidatePath(Path{Target: roots[1].Target, Relative: "file.txt"}, true); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("alias bypassed Environment readonly policy", err)
	}
	if err := ValidatePath(Path{Target: notebook, Relative: "file.txt"}, true); err != nil {
		t.Fatal(err)
	}
	for _, root := range []Root{{Name: "builtin", Target: environment}, {Name: "env", Target: notebook}, {Name: "invented", Target: notebook}} {
		if err := ValidateRoots(notebook, []Root{root}); !errors.Is(err, fs.ErrInvalid) {
			t.Fatal("invalid root accepted", root, err)
		}
	}
	if err := ValidateRoots(notebook, []Root{roots[0], roots[0]}); !errors.Is(err, fs.ErrInvalid) {
		t.Fatal("duplicate root accepted", err)
	}
}

func TestResourcePathsCannotEscapeOrMutateRoot(t *testing.T) {
	target := Target{WorkspaceID: "11111111-1111-1111-1111-111111111111", ItemID: "22222222-2222-2222-2222-222222222222", Kind: "Notebook"}
	for _, path := range []string{"/a", "../a", "a/../b", "a//b", "a/./b", `a\b`, "a/\x00"} {
		if err := ValidatePath(Path{Target: target, Relative: path}, true); !errors.Is(err, fs.ErrInvalid) {
			t.Fatal("unsafe resource path", path, err)
		}
	}
	if err := ValidatePath(Path{Target: target}, true); !errors.Is(err, fserrors.ErrReadOnly) {
		t.Fatal("resource root mutation allowed", err)
	}
	if err := ValidatePath(Path{Target: target, Relative: "literal%2e%2e"}, true); err != nil {
		t.Fatal("literal percent name incorrectly reinterpreted", err)
	}
}
