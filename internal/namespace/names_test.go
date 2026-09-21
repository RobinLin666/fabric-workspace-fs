package namespace

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestFileNamesRoundTrip(t *testing.T) {
	for _, raw := range []string{"data.csv", "space name.txt", "%2e%2e", "ending.", "ending ", "a:b?c", "\u6570\u636e.csv", "e\u0301", "\u00e9", "line\nbreak", "CON", "con.txt", "LPT9.log"} {
		t.Run(raw, func(t *testing.T) {
			local, err := FileName(raw)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := ParseFileName(local)
			if err != nil || decoded != raw {
				t.Fatalf("%q => %q => %q: %v", raw, local, decoded, err)
			}
		})
	}
}

func TestWindowsDeviceNamesAreEscapedOnEveryPlatform(t *testing.T) {
	for raw, want := range map[string]string{
		"CON": "%43ON", "con.txt": "%63on.txt", "PRN.report": "%50RN.report",
		"AUX": "%41UX", "NUL": "%4EUL", "COM1": "%43OM1", "LPT9.log": "%4CPT9.log",
	} {
		got, err := FileName(raw)
		if err != nil || got != want {
			t.Errorf("FileName(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	for _, ordinary := range []string{"CONSOLE", "COM0", "COM10", "LPT0", ".CON"} {
		got, err := FileName(ordinary)
		if err != nil || got != ordinary {
			t.Errorf("ordinary FileName(%q) = %q, %v", ordinary, got, err)
		}
	}
}

func TestRejectAliasesAndEscapes(t *testing.T) {
	for _, name := range []string{"", ".", "..", "a/b", `a\b`, "%2F", "%2E%2E", "%00", "%", "%61", "trailing.", "%c3%a9"} {
		if _, err := ParseFileName(name); err == nil {
			t.Errorf("accepted unsafe/noncanonical filename %q", name)
		}
	}
	for _, path := range []string{"", "/foo", "a//b", "a/../b", "a/./b", `a\b`, "a/\x00"} {
		if err := PartPath(path); err == nil {
			t.Errorf("accepted unsafe part %q", path)
		}
	}
}

func TestCatalogLabelsDoNotExposeIDsAndAreBounded(t *testing.T) {
	a := "11111111-1111-1111-1111-111111111111"
	b := "22222222-2222-2222-2222-222222222222"
	x, err := CatalogName("../../same/label", a)
	if err != nil {
		t.Fatal(err)
	}
	y, _ := CatalogName("../../same/label", b)
	if x != y || strings.ContainsAny(x, "/\\") || strings.Contains(x, a) {
		t.Fatalf("unsafe or ID-bearing labels: %q, %q", x, y)
	}
	if plain, _ := CatalogName("Readable display name", a); plain != "Readable display name" {
		t.Fatal("ordinary display name changed", plain)
	}
	for _, label := range []string{strings.Repeat("\u6570", 200), strings.Repeat("%", 200)} {
		first, _ := CatalogName(label, a)
		second, _ := CatalogName(label, a)
		if first != second || len(first) > 255 || !utf8.ValidString(first) {
			t.Fatalf("bad long label %q", first)
		}
	}
	if _, err := CatalogName("label", "../escape"); err == nil {
		t.Fatal("accepted non-UUID identity")
	}
	if NotebookContentFileName("Sample notebook") != "Sample notebook.ipynb" {
		t.Fatal("notebook local filename is not stable")
	}
	if got := NotebookContentFileName("a/b"); got != "a%2Fb.ipynb" {
		t.Fatalf("NotebookContentFileName escaped path = %q", got)
	}
	if got := NotebookContentFileName(strings.Repeat("x", 300)); len(got) > 255 || !strings.HasSuffix(got, ".ipynb") {
		t.Fatalf("NotebookContentFileName was not bounded: %q", got)
	}
}
