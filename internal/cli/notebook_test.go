package cli

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestPrewarmArgumentsFailBeforeAuthentication(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "must-not-be-used")
	for _, args := range [][]string{
		{"mount", "--all-workspaces", "--prewarm-notebooks", "-1", "point"},
		{"mount", "--workspace", "invalid", "--prewarm-notebooks", "1", "point"},
		{"mount", "--all-workspaces", "--prewarm-notebooks", "33", "point"},
		{"mount", "--all-workspaces", "--prewarm-notebook", "unused", "point"},
		{"mount", "--all-workspaces", "--notebook-format", "html", "point"},
	} {
		var output bytes.Buffer
		if code := Run(context.Background(), args, &output, &output, "test"); code != 2 ||
			strings.Contains(output.String(), "Azure Identity") {
			t.Fatalf("%v = %d: %s", args, code, &output)
		}
	}
}

func TestMountHelpDescribesMountLevelPrewarm(t *testing.T) {
	var output bytes.Buffer
	if code := Run(context.Background(), []string{"mount", "--help"}, &output, &output, "test"); code != 0 {
		t.Fatal(code, output.String())
	}
	if !strings.Contains(output.String(), "prewarm-notebooks") {
		t.Fatal(output.String())
	}
}
