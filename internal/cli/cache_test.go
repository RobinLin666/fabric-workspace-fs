package cli

import (
	"bytes"
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fusefs"
	"fabric-workspace-fs/internal/workspacefs"
)

func writeCacheConfig(t *testing.T, data string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "cache.json")
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMountCachePolicyDefaultAndLegacyTTL(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		ttl  time.Duration
	}{
		{"default", nil, 2 * time.Minute},
		{"explicit default", []string{"--cache-ttl=2m"}, 2 * time.Minute},
		{"explicit zero", []string{"--cache-ttl=0s"}, 0},
		{"custom", []string{"--cache-ttl", "13s"}, 13 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags := flag.NewFlagSet("mount", flag.ContinueOnError)
			ttl := flags.Duration("cache-ttl", cachepolicy.DefaultTTL, "")
			path := flags.String("cache-config", "", "")
			if err := flags.Parse(test.args); err != nil {
				t.Fatal(err)
			}
			policy, err := mountCachePolicy(flags, *ttl, *path)
			if err != nil {
				t.Fatal(err)
			}
			want := cachepolicy.Values{
				Catalog: test.ttl, Attr: test.ttl, Directory: test.ttl,
				Definition: test.ttl, Content: test.ttl, KernelAttr: test.ttl,
				KernelEntry: test.ttl, KernelNegative: test.ttl,
			}
			if got := policy.Resolve(cachepolicy.Selector{Type: "Notebook", Surface: "builtin"}); got != want {
				t.Fatalf("policy = %+v, want %+v", got, want)
			}
		})
	}
}

func TestMountCacheConfigPreservesLegacyOption(t *testing.T) {
	path := writeCacheConfig(t, `{"defaults":{"attr":"0s","kernelEntry":"3s"},"types":{"Notebook":{"surfaces":{"builtin":{"content":"5s"}}}}}`)
	opts := workspacefs.DefaultOptions()
	legacy := opts.CacheTTL
	if legacy != cachepolicy.DefaultTTL {
		t.Fatalf("default legacy CacheTTL = %s, want %s", legacy, cachepolicy.DefaultTTL)
	}
	flags := flag.NewFlagSet("mount", flag.ContinueOnError)
	flags.DurationVar(&opts.CacheTTL, "cache-ttl", opts.CacheTTL, "")
	config := flags.String("cache-config", "", "")
	if err := flags.Parse([]string{"--cache-config", path}); err != nil {
		t.Fatal(err)
	}
	policy, err := mountCachePolicy(flags, opts.CacheTTL, *config)
	if err != nil {
		t.Fatal(err)
	}
	opts.CachePolicy = policy
	if opts.CacheTTL != legacy {
		t.Fatalf("config changed legacy CacheTTL from %s to %s", legacy, opts.CacheTTL)
	}
	want := cachepolicy.Default().Resolve(cachepolicy.Selector{})
	want.Attr, want.Content, want.KernelEntry = 0, 5*time.Second, 3*time.Second
	if got := opts.CachePolicy.Resolve(cachepolicy.Selector{Type: "Notebook", Surface: "builtin"}); got != want {
		t.Fatalf("config policy = %+v, want %+v", got, want)
	}
}

func TestMountCacheFlagsAreExplicitlyMutuallyExclusive(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	path := filepath.Join(t.TempDir(), "must-not-be-read.json")
	for _, flags := range [][]string{
		{"--cache-config", path, "--cache-ttl", "2m"},
		{"--cache-ttl=2m", "--cache-config=" + path},
		{"--cache-config=" + path, "--cache-ttl=0s"},
		{"--cache-ttl=13s", "--cache-config=" + path},
		{"--cache-config=", "--cache-ttl=2m"},
	} {
		args := append([]string{"mount", "--all-workspaces"}, flags...)
		args = append(args, "point")
		var output bytes.Buffer
		if code := Run(context.Background(), args, &output, &output, "test"); code != 2 ||
			!strings.Contains(output.String(), "--cache-ttl and --cache-config are mutually exclusive") {
			t.Fatalf("%v = %d, %s", args, code, &output)
		}
	}
}

func TestMountInvalidCacheConfigPrecedesPlatformAndCredentials(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	for _, test := range []struct {
		name string
		data string
	}{
		{"malformed", `{"defaults":`},
		{"unknown key", `{"defaults":{"unknown":"2m"}}`},
		{"unknown type", `{"types":{"Warehouse":{}}}`},
		{"negative", `{"defaults":{"attr":"-1s"}}`},
		{"numeric duration", `{"defaults":{"directory":0}}`},
		{"duplicate", `{"defaults":{},"defaults":{}}`},
		{"trailing", `{} {}`},
		{"catalog scope", `{"types":{"Notebook":{"catalog":"1s"}}}`},
		{"streamed content", `{"types":{"Lakehouse":{"surfaces":{"Files":{"content":"1s"}}}}}`},
		{"oversized", strings.Repeat(" ", cachepolicy.MaxConfigBytes+1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := writeCacheConfig(t, test.data)
			var output bytes.Buffer
			code := Run(context.Background(), []string{"mount", "--all-workspaces", "--cache-config", path, "point"}, &output, &output, "test")
			if code != 2 || !strings.Contains(output.String(), "--cache-config:") {
				t.Fatalf("invalid config = %d, %s", code, &output)
			}
			if strings.Contains(output.String(), "only on Linux") {
				t.Fatal("platform check preceded cache config validation")
			}
		})
	}
	for _, path := range []string{"", filepath.Join(t.TempDir(), "missing.json"), t.TempDir()} {
		var output bytes.Buffer
		code := Run(context.Background(), []string{"mount", "--all-workspaces", "--cache-config", path, "point"}, &output, &output, "test")
		if code != 2 || !strings.Contains(output.String(), "--cache-config") {
			t.Fatalf("bad path %q = %d, %s", path, code, &output)
		}
	}
}

func TestMountInvalidLegacyCacheTTLIsUsageError(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	for _, value := range []string{"-1ns", "invalid", "9999999999999999999h"} {
		var output bytes.Buffer
		code := Run(context.Background(), []string{"mount", "--all-workspaces", "--cache-ttl", value, "point"}, &output, &output, "test")
		if code != 2 || !strings.Contains(output.String(), "cache-ttl") {
			t.Fatalf("invalid TTL %q = %d, %s", value, code, &output)
		}
	}
}

func TestMountValidCacheOptionsReachPlatformBoundary(t *testing.T) {
	if fusefs.Supported() && fusefs.CheckPrerequisites() == nil {
		t.Skip("current platform has an available mount runtime")
	}
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	path := writeCacheConfig(t, `{"defaults":{"content":"0s"},"types":{"Lakehouse":{"surfaces":{"Files":{"attr":"0s"}}}}}`)
	for _, flags := range [][]string{
		{"--cache-config", path},
		{"--cache-ttl=0s"},
		{"--cache-ttl=2m"},
	} {
		args := append([]string{"mount", "--all-workspaces"}, flags...)
		args = append(args, "point")
		var output bytes.Buffer
		if code := Run(context.Background(), args, &output, &output, "test"); code != 1 ||
			(!strings.Contains(output.String(), "not supported") && !strings.Contains(output.String(), "without CGO") &&
				!strings.Contains(output.String(), "WinFsp")) {
			t.Fatalf("valid cache flags %v = %d, %s (backend defaults: %+v)", flags, code, &output, workspacefs.DefaultOptions())
		}
	}
}

func TestCacheConfigurationReferenceRemainsValid(t *testing.T) {
	t.Setenv("AZURE_TOKEN_CREDENTIALS", "invalid-credential-must-not-be-read")
	for _, text := range []string{
		"cache-config", "default to 2m", "explicitly supplied --cache-ttl",
		"missing/null durations inherit", "workspace/type/surface -> item -> item/surface",
		"catalog is allowed only in defaults", "buffered Notebook/Environment/MWC",
		"bounded streaming/ranges", "content overrides are rejected", "no live reload",
	} {
		if !strings.Contains(cacheConfigUsage, text) {
			t.Fatalf("cache configuration reference missing %q: %s", text, cacheConfigUsage)
		}
	}
	_, example, ok := strings.Cut(cacheConfigUsage, "Example: ")
	if !ok {
		t.Fatal("cache configuration example missing")
	}
	if _, err := cachepolicy.Parse([]byte(strings.TrimSpace(example))); err != nil {
		t.Fatalf("help's cache configuration example is invalid: %v", err)
	}
}
