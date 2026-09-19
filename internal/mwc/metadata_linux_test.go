//go:build linux

package mwc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fusefs"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"
)

type largeMetadataCatalog struct{}

func (largeMetadataCatalog) ListWorkspaces(context.Context) ([]fabric.Workspace, error) {
	return []fabric.Workspace{{ID: testWorkspace, DisplayName: "Workspace"}}, nil
}
func (largeMetadataCatalog) GetWorkspace(context.Context, string) (fabric.Workspace, error) {
	return fabric.Workspace{ID: testWorkspace, DisplayName: "Workspace"}, nil
}
func (largeMetadataCatalog) ListItems(context.Context, string) ([]fabric.Item, error) {
	return []fabric.Item{{ID: testItem, DisplayName: "Large resources", Type: "Notebook"}}, nil
}
func (largeMetadataCatalog) ListFolders(context.Context, string) ([]fabric.Folder, error) {
	return []fabric.Folder{}, nil
}
func (largeMetadataCatalog) GetDefinition(context.Context, string, string, string, string) (fabric.Definition, error) {
	return fabric.Definition{}, errors.New("resource metadata must not export a Notebook")
}
func (largeMetadataCatalog) UpdateNotebook(context.Context, string, string, fabric.Definition) error {
	return errors.New("unexpected Notebook update")
}

func TestMountedLargeResourceLSUsesMetadataNotContent(t *testing.T) {
	if os.Getenv("FABRICFS_FUSE_TEST") != "1" {
		t.Skip("set FABRICFS_FUSE_TEST=1 for kernel FUSE metadata regression")
	}
	for _, missingLength := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing-listing-length=%v", missingLength), func(t *testing.T) {
			w := newWire(t)
			var bodies []*observedBody
			var bodiesMu sync.Mutex
			w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
				if r.URL.Query().Get("recursive") == "false" {
					item := map[string]any{"name": "large.bin", "fileSystemEntryType": "file", "lastModified": testNow.Format(http.TimeFormat)}
					if !missingLength {
						item["contentLength"] = largeMetadataSize
					}
					return jsonResponse(200, map[string]any{"children": []any{item}}), nil, true
				}
				if strings.HasSuffix(r.URL.Path, "/large.bin") {
					body := &observedBody{reader: strings.NewReader("must never be downloaded")}
					bodiesMu.Lock()
					bodies = append(bodies, body)
					bodiesMu.Unlock()
					resp := response(200, nil)
					resp.ContentLength = largeMetadataSize
					resp.Body = body
					resp.Header.Set("Last-Modified", testNow.Format(http.TimeFormat))
					return resp, nil, true
				}
				return nil, nil, false
			}
			backend := w.client(func(o *Options) { o.MaxFileSize = 16 << 20; o.CacheTTL = 0 })
			runtime, err := os.MkdirTemp("/tmp", "fabric-workspace-fs-test-large-stat-")
			if err != nil {
				t.Fatal(err)
			}
			fmt.Fprintln(os.Stderr, "large resource metadata test runtime:", runtime)
			mountpoint := filepath.Join(runtime, "mount")
			if err := os.Mkdir(mountpoint, 0700); err != nil {
				t.Fatal(err)
			}
			public := testutil.New(t)
			_, lake := public.Clients()
			opts := workspacefs.DefaultOptions()
			opts.WorkspaceIDs = []string{testWorkspace}
			opts.SpoolDirectory = filepath.Join(runtime, "spool")
			opts.ResourceBackend = backend
			opts.CacheTTL = 0
			filesystem, err := workspacefs.New(largeMetadataCatalog{}, lake, opts)
			if err != nil {
				t.Fatal(err)
			}
			server, err := fusefs.Mount(mountpoint, filesystem, fusefs.Options{Logger: log.New(io.Discard, "", 0)})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := server.Unmount(); err != nil {
					t.Errorf("retaining %s after unmount failure: %v", runtime, err)
					return
				}
				server.Wait()
				if err := filesystem.Close(); err != nil {
					t.Error(err)
					return
				}
				file, err := os.Open("/proc/self/mountinfo")
				if err != nil {
					t.Error(err)
					return
				}
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					fields := strings.Fields(scanner.Text())
					if len(fields) < 6 || fields[4] == runtime || strings.HasPrefix(fields[4], runtime+"/") {
						_ = file.Close()
						t.Errorf("retaining possibly mounted %s", runtime)
						return
					}
				}
				_ = file.Close()
				if err := scanner.Err(); err != nil {
					t.Error(err)
					return
				}
				if err := os.RemoveAll(runtime); err != nil {
					t.Error(err)
				}
			})
			if err := server.WaitMount(); err != nil {
				t.Fatal(err)
			}
			root := filepath.Join(mountpoint, "Workspace", "Large resources.Notebook", "builtin")
			for _, args := range [][]string{{"ls", root}, {"ls", "-la", root}, {"find", root, "-maxdepth", "1", "-ls"}} {
				if output, err := exec.Command(args[0], args[1:]...).CombinedOutput(); err != nil {
					t.Fatalf("%v: %s: %v", args, output, err)
				}
			}
			path := filepath.Join(root, "large.bin")
			info, err := os.Stat(path)
			if err != nil || !info.Mode().IsRegular() || info.Size() != largeMetadataSize || !info.ModTime().Equal(testNow) {
				t.Fatal("large file stat is inaccurate", info, err)
			}
			handle, err := os.Open(path)
			if handle != nil {
				_ = handle.Close()
			}
			if !errors.Is(err, syscall.EFBIG) {
				t.Fatal("open content limit was removed", err)
			}
			bodiesMu.Lock()
			for _, body := range bodies {
				if body.read.Load() != 0 || !body.closed.Load() {
					bodiesMu.Unlock()
					t.Fatal("ls/stat consumed large content", body.read.Load())
				}
			}
			bodiesMu.Unlock()
			gets := w.count(http.MethodGet, "/workdir/large.bin")
			if !missingLength && gets != 0 {
				t.Fatal("complete listing metadata still issued file GETs", gets)
			}
			if missingLength && gets == 0 {
				t.Fatal("missing length was not resolved from real GET headers")
			}
			t.Logf("large stat bytes buffered=0; header-only GETs=%d; max-readable=%d; actual-size=%d", gets, 16<<20, largeMetadataSize)
		})
	}
}
