//go:build linux

package fusefs

import (
	"context"
	"fmt"
	"syscall"
	"testing"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/testutil"
	"fabric-workspace-fs/internal/workspacefs"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

func TestAdapterUsesResolvedTimeoutsAndSourceDeadline(t *testing.T) {
	now := time.Unix(2_000_000_000, 0)
	for _, test := range []struct {
		name       string
		config     string
		validUntil time.Time
		attr       time.Duration
		entry      time.Duration
		negative   time.Duration
	}{
		{name: "defaults", config: `{}`, attr: 2 * time.Minute, entry: 2 * time.Minute, negative: 2 * time.Minute},
		{name: "type", config: `{"types":{"Notebook":{"kernelAttr":"7s","kernelEntry":"9s","kernelNegative":"11s"}}}`, attr: 7 * time.Second, entry: 9 * time.Second, negative: 11 * time.Second},
		{name: "surface", config: `{"types":{"Notebook":{"kernelAttr":"7s","surfaces":{"content":{"kernelAttr":"3s","kernelEntry":"4s","kernelNegative":"5s"}}}}}`, attr: 3 * time.Second, entry: 4 * time.Second, negative: 5 * time.Second},
		{name: "source deadline", config: `{}`, validUntil: now.Add(5 * time.Second), attr: 5 * time.Second, entry: 5 * time.Second, negative: 5 * time.Second},
		{name: "expired source", config: `{}`, validUntil: now.Add(-time.Second)},
		{name: "disabled", config: `{"defaults":{"kernelAttr":"0s","kernelEntry":"0s","kernelNegative":"0s"}}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy, err := cachepolicy.Parse([]byte(test.config))
			if err != nil {
				t.Fatal(err)
			}
			service := testutil.New(t)
			fab, lake := service.Clients()
			opts := workspacefs.DefaultOptions()
			opts.WorkspaceIDs = []string{testutil.WorkspaceID}
			opts.SpoolDirectory = t.TempDir()
			opts.CachePolicy, opts.Now = policy, func() time.Time { return now }
			backend, err := workspacefs.New(fab, lake, opts)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := backend.Close(); err != nil {
					t.Error(err)
				}
			})
			e := workspacefs.Entry{
				Kind: workspacefs.NotebookContent, Workspace: testutil.WorkspaceID, Size: 123,
				Item: fabric.Item{ID: testutil.NotebookID, Type: "Notebook"}, ValidUntil: test.validUntil,
			}
			n := &node{adapter: &adapter{backend: backend}, entry: e}
			var attr fuse.AttrOut
			var entry, negative fuse.EntryOut
			n.attrOut(e, &attr)
			n.entryOut(e, &entry)
			n.negativeOut(e, nil, &negative)
			if attr.Timeout() != test.attr || entry.AttrTimeout() != test.attr ||
				entry.EntryTimeout() != test.entry || negative.EntryTimeout() != test.negative ||
				attr.Size != 123 || entry.Size != 123 || negative.NodeId != 0 {
				t.Fatalf("wrong wire timeouts: attr=%s entry=(%s,%s) negative=%s size=%d/%d",
					attr.Timeout(), entry.AttrTimeout(), entry.EntryTimeout(), negative.EntryTimeout(), attr.Size, entry.Size)
			}
		})
	}
}

type cacheDeadlineError struct {
	deadline time.Time
	cause    error
}

func (e cacheDeadlineError) Error() string            { return e.cause.Error() }
func (e cacheDeadlineError) Unwrap() error            { return e.cause }
func (e cacheDeadlineError) CacheDeadline() time.Time { return e.deadline }

func TestNegativeTimeoutDoesNotRenewOriginalMissProof(t *testing.T) {
	observed := time.Unix(2_000_000_000, 0)
	now := observed
	service := testutil.New(t)
	fab, lake := service.Clients()
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs, opts.SpoolDirectory = []string{testutil.WorkspaceID}, t.TempDir()
	opts.Now = func() time.Time { return now }
	backend, err := workspacefs.New(fab, lake, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := backend.Close(); err != nil {
			t.Error(err)
		}
	})
	n := &node{adapter: &adapter{backend: backend}}
	for _, test := range []struct {
		name     string
		elapsed  time.Duration
		parent   time.Time
		proof    time.Time
		hasProof bool
		want     time.Duration
	}{
		{name: "parent only", parent: observed.Add(45 * time.Second), want: 45 * time.Second},
		{name: "earlier child proof", parent: observed.Add(time.Minute), proof: observed.Add(10 * time.Second), hasProof: true, want: 10 * time.Second},
		{name: "earlier parent proof", parent: observed.Add(5 * time.Second), proof: observed.Add(10 * time.Second), hasProof: true, want: 5 * time.Second},
		{name: "fixed parent", proof: observed.Add(10 * time.Second), hasProof: true, want: 10 * time.Second},
		{name: "original t119", elapsed: 119 * time.Second, parent: observed.Add(5 * time.Minute), proof: observed.Add(2 * time.Minute), hasProof: true, want: time.Second},
		{name: "expired t121", elapsed: 121 * time.Second, parent: observed.Add(5 * time.Minute), proof: observed.Add(2 * time.Minute), hasProof: true},
		{name: "unknown due now", elapsed: time.Second, parent: observed.Add(5 * time.Minute), proof: observed.Add(time.Second), hasProof: true},
		{name: "unknown zero deadline", parent: observed.Add(5 * time.Minute), hasProof: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			now = observed.Add(test.elapsed)
			parent := workspacefs.Entry{
				Kind: workspacefs.LakeDirectory, Directory: true, Workspace: testutil.WorkspaceID,
				Item: fabric.Item{ID: testutil.LakehouseID, Type: "Lakehouse"}, Remote: "Files", ValidUntil: test.parent,
			}
			var cause error = syscall.ENOENT
			if test.hasProof {
				cause = fmt.Errorf("wrapped source: %w", cacheDeadlineError{deadline: test.proof, cause: cause})
			}
			out := fuse.EntryOut{NodeId: 42}
			out.SetEntryTimeout(time.Hour)
			n.negativeOut(parent, cause, &out)
			if out.NodeId != 0 || out.EntryTimeout() != test.want || errno(cause) != syscall.ENOENT {
				t.Fatalf("negative source lifetime/error lost: node=%d ttl=%s want=%s errno=%v",
					out.NodeId, out.EntryTimeout(), test.want, errno(cause))
			}
		})
	}
}

type negativeLookupNode struct {
	fs.Inode
	timeout time.Duration
	err     syscall.Errno
}

func (n *negativeLookupNode) Lookup(_ context.Context, _ string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	out.SetEntryTimeout(n.timeout)
	return nil, n.err
}

func TestPerDirectoryNegativeTimeoutSurvivesGoFuseBridge(t *testing.T) {
	for _, test := range []struct {
		name    string
		timeout time.Duration
		err     syscall.Errno
		want    fuse.Status
	}{
		{"negative cached", 2 * time.Minute, syscall.ENOENT, fuse.OK},
		{"negative disabled", 0, syscall.ENOENT, fuse.OK},
		{"permission is not negative", 2 * time.Minute, syscall.EACCES, fuse.EACCES},
		{"backend error is not negative", 2 * time.Minute, syscall.EIO, fuse.EIO},
	} {
		t.Run(test.name, func(t *testing.T) {
			zero := time.Duration(0)
			root := &negativeLookupNode{timeout: test.timeout, err: test.err}
			raw := &cacheRawFS{RawFileSystem: fs.NewNodeFS(root, &fs.Options{
				AttrTimeout: &zero, EntryTimeout: &zero, NegativeTimeout: &zero,
			})}
			var out fuse.EntryOut
			if status := raw.Lookup(nil, &fuse.InHeader{NodeId: 1}, "missing", &out); status != test.want ||
				out.NodeId != 0 || out.EntryTimeout() != test.timeout {
				t.Fatalf("negative lookup status=%v node=%d timeout=%s", status, out.NodeId, out.EntryTimeout())
			}
		})
	}
}

func TestMountRequiresKernelNotificationSupport(t *testing.T) {
	if got := notificationSupport(&fuse.InitIn{Major: 7, Minor: 11}); got != syscall.ENOSYS {
		t.Fatalf("unsupported notification protocol accepted: %v", got)
	}
	if got := notificationSupport(&fuse.InitIn{Major: 7, Minor: 12}); got != 0 {
		t.Fatalf("supported notification protocol rejected: %v", got)
	}
}
