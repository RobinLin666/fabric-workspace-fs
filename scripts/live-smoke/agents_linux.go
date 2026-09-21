//go:build linux

package main

import (
	"bytes"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/workspacefs"
)

type bundleNode struct {
	location
	children []workspacefs.Entry
	data     []byte
}

func (r *runner) checkReadTraffic(allowReads bool, call func() (int, error)) (size int, err error) {
	before := r.counter.snapshot()
	_, deniedBefore := r.guard.state()
	defer func() {
		_, deniedAfter := r.guard.state()
		delta := countDelta(before, r.counter.snapshot())
		if deniedAfter != deniedBefore || mutationCount(delta) != 0 || !allowReads && totalCounts(delta) != 0 {
			err = errors.Join(err, fail("local readonly check issued unexpected HTTP traffic or reached the outbound scope guard"))
		}
	}()
	return call()
}

func (r *runner) readBundleFile(entry workspacefs.Entry) (data []byte, err error) {
	handle, err := r.backend.Open(r.ctx, entry, os.O_RDONLY)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, handle.Close()) }()
	if handle.Size() <= 0 || handle.Size() > maxNotebookBytes || handle.Size() != entry.Size {
		return nil, fail("injected bundle file has an invalid bounded size")
	}
	data = make([]byte, int(handle.Size()))
	n, err := handle.ReadAt(r.ctx, data, 0)
	if n != len(data) || err != nil && !errors.Is(err, io.EOF) {
		return nil, fail("injected bundle file did not read its exact declared size")
	}
	if _, err := handle.WriteAt(r.ctx, []byte("readonly-probe"), 0); !errors.Is(err, fserrors.ErrReadOnly) {
		return nil, fail("injected bundle read handle accepted a write")
	}
	if err := handle.Truncate(r.ctx, 0); !errors.Is(err, fserrors.ErrReadOnly) {
		return nil, fail("injected bundle read handle accepted truncation")
	}
	current := make([]byte, len(data))
	n, err = handle.ReadAt(r.ctx, current, 0)
	if n != len(current) || err != nil && !errors.Is(err, io.EOF) || !bytes.Equal(current, data) {
		return nil, fail("injected bundle changed during a denied handle write")
	}
	return data, nil
}

func (r *runner) bundleWriteDenials(entry workspacefs.Entry) error {
	probe := r.doc.Plan.Stem + "-readonly"
	if entry.Directory {
		_, handle, err := r.backend.Create(r.ctx, entry, probe, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
		if handle != nil {
			return errors.Join(fail("injected bundle creation returned a file handle"), err, handle.Close())
		}
		if !errors.Is(err, fserrors.ErrReadOnly) {
			return fail("injected bundle directory accepted file creation")
		}
		if _, err := r.backend.Mkdir(r.ctx, entry, probe); !errors.Is(err, fserrors.ErrReadOnly) {
			return fail("injected bundle directory accepted mkdir")
		}
		if _, err := r.backend.Lookup(r.ctx, entry, probe); !errors.Is(err, fs.ErrNotExist) {
			return fail("rejected injected-bundle creation left an entry behind")
		}
		return nil
	}
	for _, flags := range []int{os.O_WRONLY, os.O_RDWR, os.O_WRONLY | os.O_TRUNC} {
		handle, err := r.backend.Open(r.ctx, entry, flags)
		if handle != nil {
			return errors.Join(fail("injected bundle writable open returned a file handle"), err, handle.Close())
		}
		if !errors.Is(err, fserrors.ErrReadOnly) {
			return fail("injected bundle file accepted a writable open")
		}
	}
	if err := r.backend.Truncate(r.ctx, entry, 0); !errors.Is(err, fserrors.ErrReadOnly) {
		return fail("injected bundle file accepted path truncation")
	}
	return nil
}

func validBundleEntry(entry workspacefs.Entry) bool {
	if entry.Workspace != "" || entry.Item.ID != "" || entry.Folder.ID != "" || entry.Remote != "" {
		return false
	}
	return entry.Kind == workspacefs.AgentFile && !entry.Directory ||
		entry.Kind == workspacefs.AgentDirectory && entry.Directory && entry.Fixed
}

// Start with exact root Lookup, not root ReadDir: the latter legitimately
// enumerates remote workspaces. Never resolve or touch any retired overlay.
func (r *runner) directInjectedBundle() ([]bundleNode, int, error) {
	rootEntry := r.backend.Root()
	guide, err := r.backend.Lookup(r.ctx, rootEntry, "AGENTS.md")
	if err != nil {
		return nil, 0, err
	}
	if !validBundleEntry(guide) || guide.Name != "AGENTS.md" || guide.Part != "AGENTS.md" {
		return nil, 0, fail("mount root did not expose the root AGENTS.md guide")
	}
	guideLocation, err := childLocation(r.paths.root, guide)
	if err != nil {
		return nil, 0, err
	}
	root, err := r.backend.Lookup(r.ctx, rootEntry, ".agents")
	if err != nil {
		return nil, 0, err
	}
	if !validBundleEntry(root) || !root.Directory || root.Name != ".agents" || root.Part != "" {
		return nil, 0, fail("mount root did not expose the typed injected bundle")
	}
	local, err := childLocation(r.paths.root, root)
	if err != nil {
		return nil, 0, err
	}
	nodes := []bundleNode{{location: guideLocation}, {location: local}}
	seen := map[string]bool{"": true}
	total, skill := 0, false
	for i := 0; i < len(nodes); i++ {
		node := nodes[i]
		if r.backend.Writable(node.entry) {
			return nil, 0, fail("injected bundle descriptor is writable")
		}
		stat, err := r.backend.Stat(r.ctx, node.entry)
		if err != nil {
			return nil, 0, err
		}
		if !validBundleEntry(stat) || stat.Kind != node.entry.Kind || stat.Part != node.entry.Part ||
			stat.Name != node.entry.Name || stat.Size != node.entry.Size {
			return nil, 0, fail("injected bundle stat changed its descriptor")
		}
		if err := r.bundleWriteDenials(node.entry); err != nil {
			return nil, 0, err
		}
		if !node.entry.Directory {
			data, err := r.readBundleFile(stat)
			if err != nil {
				return nil, 0, err
			}
			total += len(data)
			if total > maxNotebookBytes {
				return nil, 0, fail("injected bundle exceeded the smoke byte bound")
			}
			nodes[i].data = data
			skill = skill || strings.HasPrefix(stat.Part, "skills/") && stat.Name == "SKILL.md"
			continue
		}
		children, err := r.backend.ReadDir(r.ctx, node.entry)
		if err != nil {
			return nil, 0, err
		}
		nodes[i].children = children
		for _, child := range children {
			if !validComponent(child.Name) || !validBundleEntry(child) ||
				child.Part != path.Join(node.entry.Part, child.Name) || seen[child.Part] ||
				strings.Count(child.Part, "/") > 8 || len(nodes) >= 128 {
				return nil, 0, fail("injected bundle walk escaped its typed bounded namespace")
			}
			resolved, err := r.backend.Lookup(r.ctx, node.entry, child.Name)
			if err != nil {
				return nil, 0, err
			}
			if !validBundleEntry(resolved) || resolved.Kind != child.Kind ||
				resolved.Name != child.Name || resolved.Part != child.Part || resolved.Size != child.Size {
				return nil, 0, fail("injected bundle lookup changed its descriptor")
			}
			childPath, err := childLocation(node.location, resolved)
			if err != nil {
				return nil, 0, err
			}
			seen[child.Part] = true
			nodes = append(nodes, bundleNode{location: childPath})
		}
	}
	if !skill {
		return nil, 0, fail("injected bundle omitted fabric-notebook-workflow")
	}
	return nodes, total, nil
}

func (r *runner) mountedInjectedBundle(nodes []bundleNode) (int, error) {
	total := 0
	for _, node := range nodes {
		if node.entry.Directory {
			children, err := os.ReadDir(node.path)
			if err != nil {
				return 0, err
			}
			if len(children) != len(node.children) {
				return 0, fail("mounted injected bundle has unexpected entries")
			}
			for i, child := range children {
				if child.Name() != node.children[i].Name || child.IsDir() != node.children[i].Directory {
					return 0, fail("mounted injected bundle differs from its typed descriptors")
				}
			}
			probe := filepath.Join(node.path, r.doc.Plan.Stem+"-readonly")
			if err := r.readonlyCall("agents.create."+node.entry.Part, false, true, func() error {
				return r.rejectOpen(probe, os.O_WRONLY|os.O_CREATE|os.O_EXCL)
			}); err != nil {
				return 0, err
			}
			if err := r.readonlyCall("agents.mkdir."+node.entry.Part, false, true, func() error {
				return os.Mkdir(probe, 0700)
			}); err != nil {
				return 0, err
			}
			if _, err := os.Lstat(probe); !errors.Is(err, fs.ErrNotExist) {
				return 0, fail("rejected mounted bundle creation left an entry behind")
			}
			continue
		}
		if _, err := r.expectBytes(node.path, node.data); err != nil {
			return 0, err
		}
		if err := r.readonlyCall("agents.open-write."+node.entry.Part, false, true, func() error {
			return r.rejectOpen(node.path, os.O_WRONLY|os.O_TRUNC)
		}); err != nil {
			return 0, err
		}
		if err := r.readonlyCall("agents.truncate."+node.entry.Part, false, true, func() error {
			return os.Truncate(node.path, 0)
		}); err != nil {
			return 0, err
		}
		if _, err := r.expectBytes(node.path, node.data); err != nil {
			return 0, err
		}
		total += len(node.data)
	}
	return total, nil
}

func (r *runner) injectedBundleChecks() error {
	r.guard.setReadOnly(true)
	defer r.guard.setReadOnly(false)
	var nodes []bundleNode
	if err := r.measure("agents.direct-typed-readonly-bundle", func() (int, error) {
		return r.checkReadTraffic(false, func() (int, error) {
			var size int
			var err error
			nodes, size, err = r.directInjectedBundle()
			return size, err
		})
	}); err != nil {
		return err
	}
	// Mounted path traversal may refresh root workspace metadata. It must
	// still reject every write locally; the bundle is never cleanup-owned.
	return r.measure("agents.mounted-readonly-bundle", func() (int, error) {
		return r.checkReadTraffic(true, func() (int, error) { return r.mountedInjectedBundle(nodes) })
	})
}
