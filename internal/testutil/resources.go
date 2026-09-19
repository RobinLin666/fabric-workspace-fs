package testutil

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"sort"
	"strings"
	"sync"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/resources"
)

type ResourceStore struct {
	mu            sync.Mutex
	files         map[resources.Path][]byte
	dirs          map[resources.Path]bool
	reads, writes int
	DenyNotebook  bool
}

func NewResources() *ResourceStore {
	return &ResourceStore{files: make(map[resources.Path][]byte), dirs: make(map[resources.Path]bool)}
}
func (s *ResourceStore) WriteGuarantee() resources.WriteGuarantee { return resources.CompareThenWrite }
func (s *ResourceStore) Roots(_ context.Context, target resources.Target) ([]resources.Root, error) {
	if err := resources.ValidateTarget(target); err != nil {
		return nil, err
	}
	name := "builtin"
	if target.Kind == "Environment" {
		name = "resources"
	}
	return []resources.Root{{Name: name, Target: target}}, nil
}
func (s *ResourceStore) Seed(path resources.Path, data []byte) {
	s.mu.Lock()
	s.files[path] = append([]byte(nil), data...)
	s.mu.Unlock()
}
func (s *ResourceStore) Counts() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads, s.writes
}
func (s *ResourceStore) info(path resources.Path) (resources.Info, error) {
	if path.Relative == "" || s.dirs[path] {
		return resources.Info{Path: path.Relative, IsDir: true, Version: "directory"}, nil
	}
	data, ok := s.files[path]
	if !ok {
		return resources.Info{}, fs.ErrNotExist
	}
	sum := sha256.Sum256(data)
	return resources.Info{Path: path.Relative, Size: int64(len(data)), Version: hex.EncodeToString(sum[:])}, nil
}
func (s *ResourceStore) Stat(ctx context.Context, path resources.Path) (resources.Info, error) {
	if err := resources.ValidatePath(path, false); err != nil {
		return resources.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return resources.Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.DenyNotebook && path.Target.Kind == "Notebook" {
		return resources.Info{}, fs.ErrPermission
	}
	return s.info(path)
}
func (s *ResourceStore) List(ctx context.Context, path resources.Path) ([]resources.Info, error) {
	if err := resources.ValidatePath(path, false); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.DenyNotebook && path.Target.Kind == "Notebook" {
		return nil, fs.ErrPermission
	}
	parent, err := s.info(path)
	if err != nil {
		return nil, err
	}
	if !parent.IsDir {
		return nil, fserrors.ErrNotDir
	}
	prefix := path.Relative
	if prefix != "" {
		prefix += "/"
	}
	var out []resources.Info
	add := func(child resources.Path) {
		if child.Target != path.Target || !strings.HasPrefix(child.Relative, prefix) {
			return
		}
		leaf := strings.TrimPrefix(child.Relative, prefix)
		if leaf == "" || strings.Contains(leaf, "/") {
			return
		}
		info, err := s.info(child)
		if err == nil {
			out = append(out, info)
		}
	}
	for p := range s.files {
		add(p)
	}
	for p := range s.dirs {
		add(p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}
func (s *ResourceStore) Read(ctx context.Context, path resources.Path, offset int64, dest []byte, version string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.DenyNotebook && path.Target.Kind == "Notebook" {
		return 0, fs.ErrPermission
	}
	info, err := s.info(path)
	if err != nil {
		return 0, err
	}
	if info.IsDir {
		return 0, fserrors.ErrIsDir
	}
	if version != "" && version != info.Version {
		return 0, fserrors.ErrConflict
	}
	if offset < 0 {
		return 0, fs.ErrInvalid
	}
	data := s.files[path]
	if offset >= int64(len(data)) {
		return 0, io.EOF
	}
	n := copy(dest, data[offset:])
	if n < len(dest) {
		return n, io.EOF
	}
	return n, nil
}
func (s *ResourceStore) Snapshot(ctx context.Context, path resources.Path, version string) (*resources.Snapshot, error) {
	if err := resources.ValidatePath(path, false); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.DenyNotebook && path.Target.Kind == "Notebook" {
		return nil, fs.ErrPermission
	}
	info, err := s.info(path)
	if err != nil {
		return nil, err
	}
	if info.IsDir {
		return nil, fserrors.ErrIsDir
	}
	if version != "" && info.Version != version {
		return nil, fserrors.ErrConflict
	}
	return resources.NewSnapshot(s.files[path], info.Version), nil
}

func (s *ResourceStore) Put(ctx context.Context, path resources.Path, source io.ReaderAt, size int64, version string) (resources.Info, error) {
	if err := resources.ValidatePath(path, true); err != nil {
		return resources.Info{}, err
	}
	if err := ctx.Err(); err != nil {
		return resources.Info{}, err
	}
	if size < 0 || size > 1<<20 {
		return resources.Info{}, fserrors.ErrTooLarge
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := s.info(path)
	if err == nil && (version == "" || version != info.Version) {
		return resources.Info{}, fserrors.ErrConflict
	}
	if err != nil && !errorsIsNotExist(err) {
		return resources.Info{}, err
	}
	if err != nil && version != "" {
		return resources.Info{}, fserrors.ErrConflict
	}
	data := make([]byte, size)
	if size > 0 {
		if _, err := source.ReadAt(data, 0); err != nil {
			return resources.Info{}, err
		}
	}
	s.files[path] = data
	s.writes++
	return s.info(path)
}
func errorsIsNotExist(err error) bool { return err == fs.ErrNotExist }
func (s *ResourceStore) Mkdir(ctx context.Context, path resources.Path) error {
	if err := resources.ValidatePath(path, true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.info(path); err == nil {
		return fs.ErrExist
	}
	s.dirs[path] = true
	s.writes++
	return nil
}
func (s *ResourceStore) Remove(ctx context.Context, path resources.Path, directory bool, version string) error {
	if err := resources.ValidatePath(path, true); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := s.info(path)
	if err != nil {
		return err
	}
	if info.IsDir != directory {
		return fs.ErrInvalid
	}
	if version != "" && version != info.Version {
		return fserrors.ErrConflict
	}
	if directory {
		for child := range s.files {
			if child.Target == path.Target && strings.HasPrefix(child.Relative, path.Relative+"/") {
				return fserrors.ErrNotEmpty
			}
		}
		for child := range s.dirs {
			if child.Target == path.Target && strings.HasPrefix(child.Relative, path.Relative+"/") {
				return fserrors.ErrNotEmpty
			}
		}
		delete(s.dirs, path)
	} else {
		delete(s.files, path)
	}
	s.writes++
	return nil
}
func (s *ResourceStore) Rename(ctx context.Context, source, target resources.Path, sourceVersion, targetVersion string, noReplace bool) (resources.Info, error) {
	if err := resources.ValidatePath(source, true); err != nil {
		return resources.Info{}, err
	}
	if err := resources.ValidatePath(target, true); err != nil {
		return resources.Info{}, err
	}
	if source.Target != target.Target {
		return resources.Info{}, fserrors.ErrCrossDevice
	}
	if err := ctx.Err(); err != nil {
		return resources.Info{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	info, err := s.info(source)
	if err != nil {
		return resources.Info{}, err
	}
	if info.IsDir {
		return resources.Info{}, fserrors.ErrUnsupported
	}
	if info.Version != sourceVersion {
		return resources.Info{}, fserrors.ErrConflict
	}
	dest, err := s.info(target)
	if err == nil {
		if noReplace {
			return resources.Info{}, fs.ErrExist
		}
		if dest.IsDir {
			return resources.Info{}, fserrors.ErrIsDir
		}
		if dest.Version != targetVersion {
			return resources.Info{}, fserrors.ErrConflict
		}
	} else if !errorsIsNotExist(err) {
		return resources.Info{}, err
	}
	s.files[target] = s.files[source]
	delete(s.files, source)
	s.writes++
	return s.info(target)
}
