package workspacefs

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
	"fabric-workspace-fs/internal/onelake"
)

func (s *FS) newLakeChild(parent Entry, name string, directory bool) (Entry, error) {
	if err := s.mutableDirectory(parent); err != nil {
		return Entry{}, err
	}
	raw, err := namespace.ParseFileName(name)
	if err != nil {
		return Entry{}, err
	}
	e := lakeEntry(parent, name, parent.Remote+"/"+raw, onelake.Info{IsDir: directory})
	if err := onelake.ValidatePath(e.LakePath(), true); err != nil {
		return Entry{}, err
	}
	return e, nil
}

func (s *FS) Create(ctx context.Context, parent Entry, name string, flags int) (Entry, Handle, error) {
	if s.opts.ReadOnly {
		return Entry{}, nil, fserrors.ErrReadOnly
	}
	if name == identityFileName && identityContainer(parent) {
		return Entry{}, nil, fserrors.ErrReadOnly
	}
	if parent.Kind == OverlayDirectory {
		return Entry{}, nil, fserrors.ErrUnsupported
	}
	if parent.Kind == ResourceDirectory {
		return s.createResource(ctx, parent, name, flags)
	}
	if managedContainer(parent) {
		return Entry{}, nil, fmt.Errorf("create a typed item or Fabric folder with mkdir: %w", fserrors.ErrUnsupported)
	}
	e, err := s.newLakeChild(parent, name, false)
	if err != nil {
		return Entry{}, nil, err
	}
	if flags&os.O_TRUNC != 0 && !writing(flags) {
		return Entry{}, nil, fs.ErrPermission
	}
	return s.openLake(ctx, e, flags, true)
}

func (s *FS) Mkdir(ctx context.Context, parent Entry, name string) (Entry, error) {
	if parent.Kind == ResourceDirectory {
		return s.mkdirResource(ctx, parent, name)
	}
	if managedContainer(parent) {
		return s.mkdirManaged(ctx, parent, name)
	}
	if parent.Kind == OverlayDirectory {
		return Entry{}, fserrors.ErrUnsupported
	}
	e, err := s.newLakeChild(parent, name, true)
	if err != nil {
		return Entry{}, err
	}
	done, err := s.mutation(ctx, e.Key())
	if err != nil {
		return Entry{}, err
	}
	defer done()
	if err := s.lake.Mkdir(ctx, e.LakePath()); err != nil {
		return Entry{}, err
	}
	return s.Stat(ctx, e)
}

func (s *FS) Remove(ctx context.Context, e Entry, directory bool) error {
	if e.Kind == ResourceDirectory || e.Kind == ResourceFile {
		return s.removeResource(ctx, e, directory)
	}
	if e.Kind == OverlayDirectory || e.Kind == OverlayFile {
		return fserrors.ErrUnsupported
	}
	if managedItem(e) || e.Kind == FabricFolder {
		return s.removeManaged(ctx, e, directory)
	}
	if !s.Writable(e) || (e.Kind != LakeFile && e.Kind != LakeDirectory) {
		return fserrors.ErrReadOnly
	}
	if err := onelake.ValidatePath(e.LakePath(), true); err != nil {
		return err
	}
	if directory != e.Directory {
		if directory {
			return fserrors.ErrNotDir
		}
		return fserrors.ErrIsDir
	}
	done, err := s.mutation(ctx, e.Key())
	if err != nil {
		return err
	}
	defer done()
	info, err := s.lake.FreshStat(ctx, e.LakePath())
	if err != nil {
		return err
	}
	if info.IsDir != directory {
		return fserrors.ErrConflict
	}
	if info.ETag == "" {
		return fmt.Errorf("delete requires a remote ETag: %w", fserrors.ErrUnsupported)
	}
	return s.lake.Remove(ctx, e.LakePath(), directory, info.ETag)
}

func (s *FS) Rename(ctx context.Context, source, targetParent Entry, name string, noReplace bool) (Entry, error) {
	if s.opts.ReadOnly {
		return Entry{}, fserrors.ErrReadOnly
	}
	if managedItem(source) || source.Kind == FabricFolder {
		return Entry{}, fserrors.ErrUnsupported
	}
	if source.Kind == OverlayDirectory || source.Kind == OverlayFile {
		return Entry{}, fserrors.ErrUnsupported
	}
	if source.Kind == ResourceDirectory || source.Kind == ResourceFile {
		return s.renameResource(ctx, source, targetParent, name, noReplace)
	}
	if targetParent.Kind == ResourceDirectory {
		if !s.Writable(targetParent) {
			return Entry{}, fserrors.ErrReadOnly
		}
		return Entry{}, fserrors.ErrCrossDevice
	}
	if targetParent.Kind == OverlayDirectory {
		return Entry{}, fserrors.ErrCrossDevice
	}
	if !s.Writable(source) || (source.Kind != LakeFile && source.Kind != LakeDirectory) {
		return Entry{}, fserrors.ErrReadOnly
	}
	if err := onelake.ValidatePath(source.LakePath(), true); err != nil {
		return Entry{}, err
	}
	target, err := s.newLakeChild(targetParent, name, source.Directory)
	if err != nil {
		return Entry{}, err
	}
	if source.Workspace != target.Workspace || source.Item.ID != target.Item.ID {
		return Entry{}, fserrors.ErrCrossDevice
	}
	if source.Remote == target.Remote {
		if noReplace {
			return Entry{}, fs.ErrExist
		}
		return source, nil
	}
	if source.Directory && strings.HasPrefix(target.Remote, source.Remote+"/") {
		return Entry{}, fs.ErrInvalid
	}
	done, err := s.mutation(ctx, source.Key(), target.Key())
	if err != nil {
		return Entry{}, err
	}
	defer done()
	srcInfo, err := s.lake.FreshStat(ctx, source.LakePath())
	if err != nil {
		return Entry{}, err
	}
	if srcInfo.IsDir != source.Directory {
		return Entry{}, fserrors.ErrConflict
	}
	dstInfo, err := s.lake.FreshStat(ctx, target.LakePath())
	exists := err == nil
	if err != nil && !IsNotExist(err) {
		return Entry{}, err
	}
	if exists {
		if noReplace {
			return Entry{}, fs.ErrExist
		}
		if dstInfo.IsDir || srcInfo.IsDir {
			return Entry{}, fmt.Errorf("replacing directories is not supported; use a new destination: %w", fserrors.ErrUnsupported)
		}
		if dstInfo.ETag == "" {
			return Entry{}, fmt.Errorf("overwrite requires a destination ETag: %w", fserrors.ErrUnsupported)
		}
	}
	if srcInfo.ETag == "" {
		return Entry{}, fmt.Errorf("rename requires a source ETag: %w", fserrors.ErrUnsupported)
	}
	info, err := s.lake.Rename(ctx, source.LakePath(), target.LakePath(), srcInfo.ETag, dstInfo.ETag, noReplace)
	if err != nil {
		return Entry{}, err
	}
	return lakeEntry(target, name, target.Remote, info), nil
}

func (s *FS) Truncate(ctx context.Context, e Entry, size int64) error {
	if err := s.writableFile(e); err != nil {
		return err
	}
	if size < 0 {
		return fs.ErrInvalid
	}
	flags := os.O_WRONLY
	if size == 0 {
		flags |= os.O_TRUNC
	}
	handle, err := s.Open(ctx, e, flags)
	if err != nil {
		return err
	}
	err = handle.Truncate(ctx, size)
	if err == nil {
		err = handle.Flush(ctx)
	}
	return errors.Join(err, handle.Close())
}
