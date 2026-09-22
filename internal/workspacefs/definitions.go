package workspacefs

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/namespace"
)

func (s *FS) definition(ctx context.Context, e Entry) (fabric.Definition, error) {
	return s.readDefinition(ctx, e, false)
}

func (s *FS) lookupNotebookPart(ctx context.Context, parent Entry, name string) (Entry, error) {
	if name != s.notebookContentFileName(parent.Item.DisplayName) {
		return Entry{}, fs.ErrNotExist
	}
	entry := Entry{Name: name, Kind: NotebookContent, Workspace: parent.Workspace, Item: parent.Item}
	return s.statDefinition(ctx, entry)
}

func (s *FS) notebookContentFileName(displayName string) string {
	if s.opts.NotebookFormat == "py" {
		return namespace.NotebookContentFileNameWithExtension(displayName, ".py")
	}
	return namespace.NotebookContentFileName(displayName)
}

func (s *FS) readDefinition(ctx context.Context, e Entry, fresh bool) (fabric.Definition, error) {
	snapshot, err := s.sourceSnapshot(ctx, e, s.policy(e).Definition, fresh)
	if err != nil {
		return fabric.Definition{}, err
	}
	return cloneDefinition(snapshot.definition), nil
}

func notebookPart(def fabric.Definition) (fabric.Part, error) {
	var result fabric.Part
	count := 0
	for _, part := range def.Parts {
		if strings.HasSuffix(strings.ToLower(part.Path), ".ipynb") {
			result = part
			count++
		}
	}
	if count != 1 {
		return fabric.Part{}, fmt.Errorf("expected exactly one ipynb definition part, got %d: %w", count, fs.ErrInvalid)
	}
	return result, nil
}

func findPart(def fabric.Definition, path string) (fabric.Part, error) {
	for _, part := range def.Parts {
		if part.Path == path {
			return part, nil
		}
	}
	return fabric.Part{}, fs.ErrNotExist
}

func partData(part fabric.Part, limit int64) ([]byte, error) {
	if int64(len(part.Payload)) > (limit/3+1)*4+4 {
		return nil, fserrors.ErrTooLarge
	}
	data, err := part.Decode()
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fserrors.ErrTooLarge
	}
	return data, nil
}

func (s *FS) definitionChildren(parent Entry, snapshot *definitionSnapshot) ([]Entry, error) {
	prefix := parent.Part
	if prefix != "" {
		prefix += "/"
	}
	children := make(map[string]Entry)
	for _, part := range snapshot.definition.Parts {
		if hiddenPlatform(part.Path) || !strings.HasPrefix(part.Path, prefix) {
			continue
		}
		relative := strings.TrimPrefix(part.Path, prefix)
		raw, rest, directory := strings.Cut(relative, "/")
		name, err := namespace.FileName(raw)
		if err != nil {
			return nil, err
		}
		if parent.Kind == Environment && name == identityFileName {
			name = "%2E" + strings.TrimPrefix(name, ".")
		}
		if parent.Kind == Environment && name == "resources" {
			name = "%72esources"
		}
		entry := Entry{Name: name, Kind: DefinitionFile, Workspace: parent.Workspace, Item: parent.Item, Part: prefix + raw, Modified: snapshot.observedAt}
		policy := s.policy(entry)
		entry.ValidUntil = snapshot.observedAt.Add(min(policy.Attr, policy.Definition, policy.Directory))
		if directory && rest != "" {
			entry.Kind, entry.Directory = DefinitionDirectory, true
		} else {
			data, exists := snapshot.parts[part.Path]
			if !exists {
				return nil, fmt.Errorf("definition part missing from decoded snapshot: %w", fs.ErrInvalid)
			}
			entry.Size = int64(len(data))
		}
		children[name] = entry
	}
	if len(children) == 0 && parent.Kind == DefinitionDirectory && !parent.Fixed {
		return nil, fs.ErrNotExist
	}
	out := make([]Entry, 0, len(children))
	for _, child := range children {
		out = append(out, child)
	}
	return out, nil
}

func (s *FS) statDefinition(ctx context.Context, e Entry) (Entry, error) {
	if hiddenPlatform(e.Part) {
		return Entry{}, fs.ErrNotExist
	}
	policy := s.policy(e)
	snapshot, err := s.sourceSnapshot(ctx, e, policy.Attr, false)
	if err != nil {
		return Entry{}, err
	}
	if e.Kind == NotebookContent {
		e.Part = snapshot.notebook
	}
	data, exists := snapshot.parts[e.Part]
	if !exists {
		return Entry{}, fs.ErrNotExist
	}
	if e.Kind == NotebookContent && s.opts.NotebookFormat == "py" {
		data, err = notebookToPython(data, pythonIdentity(e))
		if err != nil {
			return Entry{}, err
		}
	}
	e.Size, e.Modified = int64(len(data)), snapshot.observedAt
	e.ValidUntil = snapshot.observedAt.Add(min(policy.Attr, policy.Definition))
	return e, nil
}

func pythonIdentity(e Entry) pythonNotebookIdentity {
	return pythonNotebookIdentity{
		ID:             e.Item.ID,
		WorkspaceID:    e.Workspace,
		DisplayName:    e.Item.DisplayName,
		RemotePartPath: e.Part,
	}
}

func validNotebook(data []byte) error {
	var notebook struct {
		Format int             `json:"nbformat"`
		Cells  json.RawMessage `json:"cells"`
	}
	if err := json.Unmarshal(data, &notebook); err != nil {
		return fmt.Errorf("notebook must be valid ipynb JSON: %w", errors.Join(fs.ErrInvalid, err))
	}
	if notebook.Format != 4 {
		return fmt.Errorf("notebook must have nbformat 4: %w", fs.ErrInvalid)
	}
	raw := bytes.TrimSpace(notebook.Cells)
	// Fabric's export omits cells for a newly created empty Notebook. Preserve
	// that real wire shape instead of rejecting an untouched exported file.
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil
	}
	var cells []json.RawMessage
	if raw[0] != '[' || json.Unmarshal(raw, &cells) != nil {
		return fmt.Errorf("notebook cells must be an array when present: %w", fs.ErrInvalid)
	}
	return nil
}

func prepareNotebookForFabric(data []byte) ([]byte, error) {
	var notebook map[string]any
	if err := json.Unmarshal(data, &notebook); err != nil {
		return nil, err
	}
	cells, ok := notebook["cells"].([]any)
	if !ok {
		return data, nil
	}
	referenced := make(map[string]bool)
	states := make(map[string]any)
	touched := false
	widgetTouched := false
	for _, rawCell := range cells {
		cell, ok := rawCell.(map[string]any)
		if !ok {
			continue
		}
		outputs, ok := cell["outputs"].([]any)
		if !ok {
			continue
		}
		for _, rawOutput := range outputs {
			output, ok := rawOutput.(map[string]any)
			if !ok {
				continue
			}
			outputData, ok := output["data"].(map[string]any)
			if !ok {
				continue
			}
			outputMetadata, ok := output["metadata"].(map[string]any)
			if !ok {
				continue
			}
			marker, ok := outputMetadata["fabric_jupyter"].(map[string]any)
			if !ok {
				continue
			}
			touched = true
			if generated, ok := marker["generated_mime_types"].([]any); ok {
				for _, rawMIME := range generated {
					if mime, ok := rawMIME.(string); ok {
						delete(outputData, mime)
					}
				}
			}
			if restoreData, ok := marker["restore_data"].(map[string]any); ok {
				for mime, value := range restoreData {
					outputData[mime] = value
				}
			}
			delete(outputMetadata, "fabric_jupyter")
			widget, ok := outputData["application/vnd.synapse.widget-view+json"].(map[string]any)
			if !ok {
				continue
			}
			widgetID, ok := widget["widget_id"].(string)
			if !ok || widgetID == "" {
				continue
			}
			widgetTouched = true
			referenced[widgetID] = true
			if state, ok := marker["widget_state"].(map[string]any); ok {
				states[widgetID] = state
			}
		}
	}
	if !touched {
		return data, nil
	}
	if !widgetTouched {
		normalized, err := json.Marshal(notebook)
		if err != nil {
			return nil, err
		}
		return normalized, nil
	}
	metadata, ok := notebook["metadata"].(map[string]any)
	if !ok {
		metadata = make(map[string]any)
		notebook["metadata"] = metadata
	}
	synapse, ok := metadata["synapse_widget"].(map[string]any)
	if !ok {
		synapse = make(map[string]any)
	}
	existingStates, ok := synapse["state"].(map[string]any)
	if !ok {
		existingStates = make(map[string]any)
	}
	for id, state := range states {
		if existing, ok := existingStates[id].(map[string]any); ok {
			if incoming, ok := state.(map[string]any); ok {
				state = mergeNotebookMetadata(existing, incoming)
			}
		}
		existingStates[id] = state
	}
	for id := range existingStates {
		if !referenced[id] {
			delete(existingStates, id)
		}
	}
	synapse["version"] = "0.1"
	synapse["state"] = existingStates
	metadata["synapse_widget"] = synapse
	normalized, err := json.Marshal(notebook)
	if err != nil {
		return nil, err
	}
	return normalized, nil
}

func mergeNotebookMetadata(existing, incoming map[string]any) map[string]any {
	merged := make(map[string]any, len(existing)+len(incoming))
	for key, value := range existing {
		merged[key] = value
	}
	for key, value := range incoming {
		if current, ok := merged[key].(map[string]any); ok {
			if next, ok := value.(map[string]any); ok {
				merged[key] = mergeNotebookMetadata(current, next)
				continue
			}
		}
		merged[key] = value
	}
	return merged
}

func (s *FS) notebookCommit(e Entry, snapshot *definitionSnapshot, partPath string) func(context.Context, io.ReaderAt, int64) error {
	base := snapshot.definition
	var before [32]byte
	var baseErr error
	if !snapshot.contentAPI {
		before, baseErr = s.snapshotDigest(snapshot)
	}
	return func(ctx context.Context, reader io.ReaderAt, size int64) (resultErr error) {
		finish := s.startNotebookCommit(e)
		defer func() { finish(resultErr == nil) }()
		start := time.Now()
		prepared := false
		defer func() {
			if !prepared {
				s.observeNotebook("save_prepare", time.Since(start), resultErr)
			}
		}()
		if baseErr != nil && !snapshot.contentAPI {
			return baseErr
		}
		if size > s.opts.MaxNotebookSize {
			return fserrors.ErrTooLarge
		}
		data := make([]byte, size)
		if size > 0 {
			if _, err := reader.ReadAt(data, 0); err != nil {
				return err
			}
		}
		var err error
		if s.opts.NotebookFormat == "py" {
			baseData := snapshot.parts[partPath]
			data, err = pythonToNotebook(data, baseData)
			if err != nil {
				return fmt.Errorf("convert Python notebook to ipynb: %w", errors.Join(fs.ErrInvalid, err))
			}
		}
		data, err = prepareNotebookForFabric(data)
		if err != nil {
			return fmt.Errorf("prepare notebook for Fabric: %w", errors.Join(fs.ErrInvalid, err))
		}
		if int64(len(data)) > s.opts.MaxNotebookSize {
			return fserrors.ErrTooLarge
		}
		if err := validNotebook(data); err != nil {
			return err
		}
		s.observeNotebook("save_prepare", time.Since(start), nil)
		prepared = true
		if snapshot.contentAPI {
			start = time.Now()
			_, err := s.notebookContent.PutNotebookContent(ctx, e.Workspace, e.Item.ID, data)
			s.observeNotebook("content_put", time.Since(start), err)
			if err == nil {
				s.snapshots.Invalidate(snapshotKey(e))
			}
			return err
		}
		desired := base
		desired.Parts = append([]fabric.Part(nil), base.Parts...)
		desired.Format = "ipynb"
		found := false
		for i := range desired.Parts {
			if desired.Parts[i].Path == partPath {
				desired.Parts[i].Payload = base64.StdEncoding.EncodeToString(data)
				desired.Parts[i].PayloadType = "InlineBase64"
				found = true
			}
		}
		if !found {
			return fmt.Errorf("notebook content part disappeared: %w", fserrors.ErrConflict)
		}
		start = time.Now()
		current, err := s.fetchDefinition(ctx, e)
		s.observeNotebook("save_preflight", time.Since(start), err)
		if err != nil {
			return err
		}
		start = time.Now()
		actual, err := digest(current)
		if err != nil {
			s.observeNotebook("digest", time.Since(start), err)
			return err
		}
		after, err := digest(desired)
		s.observeNotebook("digest", time.Since(start), err)
		if err != nil {
			return err
		}
		if actual == after {
			// Reconcile a previous ambiguous response only with an exact full
			// definition match; never discard a different external edit.
			base, before = current, actual
			return nil
		}
		if actual != before {
			return fserrors.ErrConflict
		}
		// Fabric has no documented conditional updateDefinition. This detects
		// prior edits, but the read/update race is explicitly not an atomic CAS.
		start = time.Now()
		err = s.fabric.UpdateNotebook(ctx, e.Workspace, e.Item.ID, desired)
		s.observeNotebook("save_update", time.Since(start), err)
		if err != nil {
			return err
		}
		// This is only the open writer's comparison base, never a published
		// cache snapshot of unobserved server state.
		base, before = desired, after
		return nil
	}
}
