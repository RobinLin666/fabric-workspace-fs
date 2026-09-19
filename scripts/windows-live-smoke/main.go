//go:build windows

// Command windows-live-smoke performs an explicitly authorized, disposable
// Windows WinFsp validation. It only mutates resources created by this run.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fusefs"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/workspacefs"
)

type identity struct {
	ID             string `json:"id"`
	Type           string `json:"type"`
	DisplayName    string `json:"displayName"`
	WorkspaceID    string `json:"workspaceId"`
	FolderID       string `json:"folderId"`
	ParentFolderID string `json:"parentFolderId"`
}

type timing struct {
	Name     string `json:"name"`
	Duration string `json:"duration"`
}

type evidence struct {
	StartedAt   time.Time         `json:"startedAt"`
	CompletedAt time.Time         `json:"completedAt,omitempty"`
	WorkspaceID string            `json:"workspaceId"`
	Workspace   string            `json:"workspaceDisplayName,omitempty"`
	Prefix      string            `json:"prefix"`
	Mountpoint  string            `json:"mountpoint"`
	PrivateRoot string            `json:"privateRoot"`
	IDs         map[string]string `json:"fixtureIds"`
	Timings     []timing          `json:"timings"`
	Checks      map[string]string `json:"checks"`
	Cleanup     map[string]string `json:"cleanup"`
	FNTKHelp    string            `json:"fntkHelp"`
	Error       string            `json:"error,omitempty"`
}

type run struct {
	ctx         context.Context
	cancel      context.CancelFunc
	evidence    *evidence
	path        string
	fab         *fabric.Client
	lake        *onelake.Client
	backend     *workspacefs.FS
	server      fusefs.Server
	logFile     *os.File
	root        string
	spool       string
	mount       string
	workspaceID string
	folder      fabric.Folder
	items       map[string]fabric.Item
}

func main() {
	var workspaceID, evidencePath string
	flag.StringVar(&workspaceID, "workspace", "", "authorized workspace UUID")
	flag.StringVar(&evidencePath, "evidence", "", "new JSON evidence file")
	flag.Parse()
	if flag.NArg() != 0 || fabric.ValidateID(workspaceID) != nil || evidencePath == "" {
		fmt.Fprintln(os.Stderr, "usage: windows-live-smoke --workspace UUID --evidence NEW_FILE.json")
		os.Exit(2)
	}
	path, err := filepath.Abs(evidencePath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "invalid evidence path")
		os.Exit(2)
	}
	if _, err := os.Lstat(path); !errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintln(os.Stderr, "evidence path must not already exist")
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	r := &run{
		ctx: ctx, cancel: cancel, path: path, workspaceID: workspaceID, items: make(map[string]fabric.Item),
		evidence: &evidence{
			StartedAt: time.Now().UTC(), WorkspaceID: workspaceID,
			IDs: make(map[string]string), Checks: make(map[string]string),
			Cleanup: make(map[string]string),
		},
	}
	err = r.execute()
	cleanupErr := r.cleanup()
	r.cancel()
	r.evidence.CompletedAt = time.Now().UTC()
	if err != nil {
		r.evidence.Error = safeError(err)
	}
	if cleanupErr != nil {
		if r.evidence.Error != "" {
			r.evidence.Error += "; "
		}
		r.evidence.Error += safeError(cleanupErr)
	}
	if saveErr := r.save(); saveErr != nil {
		fmt.Fprintln(os.Stderr, "save evidence:", safeError(saveErr))
		os.Exit(1)
	}
	if err != nil || cleanupErr != nil {
		fmt.Fprintln(os.Stderr, r.evidence.Error)
		os.Exit(1)
	}
	fmt.Println("windows live smoke passed:", path)
}

func safeError(err error) string {
	var response *transport.HTTPError
	if errors.As(err, &response) {
		return response.Error()
	}
	return err.Error()
}

func (r *run) save() error {
	data, err := json.MarshalIndent(r.evidence, "", "  ")
	if err != nil {
		return err
	}
	tmp := r.path + ".tmp"
	if err := os.WriteFile(tmp, append(data, '\n'), 0600); err != nil {
		return err
	}
	return os.Rename(tmp, r.path)
}

func (r *run) execute() error {
	if err := fusefs.CheckPrerequisites(); err != nil {
		return err
	}
	var err error
	r.root, err = os.MkdirTemp("", "fabric-workspace-fs-win-live-")
	if err != nil {
		return err
	}
	r.spool = filepath.Join(r.root, "spool")
	if err := os.Mkdir(r.spool, 0700); err != nil {
		return err
	}
	r.mount, err = unusedDrive()
	if err != nil {
		return err
	}
	r.evidence.PrivateRoot, r.evidence.Mountpoint = r.root, r.mount
	if err := r.save(); err != nil {
		return err
	}
	if err := r.clients(); err != nil {
		return err
	}
	workspace, err := r.fab.GetWorkspace(r.ctx, r.workspaceID)
	if err != nil {
		return err
	}
	r.evidence.Workspace = workspace.DisplayName
	prefix, err := uniquePrefix()
	if err != nil {
		return err
	}
	r.evidence.Prefix = prefix
	if err := r.assertNamesAbsent(); err != nil {
		return err
	}
	if err := r.createFixtures(); err != nil {
		return err
	}
	if err := r.mountFilesystem(); err != nil {
		return err
	}
	if err := r.verifyMounted(); err != nil {
		return err
	}
	r.checkFNTK()
	return r.save()
}

func (r *run) clients() error {
	tokens, err := auth.NewDefault()
	if err != nil {
		return err
	}
	httpClient := &http.Client{Timeout: time.Minute}
	makeTransport := func(origin, scope string) (*transport.Client, error) {
		return transport.New(transport.Options{
			BaseURL: origin, Scope: scope, Tokens: tokens, HTTPClient: httpClient,
			MaxRetries: 3, RetryDelay: time.Second, MaxRetryDelay: 30 * time.Second,
		})
	}
	fabTransport, err := makeTransport(fabric.BaseURL, auth.FabricScope)
	if err != nil {
		return err
	}
	lakeTransport, err := makeTransport(onelake.Endpoint, auth.OneLakeScope)
	if err != nil {
		return err
	}
	r.fab = fabric.New(fabTransport, fabric.Options{
		OperationTimeout: 5 * time.Minute, PollInterval: time.Second, MaxDefinitionBytes: 64 << 20,
	})
	r.lake = onelake.New(lakeTransport, onelake.Options{ChunkSize: 4 << 20, MaxPages: 1000})
	return nil
}

func uniquePrefix() (string, error) {
	var random [5]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", err
	}
	return "FabricFsWinE2E" + time.Now().UTC().Format("20060102T150405") + hex.EncodeToString(random[:]), nil
}

func (r *run) assertNamesAbsent() error {
	folders, err := r.fab.ListFolders(r.ctx, r.workspaceID)
	if err != nil {
		return err
	}
	items, err := r.fab.ListItems(r.ctx, r.workspaceID)
	if err != nil {
		return err
	}
	for _, folder := range folders {
		if strings.HasPrefix(folder.DisplayName, r.evidence.Prefix) {
			return errors.New("random fixture prefix already exists")
		}
	}
	for _, item := range items {
		if strings.HasPrefix(item.DisplayName, r.evidence.Prefix) {
			return errors.New("random fixture prefix already exists")
		}
	}
	return nil
}

func (r *run) createFixtures() error {
	var err error
	r.folder, err = r.fab.CreateFolder(r.ctx, r.workspaceID, r.evidence.Prefix+"Folder", "")
	if err != nil {
		return err
	}
	r.evidence.IDs["Folder"] = r.folder.ID
	if err := r.save(); err != nil {
		return err
	}
	for _, kind := range []string{"Notebook", "Lakehouse", "Environment"} {
		item, err := r.fab.CreateItem(r.ctx, r.workspaceID, kind, r.evidence.Prefix+kind, r.folder.ID)
		if err != nil {
			return err
		}
		r.items[kind] = item
		r.evidence.IDs[kind] = item.ID
		if err := r.save(); err != nil {
			return err
		}
	}
	return nil
}

func (r *run) mountFilesystem() error {
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs = []string{r.workspaceID}
	opts.SpoolDirectory = r.spool
	opts.CacheTTL = 2 * time.Minute
	backend, err := workspacefs.New(r.fab, r.lake, opts)
	if err != nil {
		return err
	}
	r.backend = backend
	logPath := filepath.Join(r.root, "fuse.log")
	r.logFile, err = os.OpenFile(logPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	start := time.Now()
	r.server, err = fusefs.Mount(r.mount, backend, fusefs.Options{Logger: log.New(r.logFile, "", log.LstdFlags)})
	r.record("mount", start)
	return err
}

func (r *run) record(name string, started time.Time) {
	r.evidence.Timings = append(r.evidence.Timings, timing{Name: name, Duration: time.Since(started).String()})
}

func (r *run) verifyMounted() error {
	root := r.mount + `\`
	start := time.Now()
	entries, err := os.ReadDir(root)
	r.record("root-cold-readdir", start)
	if err != nil {
		return err
	}
	start = time.Now()
	if _, err := os.ReadDir(root); err != nil {
		return err
	}
	r.record("root-warm-readdir", start)
	if len(entries) == 0 {
		return errors.New("mounted root is empty")
	}
	workspaceName, err := r.workspaceDirectory()
	if err != nil {
		return err
	}
	folderPath := filepath.Join(root, workspaceName, r.folder.DisplayName)
	if id, err := readIdentity(filepath.Join(folderPath, ".fabric.json")); err != nil || !strings.EqualFold(id.ID, r.folder.ID) {
		return fmt.Errorf("mounted folder identity mismatch: %w", err)
	}
	paths := make(map[string]string)
	for kind, item := range r.items {
		path := filepath.Join(folderPath, item.DisplayName+"."+kind)
		meta, err := readIdentity(filepath.Join(path, ".fabric.json"))
		if err != nil || !strings.EqualFold(meta.ID, item.ID) || meta.Type != kind ||
			!strings.EqualFold(meta.FolderID, r.folder.ID) {
			return fmt.Errorf("mounted %s identity mismatch: %w", kind, err)
		}
		paths[kind] = path
	}
	if err := r.notebookChecks(paths["Notebook"]); err != nil {
		return err
	}
	if err := r.lakehouseChecks(paths["Lakehouse"]); err != nil {
		return err
	}
	if err := expectWriteDenied(filepath.Join(paths["Lakehouse"], "Tables", "blocked.txt")); err != nil {
		return fmt.Errorf("Tables boundary: %w", err)
	}
	r.evidence.Checks["Tables"] = "write-denied"
	if err := expectWriteDenied(filepath.Join(paths["Environment"], "blocked.txt")); err != nil {
		return fmt.Errorf("Environment boundary: %w", err)
	}
	r.evidence.Checks["Environment"] = "write-denied"
	return nil
}

func (r *run) workspaceDirectory() (string, error) {
	entries, err := r.backend.ReadDir(r.ctx, r.backend.Root())
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if entry.Kind == workspacefs.Workspace && strings.EqualFold(entry.Workspace, r.workspaceID) {
			return entry.Name, nil
		}
	}
	return "", fs.ErrNotExist
}

func readIdentity(path string) (identity, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return identity{}, err
	}
	var result identity
	if err := json.Unmarshal(data, &result); err != nil {
		return identity{}, err
	}
	return result, nil
}

func (r *run) notebookChecks(path string) error {
	content := filepath.Join(path, "content.ipynb")
	start := time.Now()
	if _, err := os.ReadFile(content); err != nil {
		return err
	}
	r.record("notebook-cold-read", start)
	start = time.Now()
	if _, err := os.ReadFile(content); err != nil {
		return err
	}
	r.record("notebook-warm-read", start)
	updated := []byte(`{"nbformat":4,"nbformat_minor":5,"cells":[],"metadata":{"fabricWorkspaceFsWindowsE2E":true}}`)
	start = time.Now()
	file, err := os.OpenFile(content, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	if _, err := file.Write(updated); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	r.record("notebook-save", start)
	start = time.Now()
	got, err := os.ReadFile(content)
	r.record("notebook-readback", start)
	if err != nil {
		return err
	}
	var notebook struct {
		Metadata map[string]any `json:"metadata"`
	}
	if json.Unmarshal(got, &notebook) != nil || notebook.Metadata["fabricWorkspaceFsWindowsE2E"] != true {
		return fmt.Errorf("notebook readback lost the owned marker (saved=%d bytes, read=%d bytes)", len(updated), len(got))
	}
	r.evidence.Checks["Notebook"] = fmt.Sprintf("read-write-readback-marker; remoteBytes=%d", len(got))
	if _, err := os.Stat(filepath.Join(path, ".platform")); !errors.Is(err, fs.ErrNotExist) {
		return errors.New("hidden .platform unexpectedly exists")
	}
	return nil
}

func (r *run) lakehouseChecks(path string) error {
	files := filepath.Join(path, "Files")
	dir := filepath.Join(files, "e2e")
	file := filepath.Join(dir, "payload.txt")
	renamed := filepath.Join(dir, "renamed.txt")
	start := time.Now()
	if err := os.Mkdir(dir, 0755); err != nil {
		return err
	}
	out, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
	if err != nil {
		return err
	}
	if _, err = out.Write([]byte("windows-winfsp-e2e")); err == nil {
		err = out.Sync()
	}
	if closeErr := out.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	got, err := os.ReadFile(file)
	if err != nil || string(got) != "windows-winfsp-e2e" {
		return errors.New("Lakehouse file readback mismatch")
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 || entries[0].Name() != "payload.txt" {
		return errors.New("Lakehouse directory listing mismatch")
	}
	if err := os.Rename(file, renamed); err != nil {
		return err
	}
	if err := os.Remove(renamed); err != nil {
		return err
	}
	if err := os.Remove(dir); err != nil {
		return err
	}
	r.record("lakehouse-files-crud", start)
	r.evidence.Checks["LakehouseFiles"] = "mkdir-write-read-list-rename-delete"
	return nil
}

func expectWriteDenied(path string) error {
	err := os.WriteFile(path, []byte("must-not-exist"), 0644)
	if err == nil {
		_ = os.Remove(path)
		return errors.New("write unexpectedly succeeded")
	}
	if _, statErr := os.Stat(path); !errors.Is(statErr, fs.ErrNotExist) {
		return errors.New("denied write left a path behind")
	}
	return nil
}

func (r *run) checkFNTK() {
	path, err := exec.LookPath("fntk")
	if err != nil {
		r.evidence.FNTKHelp = "not-installed"
		return
	}
	ctx, cancel := context.WithTimeout(r.ctx, 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, path, "help", "--json")
	command.Stdout, command.Stderr = io.Discard, io.Discard
	if err := command.Run(); err != nil {
		r.evidence.FNTKHelp = "help-failed"
		return
	}
	r.evidence.FNTKHelp = "help-json-passed-no-execution"
}

func (r *run) cleanup() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	var failures []error
	if r.server != nil {
		if err := r.server.Unmount(); err != nil {
			failures = append(failures, err)
			r.evidence.Cleanup["Mount"] = "unmount-failed"
		} else {
			r.server.Wait()
			r.evidence.Cleanup["Mount"] = "unmounted-and-waited"
		}
	}
	if r.logFile != nil {
		failures = append(failures, errors.Join(r.logFile.Sync(), r.logFile.Close()))
	}
	if r.backend != nil {
		failures = append(failures, r.backend.Close())
	}
	for _, kind := range []string{"Environment", "Lakehouse", "Notebook"} {
		item, ok := r.items[kind]
		if !ok {
			continue
		}
		current, err := r.fab.GetItem(ctx, r.workspaceID, item.ID)
		if err != nil || current.ID != item.ID || current.Type != kind ||
			current.DisplayName != item.DisplayName || !strings.EqualFold(current.FolderID, r.folder.ID) {
			failures = append(failures, fmt.Errorf("refusing cleanup for unverified %s identity: %w", kind, err))
			continue
		}
		if err := r.fab.DeleteItem(ctx, r.workspaceID, item.ID); err != nil {
			failures = append(failures, err)
			continue
		}
		if err := r.waitItemAbsent(ctx, r.fab, item.ID); err != nil {
			failures = append(failures, err)
			continue
		}
		r.evidence.Cleanup[kind] = "exact-id-deleted-confirmed-404"
	}
	if r.folder.ID != "" {
		current, err := r.fab.GetFolder(ctx, r.workspaceID, r.folder.ID)
		if err != nil || current.ID != r.folder.ID || current.DisplayName != r.folder.DisplayName {
			failures = append(failures, fmt.Errorf("refusing cleanup for unverified Folder identity: %w", err))
		} else if err := r.fab.DeleteFolder(ctx, r.workspaceID, r.folder.ID); err != nil {
			failures = append(failures, err)
		} else if err := r.waitFolderAbsent(ctx, r.fab, r.folder.ID); err != nil {
			failures = append(failures, err)
		} else {
			r.evidence.Cleanup["Folder"] = "exact-id-deleted-confirmed-404"
		}
	}
	if r.mount != "" {
		if _, err := os.Stat(r.mount + `\`); errors.Is(err, fs.ErrNotExist) {
			r.evidence.Cleanup["Drive"] = "absent"
		} else {
			failures = append(failures, errors.New("mount drive remains present"))
		}
	}
	if r.root != "" && r.evidence.Cleanup["Mount"] == "unmounted-and-waited" {
		if err := os.RemoveAll(r.root); err != nil {
			failures = append(failures, err)
		} else {
			r.evidence.Cleanup["PrivateRoot"] = "removed"
		}
	}
	return errors.Join(failures...)
}

func (r *run) waitItemAbsent(ctx context.Context, client *fabric.Client, id string) error {
	for {
		_, err := client.GetItem(ctx, r.workspaceID, id)
		if notFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := sleep(ctx); err != nil {
			return err
		}
	}
}

func (r *run) waitFolderAbsent(ctx context.Context, client *fabric.Client, id string) error {
	for {
		_, err := client.GetFolder(ctx, r.workspaceID, id)
		if notFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := sleep(ctx); err != nil {
			return err
		}
	}
}

func notFound(err error) bool {
	var response *transport.HTTPError
	return errors.As(err, &response) && response.StatusCode == http.StatusNotFound
}

func sleep(ctx context.Context) error {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func unusedDrive() (string, error) {
	for letter := 'Z'; letter >= 'D'; letter-- {
		drive := string(letter) + ":"
		if _, err := os.Stat(drive + `\`); errors.Is(err, fs.ErrNotExist) {
			return drive, nil
		}
	}
	return "", errors.New("no unused Windows drive letter is available")
}
