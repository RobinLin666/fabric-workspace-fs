//go:build linux

// This opt-in diagnostic mounts only a private temporary read-only filesystem.
// It never reuses, upgrades or unmounts an existing user mount.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"fabric-workspace-fs/internal/auth"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fusefs"
	"fabric-workspace-fs/internal/mwc"
	"fabric-workspace-fs/internal/onelake"
	"fabric-workspace-fs/internal/transport"
	"fabric-workspace-fs/internal/workspacefs"
)

type counts struct {
	HTTP, ExportAttempts, Exports, RejectedMutations int
}

func notebookContentPath(path string) (string, error) {
	entries, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".ipynb") {
			return filepath.Join(path, entry.Name()), nil
		}
	}
	return "", fmt.Errorf("Notebook content file not found in %s", path)
}

type meter struct {
	mu sync.Mutex
	counts
	exportStarts []time.Time
}

func (m *meter) RoundTrip(req *http.Request) (*http.Response, error) {
	m.mu.Lock()
	m.HTTP++
	readPost := req.Method == http.MethodPost && (strings.HasSuffix(req.URL.Path, "/getDefinition") ||
		strings.HasSuffix(req.URL.Path, "/generatemwctoken"))
	export := req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/getDefinition")
	if export {
		m.ExportAttempts++
		if len(m.exportStarts) == 32 {
			m.mu.Unlock()
			return nil, errors.New("read-only smoke exceeded its bounded export history")
		}
		m.exportStarts = append(m.exportStarts, time.Now())
	}
	forbidden := req.Method != http.MethodGet && req.Method != http.MethodHead && !readPost
	if forbidden {
		m.RejectedMutations++
	}
	m.mu.Unlock()
	if forbidden {
		return nil, errors.New("read-only smoke blocked a mutating HTTP request")
	}
	response, err := http.DefaultTransport.RoundTrip(req)
	if export && err == nil && response.StatusCode >= 200 && response.StatusCode < 300 {
		m.mu.Lock()
		m.Exports++
		m.mu.Unlock()
	}
	return response, err
}

func (m *meter) snapshot() counts {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.counts
}

func (m *meter) startsSince(index int) []time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]time.Time(nil), m.exportStarts[index:]...)
}

type sample struct {
	RequestedSeconds int                       `json:"requestedSeconds"`
	StartedSourceAge float64                   `json:"startedSourceAgeSeconds"`
	SourceAgeSeconds float64                   `json:"sourceAgeSeconds"`
	ExportSourceAges []float64                 `json:"exportRequestSourceAges,omitempty"`
	ListMilliseconds float64                   `json:"lsMilliseconds"`
	ReadMilliseconds float64                   `json:"catMilliseconds"`
	BodySize         int64                     `json:"bodySize"`
	Counters         counts                    `json:"cumulativeRequests"`
	Snapshots        workspacefs.SnapshotStats `json:"snapshots"`
}

type evidence struct {
	Started           time.Time `json:"started"`
	WorkspaceID       string    `json:"workspaceId"`
	NotebookID        string    `json:"notebookId"`
	RuntimeDirectory  string    `json:"runtimeDirectory"`
	FixedRootExports  int       `json:"fixedRootExports"`
	FixedRootDecodes  uint64    `json:"fixedRootDecodes"`
	Samples           []sample  `json:"samples"`
	BuiltinListMillis float64   `json:"builtinListMilliseconds"`
	Finished          time.Time `json:"finished"`
	Status            string    `json:"status"`
	Cleanup           string    `json:"cleanup"`
	Error             string    `json:"error,omitempty"`
}

func main() {
	var workspace, notebook, relative, output string
	var confirm bool
	flag.StringVar(&workspace, "workspace", "", "authorized workspace UUID")
	flag.StringVar(&notebook, "notebook-id", "", "authorized existing Notebook UUID")
	flag.StringVar(&relative, "notebook-path", "", "relative workspace-name/.../Name.Notebook path, without a Workspaces wrapper")
	flag.StringVar(&output, "evidence", "", "new absolute private JSON evidence path outside the mount")
	flag.BoolVar(&confirm, "confirm-read-only", false, "authorize bounded reads of this exact existing Notebook, never execution or writes")
	flag.Parse()
	if !confirm || fabric.ValidateID(workspace) != nil || fabric.ValidateID(notebook) != nil ||
		relative == "" || filepath.IsAbs(relative) || filepath.Clean(relative) != relative ||
		relative == ".." || strings.HasPrefix(relative, "../") || !filepath.IsAbs(output) || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "explicit --confirm-read-only, UUIDs, safe relative --notebook-path and absolute --evidence are required")
		os.Exit(2)
	}
	file, err := os.OpenFile(output, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	doc := evidence{Started: time.Now().UTC(), WorkspaceID: workspace, NotebookID: notebook, Status: "running"}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	err = run(ctx, workspace, notebook, relative, &doc)
	cancel()
	doc.Finished = time.Now().UTC()
	if err != nil {
		doc.Status, doc.Error = "failed", err.Error()
	} else {
		doc.Status = "passed"
	}
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	err = errors.Join(err, encoder.Encode(doc), file.Sync(), file.Close())
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("Read-only cache smoke passed; evidence: %s\n", output)
}

func run(ctx context.Context, workspace, notebook, relative string, doc *evidence) (result error) {
	tokens, err := auth.NewDefault()
	if err != nil {
		return err
	}
	metrics := &meter{}
	httpClient := &http.Client{Timeout: time.Minute, Transport: metrics}
	newTransport := func(origin, scope string) (*transport.Client, error) {
		return transport.New(transport.Options{BaseURL: origin, Scope: scope, Tokens: tokens, HTTPClient: httpClient, MaxRetries: 2})
	}
	fabricHTTP, err := newTransport(fabric.BaseURL, auth.FabricScope)
	if err != nil {
		return err
	}
	lakeHTTP, err := newTransport("https://onelake.dfs.fabric.microsoft.com", auth.OneLakeScope)
	if err != nil {
		return err
	}
	fab := fabric.New(fabricHTTP, fabric.Options{OperationTimeout: 5 * time.Minute, MaxDefinitionBytes: 64 << 20})
	provider, err := mwc.New(mwc.Options{
		Tokens: tokens, Workspaces: fab, HTTPClient: httpClient, CacheTTL: 2 * time.Minute,
	})
	if err != nil {
		return err
	}
	opts := workspacefs.DefaultOptions()
	opts.WorkspaceIDs, opts.ReadOnly, opts.ResourceBackend, opts.Version = []string{workspace}, true, provider, "read-cache-smoke"
	backend, err := workspacefs.New(fab, onelake.New(lakeHTTP, onelake.Options{}), opts)
	if err != nil {
		return err
	}
	defer func() { result = errors.Join(result, backend.Close()) }()
	root, err := os.MkdirTemp("/tmp", "fabric-workspace-fs-test-readcache-")
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stderr, "private read-cache runtime:", root)
	doc.RuntimeDirectory = root
	var server fusefs.Server
	defer func() {
		if server != nil {
			if err := server.Unmount(); err != nil {
				doc.Cleanup = "retained-unmount-not-confirmed"
				result = errors.Join(result, err)
				return
			}
			server.Wait()
		}
		if err := confirmUnmounted(root); err != nil {
			doc.Cleanup = "retained-mount-state-unconfirmed"
			result = errors.Join(result, err)
			return
		}
		if err := os.RemoveAll(root); err != nil {
			doc.Cleanup = "runtime-removal-failed"
			result = errors.Join(result, err)
			return
		}
		doc.Cleanup = "own-server-stopped-and-exact-runtime-removed"
	}()
	mountpoint := filepath.Join(root, "mount")
	if err := os.Mkdir(mountpoint, 0700); err != nil {
		return err
	}
	server, err = fusefs.Mount(mountpoint, backend, fusefs.Options{ReadOnly: true, Logger: log.New(os.Stderr, "read-cache-smoke: ", log.LstdFlags)})
	if err != nil {
		return err
	}
	path := filepath.Join(mountpoint, relative)
	identity, err := os.Open(filepath.Join(path, ".fabric.json"))
	if err != nil {
		return err
	}
	var meta struct {
		ID, Type, WorkspaceID string
	}
	err = json.NewDecoder(io.LimitReader(identity, 64<<10)).Decode(&meta)
	err = errors.Join(err, identity.Close())
	if err != nil || !strings.EqualFold(meta.ID, notebook) || !strings.EqualFold(meta.WorkspaceID, workspace) || meta.Type != "Notebook" {
		return errors.Join(err, errors.New("mounted Notebook identity did not match the explicitly authorized target"))
	}
	before := metrics.snapshot()
	beforeSnapshots := backend.SnapshotStats()
	entries, err := os.ReadDir(path)
	if err != nil || len(entries) != 3 {
		return errors.Join(err, errors.New("Notebook fixed root did not have exactly three children"))
	}
	doc.FixedRootExports = metrics.snapshot().Exports - before.Exports
	doc.FixedRootDecodes = backend.SnapshotStats().Decodes - beforeSnapshots.Decodes
	if doc.FixedRootExports != 0 || doc.FixedRootDecodes != 0 || before.Exports != 0 {
		return errors.New("fixed listing or basic identity exported/decoded the Notebook")
	}
	var observed, currentObserved time.Time
	for _, seconds := range []int{0, 10, 60, 119, 121} {
		if seconds != 0 {
			timer := time.NewTimer(max(0, time.Until(observed.Add(time.Duration(seconds)*time.Second))))
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			}
		}
		before := metrics.snapshot()
		s := sample{RequestedSeconds: seconds}
		if !observed.IsZero() {
			s.StartedSourceAge = time.Since(observed).Seconds()
		}
		s.ListMilliseconds, err = command(ctx, "ls", "-la", path)
		if err != nil {
			return err
		}
		content, err := notebookContentPath(path)
		if err != nil {
			return err
		}
		info, err := os.Stat(content)
		if err != nil {
			return err
		}
		s.BodySize = info.Size()
		if observed.IsZero() {
			observed = info.ModTime()
		}
		s.SourceAgeSeconds = time.Since(observed).Seconds()
		s.ReadMilliseconds, err = command(ctx, "cat", content)
		if err != nil {
			return err
		}
		info, err = os.Stat(content)
		if err != nil {
			return err
		}
		s.Counters, s.Snapshots = metrics.snapshot(), backend.SnapshotStats()
		for _, at := range metrics.startsSince(before.ExportAttempts) {
			if !currentObserved.IsZero() {
				age := at.Sub(currentObserved).Seconds()
				s.ExportSourceAges = append(s.ExportSourceAges, age)
				if age < 120 {
					doc.Samples = append(doc.Samples, s)
					return fmt.Errorf("definition refreshed early at source age %.6fs", age)
				}
			}
		}
		currentObserved = info.ModTime()
		doc.Samples = append(doc.Samples, s)
		// A long cold export starts after catalog discovery. At source age
		// 119s the catalog's own two-minute deadline may already have passed;
		// record those requests rather than resetting its clock to look warm.
		if seconds > 0 && seconds <= 60 && s.Counters.HTTP != doc.Samples[0].Counters.HTTP {
			return fmt.Errorf("warm access at %ds issued HTTP requests", seconds)
		}
		// At 119s, an expired ancestor catalog can take the content lookup
		// across 120s. Prove the export's actual request time, not merely the
		// caller's scheduled start, and keep that boundary visible in evidence.
		want := uint64(s.Counters.Exports)
		if s.Snapshots.Loads != want || s.Snapshots.Decodes != want ||
			s.Counters.RejectedMutations != 0 || s.BodySize <= 0 ||
			(seconds == 0 && want != 1) || (seconds == 121 && want != 2) ||
			time.Since(currentObserved) >= 2*time.Minute {
			return fmt.Errorf("cache observation at %ds did not match its source deadline: exports=%d loads=%d decodes=%d",
				seconds, s.Counters.Exports, s.Snapshots.Loads, s.Snapshots.Decodes)
		}
	}
	doc.BuiltinListMillis, err = command(ctx, "ls", "-la", filepath.Join(path, "builtin"))
	return err
}

func command(ctx context.Context, name string, args ...string) (float64, error) {
	start := time.Now()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout, cmd.Stderr = io.Discard, os.Stderr
	err := cmd.Run()
	return float64(time.Since(start)) / float64(time.Millisecond), err
}

func confirmUnmounted(root string) error {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return err
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 6 {
			return errors.New("cannot confirm private runtime mount state")
		}
		if fields[4] == root || strings.HasPrefix(fields[4], root+"/") {
			return fmt.Errorf("private runtime still contains a mount: %s", root)
		}
	}
	return scanner.Err()
}
