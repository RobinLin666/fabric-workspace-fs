package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/onelake"
)

type fixturePlan struct {
	Stem                string            `json:"stem"`
	OwnershipMarker     string            `json:"ownershipMarker"`
	NotebookCreateURL   string            `json:"notebookCreateURL"`
	NotebookDisplayName string            `json:"notebookDisplayName"`
	NotebookDescription string            `json:"notebookDescription"`
	LakeDirectory       string            `json:"lakeDirectory"`
	RemotePaths         map[string]string `json:"remotePaths"`
	Managed             managedPlan       `json:"managed"`
}

func newPlan(opts options, now time.Time, entropy io.Reader) (fixturePlan, error) {
	random := make([]byte, 12)
	if _, err := io.ReadFull(entropy, random); err != nil {
		return fixturePlan{}, fail("cryptographic fixture name generation failed")
	}
	stem := "fabricfs-e2e-" + now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(random)
	directory := "Files/" + stem
	prefix := onelake.Endpoint + "/" + opts.Workspace + "/" + opts.Lakehouse + "/"
	paths := map[string]string{
		"directory":             prefix + directory,
		"empty":                 prefix + directory + "/empty.txt",
		"payload":               prefix + directory + "/payload.bin",
		"replace":               prefix + directory + "/replace.bin",
		"nested":                prefix + directory + "/nested",
		"tables-readonly-probe": prefix + "Tables/" + stem + "-readonly",
	}
	marker := "fabric-workspace-fs-live-smoke:" + stem
	managedPrefix := strings.ReplaceAll(stem, "-", "_")
	managed := managedPlan{
		FolderName:      managedPrefix + "_folder",
		FolderCreateURL: fabric.BaseURL + "/v1/workspaces/" + opts.Workspace + "/folders",
	}
	for _, kind := range managedKinds {
		name := managedPrefix + "_" + strings.ToLower(kind)
		managed.Items = append(managed.Items, managedItemPlan{
			Type: kind, DisplayName: name, LocalName: name + "." + kind,
			CreateURL: fabric.BaseURL + "/v1/workspaces/" + opts.Workspace + "/" + strings.ToLower(kind) + "s",
		})
	}
	return fixturePlan{
		Stem: stem, OwnershipMarker: marker, NotebookDisplayName: stem,
		NotebookDescription: marker,
		NotebookCreateURL:   fabric.BaseURL + "/v1/workspaces/" + opts.Workspace + "/notebooks",
		LakeDirectory:       directory, RemotePaths: paths, Managed: managed,
	}, nil
}

type counts map[string]uint64

type stageResult struct {
	Name       string        `json:"name"`
	Status     string        `json:"status"`
	StartedAt  time.Time     `json:"startedAt"`
	Duration   time.Duration `json:"durationNanoseconds"`
	HTTP       counts        `json:"httpAttempts"`
	DeniedHTTP uint64        `json:"scopeGuardRejections"`
	Error      string        `json:"error,omitempty"`
}

type measurement struct {
	Name       string        `json:"name"`
	Duration   time.Duration `json:"durationNanoseconds"`
	HTTP       counts        `json:"httpAttempts"`
	Bytes      int           `json:"bytes,omitempty"`
	Detail     string        `json:"detail,omitempty"`
	DeniedHTTP uint64        `json:"scopeGuardRejections"`
}

type cleanupResult struct {
	Handles         string   `json:"handles"`
	Mount           string   `json:"mount"`
	Backend         string   `json:"backend"`
	Notebook        string   `json:"notebook"`
	LakeDirectory   string   `json:"lakeDirectory"`
	LocalParent     string   `json:"localParent"`
	UnexpectedItems int      `json:"unexpectedOwnedDirectoryChildren"`
	Errors          []string `json:"errors,omitempty"`
	Managed         string   `json:"managedResources"`
	Overlay         string   `json:"overlay,omitempty"` // Legacy status is retained, never inferred from the injected bundle.
}

type report struct {
	Version             int                        `json:"version"`
	Status              string                     `json:"status"`
	StartedAt           time.Time                  `json:"startedAt"`
	UpdatedAt           time.Time                  `json:"updatedAt"`
	FinishedAt          time.Time                  `json:"finishedAt,omitempty"`
	AllowWrites         bool                       `json:"allowWrites"`
	Workspace           string                     `json:"workspace"`
	Lakehouse           string                     `json:"lakehouse"`
	EvidenceFile        string                     `json:"evidenceFile"`
	Plan                fixturePlan                `json:"plan"`
	ReturnedFixtureID   string                     `json:"returnedFixtureID,omitempty"`
	FixtureID           string                     `json:"verifiedFixtureID,omitempty"`
	CreateOperationID   string                     `json:"createOperationID,omitempty"`
	DeleteOperationID   string                     `json:"deleteOperationID,omitempty"`
	NotebookItemURL     string                     `json:"notebookItemURL,omitempty"`
	PrivateParent       string                     `json:"privateParent,omitempty"`
	Mountpoint          string                     `json:"mountpoint,omitempty"`
	SpoolDirectory      string                     `json:"spoolDirectory,omitempty"`
	OverlayDirectory    string                     `json:"overlayDirectory,omitempty"` // Legacy only; do not open or remove.
	LogFile             string                     `json:"logFile,omitempty"`
	LakeDirectoryOwned  bool                       `json:"lakeDirectoryOwnershipConfirmed"`
	CreatedPaths        map[string]string          `json:"confirmedCreatedPaths"`
	Stages              []stageResult              `json:"stages"`
	Measurements        []measurement              `json:"measurements"`
	Notes               []string                   `json:"notes,omitempty"`
	HTTP                counts                     `json:"totalHTTPAttempts"`
	DeniedHTTP          uint64                     `json:"scopeGuardRejections"`
	BackendCache        any                        `json:"backendCacheStats,omitempty"`
	Cleanup             cleanupResult              `json:"cleanup"`
	Error               string                     `json:"error,omitempty"`
	CounterScope        string                     `json:"counterScope"`
	CacheTTLNanoseconds time.Duration              `json:"cacheTTLNanoseconds"`
	FixtureSDK          string                     `json:"fixtureSDK"`
	ManagedResources    map[string]managedEvidence `json:"managedResources"`
	CreatedOverlayPaths map[string]bool            `json:"createdOverlayPaths,omitempty"` // Legacy only; no cleanup authority.
}

// Even an old "complete" receipt does not authorize opening or deleting its
// retired overlay store. A plan alone, before any creation, needs no cleanup.
func (r *report) refuseLegacyOverlayCleanup() error {
	if r.OverlayDirectory == "" && len(r.CreatedOverlayPaths) == 0 &&
		(r.Cleanup.Overlay == "" || r.Cleanup.Overlay == "not-created") {
		return nil
	}
	r.Cleanup.Overlay = "unsupported-legacy-overlay-preserved"
	return fail("legacy overlay cleanup is unsupported; recorded paths and their private parent are retained without opening or deleting overlay data")
}

type evidenceFile struct {
	path string
	info os.FileInfo
}

func openEvidence(path string, doc *report) (*evidenceFile, error) {
	doc.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return nil, usageError("initial evidence could not be encoded")
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, usageError("evidence must be a NEW file in an existing writable directory")
	}
	writeErr := writeDurable(file, append(data, '\n'))
	info, statErr := file.Stat()
	closeErr := file.Close()
	if errors.Join(writeErr, statErr, closeErr, syncEvidenceDirectory(filepath.Dir(path))) != nil {
		return nil, usageError("initial evidence could not be made durable; no remote mutation was attempted")
	}
	if runtime.GOOS == "linux" && info.Mode().Perm()&0077 != 0 {
		return nil, usageError("evidence filesystem did not enforce private file permissions")
	}
	return &evidenceFile{path: path, info: info}, nil
}

func writeDurable(file *os.File, data []byte) error {
	n, err := file.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return file.Sync()
}

// Atomic replacement preserves the previous valid evidence on a failed update.
// Both the original and replacement files are private and exclusively created.
func (e *evidenceFile) save(doc *report) error {
	doc.UpdatedAt = time.Now().UTC()
	data, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return fail("evidence encoding failed")
	}
	current, err := os.Lstat(e.path)
	if err != nil || !current.Mode().IsRegular() || !os.SameFile(e.info, current) {
		return fail("evidence file identity changed; refusing to replace it")
	}
	file, err := os.CreateTemp(filepath.Dir(e.path), "."+filepath.Base(e.path)+".update-")
	if err != nil {
		return fail("cannot create private evidence replacement")
	}
	name := file.Name()
	defer os.Remove(name)
	writeErr := writeDurable(file, append(data, '\n'))
	info, statErr := file.Stat()
	closeErr := file.Close()
	if errors.Join(writeErr, statErr, closeErr) != nil {
		return fail("evidence replacement could not be made durable")
	}
	if runtime.GOOS == "linux" && info.Mode().Perm()&0077 != 0 {
		return fail("evidence replacement permissions are not private")
	}
	current, err = os.Lstat(e.path)
	if err != nil || !os.SameFile(e.info, current) {
		return fail("evidence file was replaced concurrently")
	}
	if os.Rename(name, e.path) != nil {
		return fail("atomic evidence replacement failed")
	}
	e.info = info
	if syncEvidenceDirectory(filepath.Dir(e.path)) != nil {
		return fail("evidence directory synchronization failed")
	}
	return nil
}

func initialReport(opts options) (*report, error) {
	now := time.Now().UTC()
	plan, err := newPlan(opts, now, rand.Reader)
	if err != nil {
		return nil, err
	}
	return &report{
		Version: 1, Status: "preflight", StartedAt: now, AllowWrites: true,
		Workspace: opts.Workspace, Lakehouse: opts.Lakehouse, EvidenceFile: opts.Evidence,
		Plan: plan, CreatedPaths: make(map[string]string), HTTP: make(counts),
		ManagedResources: make(map[string]managedEvidence),
		Notes: []string{
			"Notebook and Environment product rmdir must be ENOTSUP without DELETE or definition-empty probing; only this harness's verified creation receipts authorize explicit REST cleanup.",
			"Lakehouse product rmdir uses fresh Files/Tables emptiness checks; the check and deletion are documented as non-atomic.",
			"The root .agents bundle is generated and read-only; it is never a fixture or cleanup target. Legacy overlay data is never opened, uploaded, or deleted.",
			"The five-second smoke cache override is intentional; fresh write comparisons and DIRECT_IO remain production responsibilities.",
		},
		CounterScope:        "actual Fabric and OneLake HTTP attempts, including retries; excludes Azure Identity token acquisition",
		CacheTTLNanoseconds: cacheTTL,
		FixtureSDK:          "github.com/microsoft/fabric-sdk-go v0.20.0 (notebook fixture creation/deletion only)",
		Cleanup: cleanupResult{
			Handles: "not-opened", Mount: "not-created", Backend: "not-created", Notebook: "not-created",
			LakeDirectory: "not-created", LocalParent: "not-created",
			Managed: "not-created",
		},
	}, nil
}
