package workspacefs

import (
	"context"
	"fmt"
	"io/fs"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/fserrors"
)

const maxPrewarmTargets = 32

type NotebookTarget struct {
	Workspace string
	Notebook  string
}

func (t NotebookTarget) entry() Entry {
	return Entry{Kind: NotebookContent, Workspace: t.Workspace, Item: fabric.Item{ID: t.Notebook, Type: "Notebook"}}
}

// ValidatePrewarmCount performs no remote calls and is also used before CLI
// authentication.
func ValidatePrewarmCount(count int) error {
	if count < 0 || count > maxPrewarmTargets {
		return fmt.Errorf("prewarm Notebook count must be 0..%d: %w", maxPrewarmTargets, fs.ErrInvalid)
	}
	return nil
}

type PrewarmStats struct {
	Queued, Completed, Failed, Skipped, BudgetSkipped, Used uint64
	Pending                                                 int
	Running                                                 bool
}

type notebookPrewarmer struct {
	fs      *FS
	mu      sync.Mutex
	pending map[NotebookTarget]bool
	saving  map[NotebookTarget]int
	queue   chan NotebookTarget
	done    chan struct{}
	cancel  context.CancelFunc
	closed  bool
	stats   PrewarmStats
}

func newNotebookPrewarmer(s *FS) *notebookPrewarmer {
	return &notebookPrewarmer{
		fs: s, pending: make(map[NotebookTarget]bool),
		saving: make(map[NotebookTarget]int), queue: make(chan NotebookTarget, maxPrewarmTargets),
	}
}

// StartPrewarm discovers and warms bounded Notebook targets after the mount is
// ready. It never delays mount readiness or turns a warming failure into an
// apparently successful foreground read.
func (s *FS) StartPrewarm(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p := s.prewarm
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fserrors.ErrClosed
	}
	if p.done != nil {
		return nil
	}
	ctx, p.cancel = context.WithCancel(ctx)
	p.done = make(chan struct{})
	go p.run(ctx)
	return nil
}

func (p *notebookPrewarmer) enqueueLocked(target NotebookTarget) {
	if p.closed || p.done == nil || p.pending[target] {
		return
	}
	select {
	case p.queue <- target:
		p.pending[target] = true
		p.stats.Queued++
	default:
		p.stats.Skipped++
	}
}

func (p *notebookPrewarmer) run(ctx context.Context) {
	defer close(p.done)
	targets, err := p.discoverTargets(ctx)
	if err != nil {
		if ctx.Err() == nil {
			p.recordFailure(err)
		}
	} else {
		p.mu.Lock()
		for _, target := range targets {
			p.enqueueLocked(target)
		}
		p.mu.Unlock()
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
			return
		case target := <-p.queue:
			p.mu.Lock()
			delete(p.pending, target)
			saving := p.saving[target] != 0
			p.stats.Running = true
			p.mu.Unlock()
			skipped, err := true, error(nil)
			if !saving && ctx.Err() == nil {
				skipped, err = p.warm(ctx, target)
			}
			p.mu.Lock()
			p.stats.Running = false
			switch {
			case skipped:
				p.stats.Skipped++
			default:
				p.stats.Completed++
			}
			p.mu.Unlock()
			if err != nil && ctx.Err() == nil {
				p.recordFailure(fmt.Errorf("%s/%s: %w", target.Workspace, target.Notebook, err))
			}
		}
	}
}

func (p *notebookPrewarmer) recordFailure(err error) {
	p.mu.Lock()
	p.stats.Failed++
	p.mu.Unlock()
	if p.fs.opts.LogPrewarmError != nil {
		p.fs.opts.LogPrewarmError(err)
		return
	}
	log.Printf("notebook prewarm failed: %s", notebookResult(err))
}

func (p *notebookPrewarmer) discoverTargets(ctx context.Context) ([]NotebookTarget, error) {
	workspaces := append([]string(nil), p.fs.opts.WorkspaceIDs...)
	if p.fs.opts.AllWorkspaces {
		listed, err := p.fs.fabric.ListWorkspaces(ctx)
		if err != nil {
			return nil, err
		}
		workspaces = make([]string, 0, len(listed))
		for _, workspace := range listed {
			workspaces = append(workspaces, strings.ToLower(workspace.ID))
		}
	}
	sort.Strings(workspaces)
	targets := make([]NotebookTarget, 0, p.fs.opts.PrewarmNotebookCount)
	for _, workspace := range workspaces {
		items, err := p.fs.fabric.ListItems(ctx, workspace)
		if err != nil {
			return nil, err
		}
		notebooks := make([]fabric.Item, 0, len(items))
		for _, item := range items {
			if item.Type == "Notebook" {
				notebooks = append(notebooks, item)
			}
		}
		sort.Slice(notebooks, func(i, j int) bool {
			left, right := strings.ToLower(notebooks[i].DisplayName), strings.ToLower(notebooks[j].DisplayName)
			if left != right {
				return left < right
			}
			return notebooks[i].ID < notebooks[j].ID
		})
		for _, item := range notebooks {
			target := NotebookTarget{Workspace: workspace, Notebook: item.ID}
			policy := p.fs.policy(target.entry())
			if min(policy.Attr, policy.Content, policy.Definition) == 0 {
				p.mu.Lock()
				p.stats.Skipped++
				p.mu.Unlock()
				continue
			}
			targets = append(targets, target)
			if len(targets) == p.fs.opts.PrewarmNotebookCount {
				return targets, nil
			}
		}
	}
	return targets, nil
}

func (p *notebookPrewarmer) warm(parent context.Context, target NotebookTarget) (bool, error) {
	ctx, cancel := context.WithTimeout(parent, p.fs.opts.PrewarmTimeout)
	defer cancel()
	e := target.entry()
	policy := p.fs.policy(e)
	maxAge := min(policy.Attr, policy.Content, policy.Definition)
	if _, ok := p.fs.peekSnapshot(e, maxAge); ok {
		return true, nil
	}
	if p.fs.DefinitionCacheStats().Bytes >= prewarmRetainedByteLimit {
		p.mu.Lock()
		p.stats.BudgetSkipped++
		p.mu.Unlock()
		return true, nil
	}
	release, ok := p.fs.definitionGate.tryBackground()
	if !ok {
		return true, nil
	}
	defer release()
	ctx = context.WithValue(ctx, prewarmSlotKey{}, true)
	_, err := p.fs.sourceSnapshot(ctx, e, maxAge, false)
	return false, err
}

func (s *FS) startNotebookCommit(e Entry) func(bool) {
	p := s.prewarm
	target := NotebookTarget{Workspace: strings.ToLower(e.Workspace), Notebook: strings.ToLower(e.Item.ID)}
	if p == nil {
		return func(bool) { s.invalidateDefinition(e) }
	}
	p.mu.Lock()
	p.saving[target]++
	p.mu.Unlock()
	// Detach any older speculative flight. Existing readers still pin their
	// version; no attempted write body is ever published as a source snapshot.
	s.invalidateDefinition(e)
	return func(success bool) {
		s.invalidateDefinition(e)
		p.mu.Lock()
		defer p.mu.Unlock()
		p.saving[target]--
		if p.saving[target] == 0 {
			delete(p.saving, target)
		}
		if success {
			p.enqueueLocked(target)
		}
	}
}

func (s *FS) PrewarmStats() PrewarmStats {
	if s.prewarm == nil {
		return PrewarmStats{}
	}
	p := s.prewarm
	p.mu.Lock()
	defer p.mu.Unlock()
	stats := p.stats
	stats.Pending = len(p.pending)
	return stats
}

func (p *notebookPrewarmer) close() error {
	p.mu.Lock()
	p.closed = true
	done := p.done
	if p.cancel != nil {
		p.cancel()
	}
	p.mu.Unlock()
	if done == nil {
		return nil
	}
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	select {
	case <-done:
		p.mu.Lock()
		clear(p.pending)
		p.mu.Unlock()
		return nil
	case <-timer.C:
		return fmt.Errorf("notebook prewarm shutdown timed out: %w", context.DeadlineExceeded)
	}
}
