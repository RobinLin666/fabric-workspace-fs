package workspacefs

import (
	"context"
	"errors"
	"io/fs"
	"sync"
	"time"

	"fabric-workspace-fs/internal/fserrors"
)

// NotebookEvent contains no request URLs, credentials, or notebook content.
type NotebookEvent struct {
	Stage   string
	Elapsed time.Duration
	Result  string
}

type NotebookStageStats struct {
	Calls, Failures uint64
	Elapsed         time.Duration
}

type notebookPerf struct {
	mu     sync.Mutex
	stages [11]NotebookStageStats
}

var notebookStages = [...]string{
	"definition_wait", "definition_export", "decode_validate", "digest",
	"save_prepare", "save_preflight", "save_update", "spool_sync", "flush_lock",
	"content_get", "content_put",
}

func (s *FS) observeNotebook(stage string, elapsed time.Duration, err error) {
	for i, name := range notebookStages {
		if stage != name {
			continue
		}
		s.notebookPerf.mu.Lock()
		stat := &s.notebookPerf.stages[i]
		stat.Calls++
		stat.Elapsed += elapsed
		if err != nil {
			stat.Failures++
		}
		s.notebookPerf.mu.Unlock()
		if s.opts.LogNotebookEvent != nil {
			s.opts.LogNotebookEvent(NotebookEvent{Stage: stage, Elapsed: elapsed, Result: notebookResult(err)})
		}
		return
	}
}

func notebookResult(err error) string {
	switch {
	case err == nil:
		return "ok"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, fserrors.ErrConflict):
		return "conflict"
	case errors.Is(err, fserrors.ErrBusy):
		return "busy"
	case errors.Is(err, fs.ErrPermission):
		return "permission"
	default:
		return "error"
	}
}

// NotebookStats includes direct save preflight requests, unlike SnapshotStats.
// It keeps only a fixed set of aggregate counters, never per-item histories.
func (s *FS) NotebookStats() map[string]NotebookStageStats {
	s.notebookPerf.mu.Lock()
	defer s.notebookPerf.mu.Unlock()
	result := make(map[string]NotebookStageStats, len(notebookStages))
	for i, name := range notebookStages {
		result[name] = s.notebookPerf.stages[i]
	}
	return result
}
