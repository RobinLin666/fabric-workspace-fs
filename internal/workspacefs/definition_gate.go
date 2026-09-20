package workspacefs

import (
	"context"
	"sync"
)

// A prewarm never queues ahead of foreground exports. One background worker
// can reserve at most one of the four slots, including while sharing a flight.
type definitionGate struct {
	mu      sync.Mutex
	active  int
	waiting int
	changed chan struct{}
}

func (g *definitionGate) acquire(ctx context.Context) (func(), error) {
	g.mu.Lock()
	g.waiting++
	for {
		if err := ctx.Err(); err != nil {
			g.waiting--
			g.mu.Unlock()
			return nil, err
		}
		if g.active < 4 {
			g.waiting--
			g.active++
			g.mu.Unlock()
			return g.release, nil
		}
		if g.changed == nil {
			g.changed = make(chan struct{})
		}
		changed := g.changed
		g.mu.Unlock()
		select {
		case <-ctx.Done():
		case <-changed:
		}
		g.mu.Lock()
	}
}

func (g *definitionGate) tryBackground() (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.waiting != 0 || g.active >= 4 {
		return nil, false
	}
	g.active++
	return g.release, true
}

func (g *definitionGate) release() {
	g.mu.Lock()
	g.active--
	if g.changed != nil {
		close(g.changed)
		g.changed = nil
	}
	g.mu.Unlock()
}

type prewarmSlotKey struct{}
