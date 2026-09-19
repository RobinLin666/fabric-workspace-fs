// Package cache provides bounded, expiring values with per-key load sharing.
package cache

import (
	"container/heap"
	"container/list"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// Options configures a Cache. Callbacks may be called concurrently and must not
// mutate their input values.
type Options[T any] struct {
	// TTL is measured from load completion, not from the last access. Zero
	// disables retention without disabling concurrent load sharing.
	TTL time.Duration
	// MaxEntries must be positive when TTL is positive.
	MaxEntries int
	// MaxBytes is an optional approximate value-size limit. Zero means no
	// explicit byte limit. A positive limit requires Size.
	MaxBytes int64
	// Size measures a retained value in bytes and must return a nonnegative
	// number. Without Size, Stats.Bytes is zero. Size is not used at TTL zero.
	Size func(T) int64
	// Clone, when provided, takes a private snapshot of successful loads and
	// copies every returned value, including shared and uncached results.
	// It must deeply copy any mutable data that callers might modify.
	// Without Clone, values must be treated as immutable.
	Clone func(T) T
	// ObservedAt preserves an upstream observation time in a derived cache.
	// Zero uses load completion. It must never return a future time.
	ObservedAt func(T) time.Time
	// Now defaults to time.Now. Expiration is reclaimed by Get, Peek and Stats.
	Now func() time.Time
}

// Stats is a point-in-time snapshot. Counters are cumulative and are not reset
// by invalidation. Misses includes Shared; Loads counts actual loader calls.
// Requests rejected before lookup (invalid options or a canceled context) do
// not affect counters. Entries and Bytes exclude expired values.
type Stats struct {
	Hits, Misses, Loads, Shared uint64
	Entries                     int
	Bytes                       int64
}

// Cache is safe for concurrent use and must not be copied after first use.
// Its zero value shares concurrent loads but does not retain their results.
type Cache[T any] struct {
	mu        sync.Mutex
	options   Options[T]
	configErr error
	entries   map[string]*entry[T]
	flights   map[string]*flight[T]
	lru       list.List
	expiry    expiryHeap[T]
	bytes     int64
	stats     Stats
}

type entry[T any] struct {
	key         string
	value       T
	size        int64
	observed    time.Time
	expires     time.Time
	lru         *list.Element
	expiryIndex int
}

type flight[T any] struct {
	done  chan struct{}
	value T
	err   error
}

var errLoadAborted = errors.New("cache: load did not complete")

// New constructs a cache. Negative TTL or budgets, a positive TTL without a
// positive MaxEntries, or a positive MaxBytes without Size are invalid. Since
// New cannot return an error, every Get on an invalid cache reports that error
// without invoking its loader.
func New[T any](options Options[T]) *Cache[T] {
	c := &Cache[T]{options: options}
	switch {
	case options.TTL < 0:
		c.configErr = errors.New("cache: TTL must not be negative")
	case options.MaxEntries < 0:
		c.configErr = errors.New("cache: MaxEntries must not be negative")
	case options.MaxBytes < 0:
		c.configErr = errors.New("cache: MaxBytes must not be negative")
	case options.TTL > 0 && options.MaxEntries == 0:
		c.configErr = errors.New("cache: positive TTL requires positive MaxEntries")
	case options.MaxBytes > 0 && options.Size == nil:
		c.configErr = errors.New("cache: positive MaxBytes requires Size")
	}
	return c
}

// Get returns an unexpired value or shares a load with callers of the same key.
// The first caller executes load synchronously with its own context. Canceling
// a waiting caller does not cancel the load; canceling the leader is observed
// by its loader and any joined callers. Loaders must cooperate with cancellation
// for the leader to return promptly. No background goroutines are created.
//
// Errors are never retained. Successful values larger than MaxBytes are returned
// normally but not retained. A negative Size result is reported as an error.
// A nil load is allowed on a hit or when joining a load, but is an error on a
// fresh miss. A nil context is an error.
func (c *Cache[T]) Get(ctx context.Context, key string, load func(context.Context) (T, error)) (T, error) {
	return c.GetWithin(ctx, key, c.options.TTL, c.options.TTL, load)
}

// GetWithTTL applies one key's policy without creating separate unbounded caches.
func (c *Cache[T]) GetWithTTL(ctx context.Context, key string, ttl time.Duration, load func(context.Context) (T, error)) (T, error) {
	return c.GetWithin(ctx, key, ttl, ttl, load)
}

// GetWithin requires a source observation younger than maxAge, while retaining
// successful loads for at most retainTTL from that same observation. A zero
// maxAge never hits a retained value, but concurrent loads are still shared.
// Using a shorter maxAge does not reset or extend an older source's deadline.
func (c *Cache[T]) GetWithin(ctx context.Context, key string, maxAge, retainTTL time.Duration, load func(context.Context) (T, error)) (T, error) {
	var zero T
	if c.configErr != nil {
		return zero, c.configErr
	}
	if maxAge < 0 || retainTTL < 0 || (retainTTL > 0 && c.options.MaxEntries <= 0) {
		return zero, errors.New("cache: invalid per-key age or retention")
	}
	if ctx == nil {
		return zero, errors.New("cache: context must not be nil")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}

	now := c.now()
	c.mu.Lock()
	if err := ctx.Err(); err != nil {
		c.mu.Unlock()
		return zero, err
	}
	c.expireLocked(now)
	if e, ok := c.entries[key]; ok && maxAge > 0 && now.Before(e.observed.Add(maxAge)) {
		c.stats.Hits++
		c.lru.MoveToFront(e.lru)
		value := e.value
		c.mu.Unlock()
		return c.result(ctx, value, nil)
	}

	c.stats.Misses++
	if f, ok := c.flights[key]; ok {
		c.stats.Shared++
		c.mu.Unlock()
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-f.done:
			return c.result(ctx, f.value, f.err)
		}
	}
	if load == nil {
		c.mu.Unlock()
		return zero, errors.New("cache: a fresh miss requires a loader")
	}
	f := &flight[T]{done: make(chan struct{})}
	if c.flights == nil {
		c.flights = make(map[string]*flight[T])
	}
	c.flights[key] = f
	c.stats.Loads++
	c.mu.Unlock()

	return c.runLoad(ctx, key, f, retainTTL, load)
}

func (c *Cache[T]) runLoad(ctx context.Context, key string, f *flight[T], retainTTL time.Duration, load func(context.Context) (T, error)) (T, error) {
	finished := false
	defer func() {
		if !finished {
			// Preserve a loader/callback panic (or Goexit), but do not strand
			// joined callers or leave a permanently pending key.
			var zero T
			c.complete(ctx, key, f, zero, errLoadAborted, 0, time.Time{}, time.Time{}, 0)
		}
	}()

	value, err := load(ctx)
	var size int64
	var now, observed time.Time
	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		value = c.clone(value)
		now = c.now()
		observed = now
		if c.options.ObservedAt != nil {
			if source := c.options.ObservedAt(value); !source.IsZero() {
				observed = source
				if source.After(now) {
					err = errors.New("cache: source observation is in the future")
				}
			}
		}
		if retainTTL > 0 {
			if c.options.Size != nil {
				size = c.options.Size(value)
				if size < 0 {
					err = fmt.Errorf("cache: Size returned a negative value for key %q", key)
				}
			}
		}
	}
	c.complete(ctx, key, f, value, err, size, now, observed, retainTTL)
	finished = true
	return c.result(ctx, f.value, f.err)
}

func (c *Cache[T]) complete(ctx context.Context, key string, f *flight[T], value T, err error, size int64, now, observed time.Time, retainTTL time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err == nil {
		err = ctx.Err()
	}
	if err == nil {
		f.value = value
	}
	f.err = err
	// Invalidation detaches a flight. Pointer identity prevents an older
	// leader from publishing data or deleting a replacement flight.
	if c.flights[key] == f {
		delete(c.flights, key)
		if err == nil {
			c.expireLocked(now)
			if old := c.entries[key]; old != nil {
				c.removeLocked(old)
			}
			if retainTTL > 0 && now.Before(observed.Add(retainTTL)) {
				c.insertLocked(key, value, size, observed, observed.Add(retainTTL))
			}
		}
	}
	close(f.done)
}

func (c *Cache[T]) result(ctx context.Context, value T, err error) (T, error) {
	var zero T
	if ctxErr := ctx.Err(); ctxErr != nil {
		return zero, ctxErr
	}
	if err != nil {
		return zero, err
	}
	value = c.clone(value)
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	return value, nil
}

func (c *Cache[T]) clone(value T) T {
	if c.options.Clone != nil {
		return c.options.Clone(value)
	}
	return value
}

func (c *Cache[T]) now() time.Time {
	if c.options.Now != nil {
		return c.options.Now()
	}
	return time.Now()
}

// Peek returns a retained, unexpired value without loading or joining a flight.
// It creates no secondary retained reference and does not affect request counts.
func (c *Cache[T]) Peek(key string) (T, bool) {
	value, _, ok := c.PeekWithin(key, time.Duration(1<<63-1))
	return value, ok
}

// PeekWithin additionally applies a source-age limit. Expired values are purged
// across the entire cache, including when the requested key is absent.
func (c *Cache[T]) PeekWithin(key string, maxAge time.Duration) (T, time.Time, bool) {
	var zero T
	now := c.now()
	c.mu.Lock()
	c.expireLocked(now)
	e := c.entries[key]
	if c.configErr != nil || e == nil || maxAge <= 0 || !now.Before(e.observed.Add(maxAge)) {
		c.mu.Unlock()
		return zero, time.Time{}, false
	}
	value, observed := e.value, e.observed
	c.mu.Unlock()
	return c.clone(value), observed, true
}

// Invalidate removes a retained value and detaches its pending load, if any.
// Existing callers may still receive that load's snapshot, but it cannot seed
// the cache and subsequent callers cannot join it.
func (c *Cache[T]) Invalidate(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		c.removeLocked(e)
	}
	delete(c.flights, key)
}

// InvalidatePrefix invalidates retained and pending keys with the literal string
// prefix. Include a trailing separator for a directory boundary; an empty prefix
// matches all keys.
func (c *Cache[T]) InvalidatePrefix(prefix string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, e := range c.entries {
		if strings.HasPrefix(key, prefix) {
			c.removeLocked(e)
		}
	}
	for key := range c.flights {
		if strings.HasPrefix(key, prefix) {
			delete(c.flights, key)
		}
	}
}

// Clear invalidates all retained values and pending loads, preserving counters.
func (c *Cache[T]) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = nil
	c.flights = nil
	c.lru.Init()
	c.expiry = nil
	c.bytes = 0
}

// Stats returns current counters and retained, unexpired value sizes.
func (c *Cache[T]) Stats() Stats {
	now := c.now()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.expireLocked(now)
	stats := c.stats
	stats.Entries = len(c.entries)
	stats.Bytes = c.bytes
	return stats
}

func (c *Cache[T]) insertLocked(key string, value T, size int64, observed, expires time.Time) {
	limit := c.options.MaxBytes
	if limit == 0 {
		// Even without a configured byte limit, keep accounting representable.
		limit = 1<<63 - 1
	}
	if size > limit {
		return
	}
	for len(c.entries) >= c.options.MaxEntries || c.bytes > limit-size {
		c.removeLocked(c.lru.Back().Value.(*entry[T]))
	}
	e := &entry[T]{key: key, value: value, size: size, observed: observed, expires: expires}
	e.lru = c.lru.PushFront(e)
	heap.Push(&c.expiry, e)
	if c.entries == nil {
		c.entries = make(map[string]*entry[T])
	}
	c.entries[key] = e
	c.bytes += size
}

func (c *Cache[T]) removeLocked(e *entry[T]) {
	delete(c.entries, e.key)
	c.lru.Remove(e.lru)
	heap.Remove(&c.expiry, e.expiryIndex)
	c.bytes -= e.size
}

func (c *Cache[T]) expireLocked(now time.Time) {
	for len(c.expiry) > 0 && !now.Before(c.expiry[0].expires) {
		c.removeLocked(c.expiry[0])
	}
}

// Each retained entry has exactly one heap node; eviction removes it rather
// than accumulating stale expiration records. The separate heap avoids a full
// cache scan on hits when LRU and expiration order differ.
type expiryHeap[T any] []*entry[T]

func (h expiryHeap[T]) Len() int { return len(h) }

func (h expiryHeap[T]) Less(i, j int) bool {
	if h[i].expires.Equal(h[j].expires) {
		return h[i].key < h[j].key
	}
	return h[i].expires.Before(h[j].expires)
}

func (h expiryHeap[T]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].expiryIndex = i
	h[j].expiryIndex = j
}

func (h *expiryHeap[T]) Push(value any) {
	e := value.(*entry[T])
	e.expiryIndex = len(*h)
	*h = append(*h, e)
}

func (h *expiryHeap[T]) Pop() any {
	old := *h
	e := old[len(old)-1]
	old[len(old)-1] = nil
	*h = old[:len(old)-1]
	e.expiryIndex = -1
	return e
}
