package cache

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newClock() *testClock {
	return &testClock{now: time.Date(2026, time.September, 18, 0, 0, 0, 0, time.UTC)}
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

func getValue[T any](t *testing.T, c *Cache[T], key string, loaded T) T {
	t.Helper()
	value, err := c.Get(context.Background(), key, func(context.Context) (T, error) {
		return loaded, nil
	})
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	return value
}

func expectHit[T comparable](t *testing.T, c *Cache[T], key string, want T) {
	t.Helper()
	value, err := c.Get(context.Background(), key, nil)
	if value != want || err != nil {
		t.Fatalf("Get(%q) = (%v, %v), want (%v, nil)", key, value, err, want)
	}
}

type outcome[T any] struct {
	value T
	err   error
}

func startGet[T any](c *Cache[T], ctx context.Context, key string, load func(context.Context) (T, error)) <-chan outcome[T] {
	result := make(chan outcome[T], 1)
	go func() {
		value, err := c.Get(ctx, key, load)
		result <- outcome[T]{value, err}
	}()
	return result
}

func expectResult[T comparable](t *testing.T, result <-chan outcome[T], want T, wantErr error) {
	t.Helper()
	got := <-result
	if got.value != want || !errors.Is(got.err, wantErr) {
		t.Fatalf("result = (%v, %v), want (%v, %v)", got.value, got.err, want, wantErr)
	}
}

func expectStats[T any](t *testing.T, c *Cache[T], want Stats) {
	t.Helper()
	if got := c.Stats(); got != want {
		t.Fatalf("Stats() = %+v, want %+v", got, want)
	}
}

func checkAccounting[T any](t *testing.T, c *Cache[T]) {
	t.Helper()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) != c.lru.Len() || len(c.entries) != len(c.expiry) {
		t.Fatalf("bookkeeping lengths: entries=%d lru=%d expiry=%d", len(c.entries), c.lru.Len(), len(c.expiry))
	}
	var total int64
	for key, e := range c.entries {
		total += e.size
		if e.key != key || e.lru.Value != e || c.expiry[e.expiryIndex] != e {
			t.Fatalf("inconsistent indexes for %q", key)
		}
	}
	if total != c.bytes || c.bytes < 0 {
		t.Fatalf("bytes = %d, sum of entries = %d", c.bytes, total)
	}
	for i, e := range c.expiry {
		if e.expiryIndex != i || (i > 0 && c.expiry.Less(i, (i-1)/2)) {
			t.Fatalf("invalid expiration heap at index %d", i)
		}
	}
}

func expectLRU[T any](t *testing.T, c *Cache[T], want ...string) {
	t.Helper()
	checkAccounting(t, c)
	c.mu.Lock()
	defer c.mu.Unlock()
	var got []string
	for e := c.lru.Front(); e != nil; e = e.Next() {
		got = append(got, e.Value.(*entry[T]).key)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("LRU (newest first) = %v, want %v", got, want)
	}
}

func TestInvalidOptions(t *testing.T) {
	tests := []struct {
		name    string
		options Options[int]
		message string
	}{
		{"negative TTL", Options[int]{TTL: -1}, "TTL"},
		{"negative entry budget", Options[int]{MaxEntries: -1}, "MaxEntries"},
		{"negative byte budget", Options[int]{MaxBytes: -1}, "MaxBytes"},
		{"unbounded entries", Options[int]{TTL: time.Minute}, "positive MaxEntries"},
		{"missing size", Options[int]{TTL: time.Minute, MaxEntries: 1, MaxBytes: 1}, "requires Size"},
		{"missing size with zero TTL", Options[int]{MaxBytes: 1}, "requires Size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := New(tt.options)
			for range 2 {
				value, err := c.Get(context.Background(), "key", func(context.Context) (int, error) {
					t.Fatal("invalid cache invoked its loader")
					return 1, nil
				})
				if value != 0 || err == nil || !strings.Contains(err.Error(), tt.message) {
					t.Fatalf("Get = (%v, %v), want explicit %q error", value, err, tt.message)
				}
			}
			c.Invalidate("key")
			c.InvalidatePrefix("")
			c.Clear()
			expectStats(t, c, Stats{})
		})
	}
}

func TestInvalidArgumentsAndPreCanceledContext(t *testing.T) {
	c := New(Options[int]{TTL: time.Minute, MaxEntries: 2})
	if _, err := c.Get(nil, "key", nil); err == nil {
		t.Fatal("nil context succeeded")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if value, err := c.Get(ctx, "key", nil); value != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled miss = (%v, %v)", value, err)
	}
	expectStats(t, c, Stats{})
	if value, err := c.Get(context.Background(), "key", nil); value != 0 || err == nil {
		t.Fatalf("nil loader on fresh miss = (%v, %v)", value, err)
	}
	getValue(t, c, "key", 5)
	before := c.Stats()
	if value, err := c.Get(ctx, "key", nil); value != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled hit = (%v, %v)", value, err)
	}
	expectStats(t, c, before)
	expectHit(t, c, "key", 5)
}

func TestCancellationBeforeLookupDoesNotInvokeLoader(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := New(Options[int]{
		TTL: time.Minute, MaxEntries: 1,
		Now: func() time.Time {
			cancel()
			return time.Now()
		},
	})
	value, err := c.Get(ctx, "key", func(context.Context) (int, error) {
		t.Fatal("canceled lookup invoked its loader")
		return 1, nil
	})
	if value != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled lookup = (%v, %v)", value, err)
	}
	expectStats(t, c, Stats{})
}

func TestCancellationWhileCloningHit(t *testing.T) {
	var cancelOnClone context.CancelFunc
	c := New(Options[int]{
		TTL: time.Minute, MaxEntries: 1,
		Clone: func(value int) int {
			if cancelOnClone != nil {
				cancelOnClone()
			}
			return value
		},
	})
	getValue(t, c, "key", 5)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelOnClone = cancel
	value, err := c.Get(ctx, "key", nil)
	if value != 0 || !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled clone = (%v, %v)", value, err)
	}
	expectStats(t, c, Stats{Hits: 1, Misses: 1, Loads: 1, Entries: 1})
	expectHit(t, c, "key", 5)
}

func TestTTLAndCounters(t *testing.T) {
	clock := newClock()
	c := New(Options[string]{
		TTL: time.Second, MaxEntries: 2, MaxBytes: 10,
		Size: func(value string) int64 { return int64(len(value)) }, Now: clock.Now,
	})
	if got := getValue(t, c, "key", "old"); got != "old" {
		t.Fatalf("initial value = %q", got)
	}
	expectStats(t, c, Stats{Misses: 1, Loads: 1, Entries: 1, Bytes: 3})
	clock.Advance(time.Second - time.Nanosecond)
	expectHit(t, c, "key", "old")
	expectStats(t, c, Stats{Hits: 1, Misses: 1, Loads: 1, Entries: 1, Bytes: 3})
	clock.Advance(time.Nanosecond)
	expectStats(t, c, Stats{Hits: 1, Misses: 1, Loads: 1})
	if got := getValue(t, c, "key", "new!"); got != "new!" {
		t.Fatalf("expired value = %q", got)
	}
	expectStats(t, c, Stats{Hits: 1, Misses: 2, Loads: 2, Entries: 1, Bytes: 4})
	c.Invalidate("key")
	expectStats(t, c, Stats{Hits: 1, Misses: 2, Loads: 2})
}

func TestTTLStartsAtLoadCompletion(t *testing.T) {
	clock := newClock()
	c := New(Options[int]{TTL: time.Second, MaxEntries: 2, Now: clock.Now})
	value, err := c.Get(context.Background(), "key", func(context.Context) (int, error) {
		clock.Advance(time.Hour)
		return 9, nil
	})
	if value != 9 || err != nil {
		t.Fatalf("load = (%v, %v)", value, err)
	}
	clock.Advance(time.Second - time.Nanosecond)
	expectHit(t, c, "key", 9)
	clock.Advance(time.Nanosecond)
	if got := getValue(t, c, "key", 10); got != 10 {
		t.Fatalf("expired load = %v", got)
	}
}

func TestExpirationIndependentOfLRU(t *testing.T) {
	clock := newClock()
	c := New(Options[int]{TTL: 10 * time.Second, MaxEntries: 3, Now: clock.Now})
	getValue(t, c, "a", 1)
	clock.Advance(time.Second)
	getValue(t, c, "b", 2)
	clock.Advance(time.Second)
	getValue(t, c, "c", 3)
	expectHit(t, c, "a", 1)
	expectLRU(t, c, "a", "c", "b")
	clock.Advance(8 * time.Second)
	getValue(t, c, "d", 4)
	expectLRU(t, c, "d", "c", "b")
	clock.Advance(2 * time.Second)
	expectStats(t, c, Stats{Hits: 1, Misses: 4, Loads: 4, Entries: 1})
	expectLRU(t, c, "d")
}

func TestExpirationHandlesClockReordering(t *testing.T) {
	clock := newClock()
	c := New(Options[int]{TTL: time.Minute, MaxEntries: 4, Now: clock.Now})
	getValue(t, c, "later", 1)
	clock.Advance(-time.Hour)
	getValue(t, c, "earlier", 2)
	clock.Advance(time.Minute)
	expectHit(t, c, "later", 1)
	expectLRU(t, c, "later")
	if got := getValue(t, c, "earlier", 3); got != 3 {
		t.Fatalf("earlier-expiring value = %v", got)
	}
}

func TestLRUCountAndBytes(t *testing.T) {
	size := func(value string) int64 { return int64(len(value)) }
	t.Run("entry count", func(t *testing.T) {
		c := New(Options[string]{TTL: time.Hour, MaxEntries: 2, Size: size})
		getValue(t, c, "a", "aa")
		getValue(t, c, "b", "b")
		expectHit(t, c, "a", "aa")
		getValue(t, c, "c", "ccc")
		expectLRU(t, c, "c", "a")
		expectStats(t, c, Stats{Hits: 1, Misses: 3, Loads: 3, Entries: 2, Bytes: 5})
		getValue(t, c, "b", "bb")
		expectLRU(t, c, "b", "c")
	})
	t.Run("byte limit evicts multiple least recent entries", func(t *testing.T) {
		c := New(Options[string]{TTL: time.Hour, MaxEntries: 5, MaxBytes: 6, Size: size})
		getValue(t, c, "a", "aa")
		getValue(t, c, "b", "bbb")
		getValue(t, c, "c", "c")
		expectHit(t, c, "a", "aa")
		getValue(t, c, "d", "dddd")
		expectLRU(t, c, "d", "a")
		expectStats(t, c, Stats{Hits: 1, Misses: 4, Loads: 4, Entries: 2, Bytes: 6})
		c.Invalidate("a")
		getValue(t, c, "a", "aaaaaa")
		expectLRU(t, c, "a")
		if got := c.Stats().Bytes; got != 6 {
			t.Fatalf("replacement byte accounting = %d", got)
		}
	})
	t.Run("zero sized entries still respect count", func(t *testing.T) {
		c := New(Options[string]{TTL: time.Hour, MaxEntries: 2, MaxBytes: 1, Size: size})
		for _, key := range []string{"a", "b", "c"} {
			getValue(t, c, key, "")
		}
		expectLRU(t, c, "c", "b")
		expectStats(t, c, Stats{Misses: 3, Loads: 3, Entries: 2})
	})
	t.Run("byte accounting cannot overflow", func(t *testing.T) {
		c := New(Options[int64]{TTL: time.Hour, MaxEntries: 2, Size: func(value int64) int64 { return value }})
		getValue(t, c, "a", int64(1<<63-1))
		getValue(t, c, "b", int64(1))
		expectLRU(t, c, "b")
		expectStats(t, c, Stats{Misses: 2, Loads: 2, Entries: 1, Bytes: 1})
	})
}

func TestOversizeSuccessReturnedWithoutRetentionOrEviction(t *testing.T) {
	c := New(Options[string]{
		TTL: time.Hour, MaxEntries: 2, MaxBytes: 3,
		Size: func(value string) int64 { return int64(len(value)) },
	})
	getValue(t, c, "small", "ok")
	for _, value := range []string{"large", "larger"} {
		if got := getValue(t, c, "large", value); got != value {
			t.Fatalf("oversize result = %q, want %q", got, value)
		}
		expectLRU(t, c, "small")
	}
	expectStats(t, c, Stats{Misses: 3, Loads: 3, Entries: 1, Bytes: 2})
	expectHit(t, c, "small", "ok")
	getValue(t, c, "exact", "yes")
	expectLRU(t, c, "exact")
}

func TestNegativeSizeIsErrorAndRetryable(t *testing.T) {
	c := New(Options[int64]{
		TTL: time.Minute, MaxEntries: 2, MaxBytes: 4, Size: func(value int64) int64 { return value },
	})
	value, err := c.Get(context.Background(), "key", func(context.Context) (int64, error) { return -1, nil })
	if value != 0 || err == nil || !strings.Contains(err.Error(), "negative") {
		t.Fatalf("negative Size = (%v, %v)", value, err)
	}
	expectStats(t, c, Stats{Misses: 1, Loads: 1})
	if got := getValue(t, c, "key", int64(2)); got != 2 {
		t.Fatalf("retry result = %v", got)
	}
	expectStats(t, c, Stats{Misses: 2, Loads: 2, Entries: 1, Bytes: 2})
}

func TestZeroTTLAndZeroValueDoNotRetain(t *testing.T) {
	caches := []*Cache[int]{
		New(Options[int]{}),
		new(Cache[int]),
		New(Options[int]{MaxEntries: 1, MaxBytes: 1, Size: func(int) int64 {
			t.Fatal("Size must not run when retention is disabled")
			return 0
		}}),
	}
	for _, c := range caches {
		for value := range 3 {
			if got := getValue(t, c, "key", value); got != value {
				t.Fatalf("uncached value = %v, want %v", got, value)
			}
		}
		expectStats(t, c, Stats{Misses: 3, Loads: 3})
		expectLRU(t, c)
	}
}

func TestConcurrentSameKeySharesOneLoad(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Minute} {
		t.Run(ttl.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New(Options[int]{TTL: ttl, MaxEntries: 2})
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				results := []<-chan outcome[int]{startGet(c, context.Background(), "key", func(context.Context) (int, error) {
					<-release
					return 21, nil
				})}
				synctest.Wait()
				for range 15 {
					results = append(results, startGet(c, context.Background(), "key", nil))
				}
				synctest.Wait()
				expectStats(t, c, Stats{Misses: 16, Loads: 1, Shared: 15})
				unblock()
				for _, result := range results {
					expectResult(t, result, 21, nil)
				}
				want := Stats{Misses: 16, Loads: 1, Shared: 15}
				wantValue := 22
				if ttl > 0 {
					want.Entries = 1
					wantValue = 21
				}
				expectStats(t, c, want)
				if got := getValue(t, c, "key", 22); got != wantValue {
					t.Fatalf("next call = %v, want %v", got, wantValue)
				}
			})
		})
	}
}

func TestDifferentKeysLoadIndependently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New(Options[int]{TTL: time.Minute, MaxEntries: 2})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		slow := startGet(c, context.Background(), "slow", func(context.Context) (int, error) {
			<-release
			return 1, nil
		})
		synctest.Wait()
		fast := startGet(c, context.Background(), "fast", func(context.Context) (int, error) { return 2, nil })
		synctest.Wait()
		expectResult(t, fast, 2, nil)
		expectStats(t, c, Stats{Misses: 2, Loads: 2, Entries: 1})
		unblock()
		expectResult(t, slow, 1, nil)
	})
}

func TestErrorsAreNotRetained(t *testing.T) {
	for _, message := range []string{"401 unauthenticated", "403 forbidden", "500 internal error", "503 unavailable"} {
		t.Run(message, func(t *testing.T) {
			c := New(Options[int]{TTL: time.Minute, MaxEntries: 2})
			cause := errors.New(message)
			failure := fmt.Errorf("remote request: %w", cause)
			for range 2 {
				value, err := c.Get(context.Background(), "key", func(context.Context) (int, error) { return 99, failure })
				if value != 0 || err != failure || !errors.Is(err, cause) {
					t.Fatalf("load failure = (%v, %v)", value, err)
				}
			}
			expectStats(t, c, Stats{Misses: 2, Loads: 2})
			getValue(t, c, "key", 7)
			expectHit(t, c, "key", 7)
		})
	}
}

func TestFailedLeaderFansOutAndFreshCallRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := New(Options[int]{TTL: time.Minute, MaxEntries: 2})
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		failure := errors.New("403 forbidden")
		results := []<-chan outcome[int]{startGet(c, context.Background(), "key", func(context.Context) (int, error) {
			<-release
			return 99, failure
		})}
		synctest.Wait()
		for range 8 {
			results = append(results, startGet(c, context.Background(), "key", nil))
		}
		synctest.Wait()
		unblock()
		for _, result := range results {
			expectResult(t, result, 0, failure)
		}
		expectStats(t, c, Stats{Misses: 9, Loads: 1, Shared: 8})
		getValue(t, c, "key", 8)
		expectStats(t, c, Stats{Misses: 10, Loads: 2, Shared: 8, Entries: 1})
	})
}

func TestWaitingCancellationAndDeadline(t *testing.T) {
	for _, ttl := range []time.Duration{0, time.Minute} {
		for _, deadline := range []bool{false, true} {
			t.Run(fmt.Sprintf("TTL=%v/deadline=%v", ttl, deadline), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					c := New(Options[int]{TTL: ttl, MaxEntries: 2})
					release := make(chan struct{})
					unblock := sync.OnceFunc(func() { close(release) })
					defer unblock()
					leader := startGet(c, context.Background(), "key", func(context.Context) (int, error) {
						<-release
						return 13, nil
					})
					synctest.Wait()
					var ctx context.Context
					var cancel context.CancelFunc
					if deadline {
						ctx, cancel = context.WithTimeout(context.Background(), time.Second)
					} else {
						ctx, cancel = context.WithCancel(context.Background())
					}
					defer cancel()
					waiter := startGet(c, ctx, "key", nil)
					synctest.Wait()
					wantErr := context.Canceled
					if deadline {
						time.Sleep(time.Second)
						wantErr = context.DeadlineExceeded
					} else {
						cancel()
					}
					expectResult(t, waiter, 0, wantErr)
					select {
					case result := <-leader:
						t.Fatalf("waiter cancellation released leader: %+v", result)
					default:
					}
					survivor := startGet(c, context.Background(), "key", nil)
					synctest.Wait()
					expectStats(t, c, Stats{Misses: 3, Loads: 1, Shared: 2})
					unblock()
					expectResult(t, leader, 13, nil)
					expectResult(t, survivor, 13, nil)
				})
			})
		}
	}
}

func TestLeaderCancellationNeverSeedsCache(t *testing.T) {
	for _, cooperative := range []bool{false, true} {
		t.Run(fmt.Sprintf("cooperative=%v", cooperative), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New(Options[int]{TTL: time.Minute, MaxEntries: 2})
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				leader := startGet(c, ctx, "key", func(gotContext context.Context) (int, error) {
					if gotContext != ctx {
						return 0, errors.New("loader did not receive leader context")
					}
					if cooperative {
						<-gotContext.Done()
						return 99, gotContext.Err()
					}
					<-release
					return 99, nil
				})
				synctest.Wait()
				waiter := startGet(c, context.Background(), "key", nil)
				synctest.Wait()
				cancel()
				unblock()
				expectResult(t, leader, 0, context.Canceled)
				expectResult(t, waiter, 0, context.Canceled)
				expectStats(t, c, Stats{Misses: 2, Loads: 1, Shared: 1})
				getValue(t, c, "key", 7)
				expectHit(t, c, "key", 7)
			})
		})
	}
}

func TestInvalidateDetachesInflightAndCannotOverwriteReplacement(t *testing.T) {
	for _, oldFinishesFirst := range []bool{false, true} {
		t.Run(fmt.Sprintf("oldFinishesFirst=%v", oldFinishesFirst), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New(Options[string]{TTL: time.Minute, MaxEntries: 2})
				oldRelease, newRelease := make(chan struct{}), make(chan struct{})
				releaseOld := sync.OnceFunc(func() { close(oldRelease) })
				releaseNew := sync.OnceFunc(func() { close(newRelease) })
				defer releaseOld()
				defer releaseNew()
				old := startGet(c, context.Background(), "key", func(context.Context) (string, error) {
					<-oldRelease
					return "old", nil
				})
				synctest.Wait()
				oldWaiter := startGet(c, context.Background(), "key", nil)
				synctest.Wait()
				c.Invalidate("key")
				replacement := startGet(c, context.Background(), "key", func(context.Context) (string, error) {
					<-newRelease
					return "new", nil
				})
				synctest.Wait()
				expectStats(t, c, Stats{Misses: 3, Loads: 2, Shared: 1})
				if oldFinishesFirst {
					releaseOld()
					expectResult(t, old, "old", nil)
					expectResult(t, oldWaiter, "old", nil)
					expectStats(t, c, Stats{Misses: 3, Loads: 2, Shared: 1})
					joinedReplacement := startGet(c, context.Background(), "key", nil)
					synctest.Wait()
					expectStats(t, c, Stats{Misses: 4, Loads: 2, Shared: 2})
					releaseNew()
					expectResult(t, joinedReplacement, "new", nil)
					expectResult(t, replacement, "new", nil)
				} else {
					releaseNew()
					expectResult(t, replacement, "new", nil)
					expectHit(t, c, "key", "new")
					releaseOld()
					expectResult(t, old, "old", nil)
					expectResult(t, oldWaiter, "old", nil)
				}
				expectHit(t, c, "key", "new")
				expectLRU(t, c, "key")
			})
		})
	}
}

func TestInvalidatePrefixBoundaries(t *testing.T) {
	c := New(Options[string]{TTL: time.Hour, MaxEntries: 8})
	keys := []string{"", "dir", "dir/one", "dir/nested/two", "directory/one", "other", "dir.*"}
	for _, key := range keys {
		getValue(t, c, key, "old")
	}
	c.InvalidatePrefix("dir/")
	for _, key := range keys {
		want := "old"
		if strings.HasPrefix(key, "dir/") {
			want = "new"
		}
		if got := getValue(t, c, key, "new"); got != want {
			t.Fatalf("Get(%q) after prefix invalidation = %q, want %q", key, got, want)
		}
	}
	c.InvalidatePrefix("dir.*")
	expectHit(t, c, "dir/one", "new")
	if got := getValue(t, c, "dir.*", "literal"); got != "literal" {
		t.Fatalf("literal prefix result = %q", got)
	}
	checkAccounting(t, c)
}

func TestBulkInvalidationCoversRetainedAndInflightKeys(t *testing.T) {
	for _, operation := range []string{"prefix", "empty prefix", "clear"} {
		t.Run(operation, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New(Options[string]{TTL: time.Hour, MaxEntries: 10})
				getValue(t, c, "dir/ready", "old")
				getValue(t, c, "outside/ready", "old")
				keys := []string{"dir/pending", "dir/nested/pending", "outside/pending"}
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				var old []<-chan outcome[string]
				for _, key := range keys {
					old = append(old, startGet(c, context.Background(), key, func(context.Context) (string, error) {
						<-release
						return "old", nil
					}))
				}
				synctest.Wait()
				switch operation {
				case "prefix":
					c.InvalidatePrefix("dir/")
				case "empty prefix":
					c.InvalidatePrefix("")
				case "clear":
					c.Clear()
				}
				retained := 0
				if operation == "prefix" {
					retained = 1
				}
				expectStats(t, c, Stats{Misses: 5, Loads: 5, Entries: retained})
				var fresh []<-chan outcome[string]
				for _, key := range keys {
					fresh = append(fresh, startGet(c, context.Background(), key, func(context.Context) (string, error) {
						return "new", nil
					}))
				}
				synctest.Wait()
				wantStats := Stats{Misses: 8, Loads: 8, Entries: 3}
				if operation == "prefix" {
					wantStats.Loads = 7
					wantStats.Shared = 1
				}
				expectStats(t, c, wantStats)
				expectResult(t, fresh[0], "new", nil)
				expectResult(t, fresh[1], "new", nil)
				unblock()
				for _, result := range old {
					expectResult(t, result, "old", nil)
				}
				outside := "new"
				if operation == "prefix" {
					outside = "old"
				}
				expectResult(t, fresh[2], outside, nil)
				expectHit(t, c, keys[0], "new")
				expectHit(t, c, keys[1], "new")
				expectHit(t, c, keys[2], outside)
				if got := getValue(t, c, "dir/ready", "new"); got != "new" {
					t.Fatalf("retained matching key survived invalidation: %q", got)
				}
				if got := getValue(t, c, "outside/ready", "new"); got != outside {
					t.Fatalf("retained outside key = %q, want %q", got, outside)
				}
				checkAccounting(t, c)
			})
		})
	}
}

func TestClearPreservesCountersAndReleasesAccounting(t *testing.T) {
	c := New(Options[int]{TTL: time.Hour, MaxEntries: 4, Size: func(value int) int64 { return int64(value) }})
	getValue(t, c, "a", 1)
	getValue(t, c, "b", 2)
	expectHit(t, c, "a", 1)
	c.Clear()
	expectStats(t, c, Stats{Hits: 1, Misses: 2, Loads: 2})
	expectLRU(t, c)
	getValue(t, c, "a", 3)
	expectStats(t, c, Stats{Hits: 1, Misses: 3, Loads: 3, Entries: 1, Bytes: 3})
}

func cloneMap(value map[string][]int) map[string][]int {
	if value == nil {
		return nil
	}
	result := make(map[string][]int, len(value))
	for key, item := range value {
		result[key] = slices.Clone(item)
	}
	return result
}

func TestCloneIsolatesLoaderLeaderAndCacheHits(t *testing.T) {
	c := New(Options[map[string][]int]{TTL: time.Minute, MaxEntries: 2, Clone: cloneMap})
	original := map[string][]int{"items": {1, 2}}
	first := getValue(t, c, "key", original)
	original["items"][0] = 10
	original["loader"] = []int{10}
	first["items"][0] = 20
	first["caller"] = []int{20}
	for range 2 {
		value, err := c.Get(context.Background(), "key", nil)
		if err != nil || len(value) != 1 || !slices.Equal(value["items"], []int{1, 2}) {
			t.Fatalf("retained snapshot = %v, err=%v", value, err)
		}
		value["items"][1] = 30
		delete(value, "items")
	}
}

func TestSharedCloneIsolationIncludingUncachedValues(t *testing.T) {
	for _, mode := range []string{"retained", "zero TTL", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				options := Options[[]int]{
					TTL: time.Minute, MaxEntries: 2, Clone: slices.Clone[[]int],
					Size: func(value []int) int64 { return int64(len(value)) },
				}
				if mode == "zero TTL" {
					options.TTL = 0
				} else if mode == "oversize" {
					options.MaxBytes = 1
				}
				c := New(options)
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				original := []int{1, 2}
				leader := startGet(c, context.Background(), "key", func(context.Context) ([]int, error) {
					<-release
					return original, nil
				})
				synctest.Wait()
				waiter := startGet(c, context.Background(), "key", nil)
				synctest.Wait()
				unblock()
				first, second := <-leader, <-waiter
				if first.err != nil || second.err != nil {
					t.Fatalf("shared errors = %v, %v", first.err, second.err)
				}
				original[0] = 10
				first.value[0] = 20
				if !slices.Equal(second.value, []int{1, 2}) {
					t.Fatalf("shared snapshot mutated: %v", second.value)
				}
				want := Stats{Misses: 2, Loads: 1, Shared: 1}
				if mode == "retained" {
					want.Entries, want.Bytes = 1, 2
				}
				expectStats(t, c, want)
			})
		})
	}
}

func TestSizeAndCloneRunOutsideGlobalLock(t *testing.T) {
	var c *Cache[[]int]
	c = New(Options[[]int]{
		TTL: time.Minute, MaxEntries: 1, MaxBytes: 10,
		Clone: func(value []int) []int {
			c.Stats()
			return slices.Clone(value)
		},
		Size: func(value []int) int64 {
			c.Stats()
			return int64(len(value))
		},
	})
	if got := getValue(t, c, "key", []int{1}); !slices.Equal(got, []int{1}) {
		t.Fatalf("value = %v", got)
	}
	value, err := c.Get(context.Background(), "key", nil)
	if err != nil || !slices.Equal(value, []int{1}) {
		t.Fatalf("hit = (%v, %v)", value, err)
	}
}

func TestAbortedLoaderReleasesWaitersAndAllowsRetry(t *testing.T) {
	for _, goexit := range []bool{false, true} {
		t.Run(fmt.Sprintf("Goexit=%v", goexit), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c := New(Options[int]{TTL: time.Minute, MaxEntries: 2})
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				leaderEnded := make(chan any, 1)
				go func() {
					defer func() { leaderEnded <- recover() }()
					c.Get(context.Background(), "key", func(context.Context) (int, error) {
						<-release
						if goexit {
							runtime.Goexit()
						}
						panic("loader panic")
					})
				}()
				synctest.Wait()
				waiter := startGet(c, context.Background(), "key", nil)
				synctest.Wait()
				unblock()
				expectResult(t, waiter, 0, errLoadAborted)
				got := <-leaderEnded
				if (!goexit && got != "loader panic") || (goexit && got != nil) {
					t.Fatalf("leader panic = %v, Goexit=%v", got, goexit)
				}
				expectStats(t, c, Stats{Misses: 2, Loads: 1, Shared: 1})
				getValue(t, c, "key", 9)
				expectHit(t, c, "key", 9)
			})
		})
	}
}

func TestBookkeepingRemainsBoundedAcrossEvictionAndInvalidation(t *testing.T) {
	c := New(Options[int]{TTL: time.Hour, MaxEntries: 4, Size: func(int) int64 { return 1 }})
	for i := range 1000 {
		key := fmt.Sprintf("key/%d", i)
		getValue(t, c, key, i)
		c.Invalidate(fmt.Sprintf("absent/%d", i))
		c.InvalidatePrefix(fmt.Sprintf("absent-prefix/%d", i))
		if i%5 == 0 {
			c.Invalidate(key)
		}
		checkAccounting(t, c)
		if stats := c.Stats(); stats.Entries > 4 || stats.Bytes > 4 {
			t.Fatalf("retention exceeded budgets: %+v", stats)
		}
		if len(c.flights) != 0 {
			t.Fatalf("completed flights retained: %d", len(c.flights))
		}
	}
	c.InvalidatePrefix("")
	expectLRU(t, c)
	if len(c.entries) != 0 || len(c.flights) != 0 {
		t.Fatal("invalidation left key bookkeeping behind")
	}
}

func TestConcurrentMixedOperations(t *testing.T) {
	clock := newClock()
	c := New(Options[[]int64]{
		TTL: time.Millisecond, MaxEntries: 7, MaxBytes: 32, Now: clock.Now,
		Clone: slices.Clone[[]int64], Size: func(value []int64) int64 { return int64(len(value)) * 8 },
	})
	const workers, iterations = 12, 200
	var sequence atomic.Int64
	var wg sync.WaitGroup
	for worker := range workers {
		wg.Go(func() {
			for i := range iterations {
				key := fmt.Sprintf("%d/%d", worker%3, i%11)
				value, err := c.Get(context.Background(), key, func(context.Context) ([]int64, error) {
					runtime.Gosched()
					return []int64{sequence.Add(1)}, nil
				})
				if err != nil || len(value) != 1 || value[0] <= 0 {
					t.Errorf("Get = (%v, %v)", value, err)
					return
				}
				value[0] = -1
				switch i % 9 {
				case 0:
					c.Invalidate(key)
				case 1:
					c.InvalidatePrefix(fmt.Sprintf("%d/", worker%3))
				case 2:
					c.Clear()
				case 3:
					clock.Advance(100 * time.Microsecond)
				case 4:
					c.Stats()
				}
			}
		})
	}
	wg.Wait()
	checkAccounting(t, c)
	stats := c.Stats()
	if stats.Hits+stats.Misses != workers*iterations || stats.Misses != stats.Loads+stats.Shared {
		t.Fatalf("incorrect concurrent counters: %+v", stats)
	}
	if stats.Entries > 4 || stats.Bytes > 32 || len(c.flights) != 0 {
		t.Fatalf("unbounded concurrent bookkeeping: %+v, flights=%d", stats, len(c.flights))
	}
}
