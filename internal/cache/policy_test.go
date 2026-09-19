package cache

import (
	"context"
	"slices"
	"testing"
	"time"
)

func TestTwoMinutePolicyUsesSourceAge(t *testing.T) {
	clock := newClock()
	c := New(Options[int]{TTL: 2 * time.Minute, MaxEntries: 4, Now: clock.Now})
	loads := 0
	load := func(context.Context) (int, error) { loads++; return loads, nil }
	start := clock.Now()
	for _, elapsed := range []time.Duration{0, 10, 60, 119, 121} {
		clock.Advance(start.Add(elapsed * time.Second).Sub(clock.Now()))
		value, err := c.Get(context.Background(), "item", load)
		want := 1
		if elapsed > 120 {
			want = 2
		}
		if err != nil || value != want || loads != want {
			t.Fatalf("t=%vs value=%d loads=%d err=%v", elapsed, value, loads, err)
		}
	}
}

func TestPerOperationFreshnessDoesNotRestartSourceClock(t *testing.T) {
	clock := newClock()
	c := New(Options[int]{MaxEntries: 4, Now: clock.Now})
	loads := 0
	load := func(context.Context) (int, error) { loads++; return loads, nil }
	get := func(age, retention time.Duration, want int) {
		t.Helper()
		got, err := c.GetWithin(context.Background(), "definition", age, retention, load)
		if err != nil || got != want || loads != want {
			t.Fatalf("value=%d loads=%d err=%v; want=%d", got, loads, err, want)
		}
		checkAccounting(t, c)
	}
	get(time.Second, 2*time.Minute, 1)
	clock.Advance(10 * time.Second)
	get(2*time.Minute, 2*time.Minute, 1)
	get(time.Second, 2*time.Minute, 2)
	clock.Advance(119 * time.Second)
	get(2*time.Minute, 2*time.Minute, 2)
	clock.Advance(time.Second)
	get(2*time.Minute, 2*time.Minute, 3)
	get(0, 0, 4)
	if _, ok := c.Peek("definition"); ok {
		t.Fatal("zero-retention refresh left an earlier cached snapshot")
	}
}

func TestDerivedCacheCannotExtendUpstreamObservation(t *testing.T) {
	clock := newClock()
	type value struct{ observed time.Time }
	source := value{observed: clock.Now()}
	c := New(Options[value]{
		TTL: 2 * time.Minute, MaxEntries: 2, Now: clock.Now,
		ObservedAt: func(v value) time.Time { return v.observed },
	})
	clock.Advance(119 * time.Second)
	getValue(t, c, "derived", source)
	if _, at, ok := c.PeekWithin("derived", 2*time.Minute); !ok || !at.Equal(source.observed) {
		t.Fatal("derived value lost source time")
	}
	if _, _, ok := c.PeekWithin("derived", time.Minute); ok {
		t.Fatal("strict age accepted a stale source")
	}
	clock.Advance(time.Second)
	if _, ok := c.Peek("derived"); ok || c.Stats().Entries != 0 {
		t.Fatal("derived cache extended source TTL")
	}
	getValue(t, c, "already-expired", source)
	if _, ok := c.Peek("already-expired"); ok {
		t.Fatal("already expired source was retained")
	}
	future := value{observed: clock.Now().Add(time.Second)}
	if _, err := c.Get(context.Background(), "future", func(context.Context) (value, error) { return future, nil }); err == nil {
		t.Fatal("future observation accepted")
	}
}

func TestPeekSharesOnlyBoundedRetainedValues(t *testing.T) {
	clock := newClock()
	c := New(Options[[]byte]{
		TTL: 2 * time.Minute, MaxEntries: 32, MaxBytes: 10, Now: clock.Now,
		Size: func(v []byte) int64 { return int64(len(v)) }, Clone: slices.Clone[[]byte],
	})
	getValue(t, c, "oversized", make([]byte, 11))
	if _, ok := c.Peek("oversized"); ok || c.Stats().Bytes != 0 {
		t.Fatal("oversized value retained by nonloading snapshot")
	}
	getValue(t, c, "workspace-a", []byte("123456"))
	getValue(t, c, "workspace-b", []byte("123456"))
	if _, ok := c.Peek("workspace-a"); ok {
		t.Fatal("peek bypassed aggregate budget eviction")
	}
	got, ok := c.Peek("workspace-b")
	if !ok {
		t.Fatal("retained value unavailable")
	}
	got[0] = 'x'
	other, _ := c.Peek("workspace-b")
	if string(other) != "123456" {
		t.Fatal("peek leaked mutable cache storage")
	}
	c.Invalidate("workspace-b")
	if _, ok := c.Peek("workspace-b"); ok {
		t.Fatal("peek returned invalidated value")
	}
	getValue(t, c, "workspace-c", []byte("123456"))
	clock.Advance(2 * time.Minute)
	c.Peek("missing")
	if len(c.entries) != 0 || c.bytes != 0 || c.lru.Len() != 0 || len(c.expiry) != 0 {
		t.Fatal("nonloading peek retained expired references")
	}
	checkAccounting(t, c)
}

func TestPerKeyPolicyDoesNotSplitBudgets(t *testing.T) {
	clock := newClock()
	c := New(Options[string]{
		MaxEntries: 32, MaxBytes: 8, Now: clock.Now, Size: func(v string) int64 { return int64(len(v)) },
	})
	load := func(context.Context) (string, error) { return "12345", nil }
	for _, key := range []string{"Notebook/builtin", "Environment/resources", "Lakehouse/Files"} {
		if _, err := c.GetWithTTL(context.Background(), key, time.Minute, load); err != nil {
			t.Fatal(err)
		}
		if stats := c.Stats(); stats.Bytes > 8 || stats.Entries != 1 {
			t.Fatal("type/surface policies split capacity", stats)
		}
		checkAccounting(t, c)
	}
	if _, err := c.GetWithTTL(context.Background(), "bad", -time.Second, load); err == nil {
		t.Fatal("negative policy TTL accepted")
	}
}
