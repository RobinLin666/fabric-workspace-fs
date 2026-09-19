package mwc

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strings"
	"testing"
	"time"

	"fabric-workspace-fs/internal/transport"
)

func negativeDeadline(t *testing.T, err error) time.Time {
	t.Helper()
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("not an absence error: %v", err)
	}
	var source interface{ CacheDeadline() time.Time }
	if !errors.As(err, &source) || source.CacheDeadline().IsZero() {
		t.Fatalf("absence has no explicit source deadline: %v", err)
	}
	return source.CacheDeadline()
}

func TestLookupNegativeDeadlineUsesCachedParentObservation(t *testing.T) {
	w := newWire(t)
	c := policyClient(t, w, `{}`)
	ctx := context.Background()
	if _, err := c.List(ctx, notebookPath("")); err != nil {
		t.Fatal(err)
	}
	w.advance(119 * time.Second)
	requests, tokens := len(w.recorded()), len(w.credentials.scopes)
	_, err := c.Stat(ctx, notebookPath("missing"))
	until := negativeDeadline(t, err)
	if !until.Equal(testNow.Add(2*time.Minute)) || until.Sub(w.clock()) != time.Second {
		t.Fatal("cached parent absence received a new lifetime", until)
	}
	if len(w.recorded()) != requests || len(w.credentials.scopes) != tokens+1 ||
		len(w.workspaceIDs) != 1 || hasHTTPError(err) {
		t.Fatal("absence metadata caused another discovery/request or lost successful-list proof")
	}
	w.advance(time.Second)
	w.mu.Lock()
	w.files["missing"] = []byte("created")
	w.mu.Unlock()
	info, err := c.Stat(ctx, notebookPath("missing"))
	if err != nil || info.Size != int64(len("created")) ||
		policyGETs(w, testNotebook, true) != 2 || policyGETs(w, testNotebook, false) != 0 {
		t.Fatal("expired parent absence hid the newly created file", info, err)
	}
}

func TestLookupNegativeDeadlineCapsBothListingAndAttributeAge(t *testing.T) {
	for _, operation := range []string{"stat", "list"} {
		t.Run(operation, func(t *testing.T) {
			w := newWire(t)
			attr, directory := "10s", "2s"
			if operation == "list" {
				attr, directory = "2s", "10s"
			}
			c := policyClient(t, w, fmt.Sprintf(`{"defaults":{"attr":%q,"directory":%q}}`, attr, directory))
			ctx := context.Background()
			if _, err := c.List(ctx, notebookPath("")); err != nil {
				t.Fatal(err)
			}
			for _, advance := range []time.Duration{time.Second, 2 * time.Second} {
				w.advance(advance)
				var err error
				if operation == "stat" {
					_, err = c.Stat(ctx, notebookPath("missing"))
				} else {
					_, err = c.List(ctx, notebookPath("missing"))
				}
				if until := negativeDeadline(t, err); !until.Equal(testNow.Add(2 * time.Second)) {
					t.Fatal("longer operation TTL extended an absence source", until)
				}
			}
			if policyGETs(w, testNotebook, true) != 1 {
				t.Fatal("deadline attachment reloaded the retained parent listing")
			}
		})
	}
}

func TestLookupNegativeZeroAttributeTTLIsExplicitlyDue(t *testing.T) {
	w := newWire(t)
	c := policyClient(t, w, `{"defaults":{"attr":"0s"}}`)
	for range 2 {
		_, err := c.Stat(context.Background(), notebookPath("missing"))
		if until := negativeDeadline(t, err); !until.Equal(w.clock()) {
			t.Fatal("zero attribute TTL granted negative cache time", until)
		}
		w.advance(time.Second)
	}
	if policyGETs(w, testNotebook, true) != 2 || c.listings.Stats().Entries != 1 {
		t.Fatal("zero-age lookup retained absence or disabled independent directory retention")
	}
}

func TestLookupFreshHTTP404CarriesBoundedNegativeDeadline(t *testing.T) {
	w := newWire(t)
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if r.URL.Query().Get("recursive") == "false" {
			w.advance(time.Second)
			return response(http.StatusNotFound, nil), nil, true
		}
		return nil, nil, false
	}
	c := policyClient(t, w, `{"defaults":{"attr":"10s","directory":"2s"}}`)
	for range 2 {
		_, err := c.Stat(context.Background(), notebookPath(""))
		if until := negativeDeadline(t, err); !until.Equal(w.clock().Add(2 * time.Second)) {
			t.Fatal("fresh 404 omitted its source age cap", until)
		}
		var remote *transport.HTTPError
		if !errors.As(err, &remote) || remote.StatusCode != http.StatusNotFound || !hasHTTPError(err) {
			t.Fatal("deadline wrapping removed HTTP identity or weakened mutation guards", err)
		}
	}
	if policyGETs(w, testNotebook, true) != 2 || c.listings.Stats().Entries != 0 {
		t.Fatal("HTTP absence errors were retained as directory snapshots")
	}
}

func TestLookupHeader404CannotExtendParentSourceDeadline(t *testing.T) {
	w := newWire(t)
	w.omitSizes = true
	w.files["file"] = []byte("old")
	w.hook = func(r *http.Request, _ []byte) (*http.Response, error, bool) {
		if strings.HasSuffix(r.URL.Path, "/workdir/file") && r.URL.RawQuery == "" {
			return response(http.StatusNotFound, nil), nil, true
		}
		return nil, nil, false
	}
	c := policyClient(t, w, `{}`)
	ctx := context.Background()
	if _, err := c.Stat(ctx, notebookPath("")); err != nil {
		t.Fatal(err)
	}
	w.advance(119 * time.Second)
	_, err := c.Stat(ctx, notebookPath("file"))
	if until := negativeDeadline(t, err); until.Sub(w.clock()) != time.Second {
		t.Fatal("header absence extended its cached parent observation", until)
	}
	if policyGETs(w, testNotebook, true) != 1 || policyGETs(w, testNotebook, false) != 1 ||
		c.metadata.Stats().Entries != 0 || c.contents.Stats().Entries != 0 {
		t.Fatal("metadata absence performed extra loads or retained a failed response")
	}
}

func TestLookupUnprovenAbsenceExpiresImmediately(t *testing.T) {
	for _, operation := range []string{"stat", "list"} {
		t.Run(operation, func(t *testing.T) {
			w := newWire(t)
			w.credentials.err = fs.ErrNotExist
			c := policyClient(t, w, `{}`)
			var err error
			if operation == "stat" {
				_, err = c.Stat(context.Background(), notebookPath("missing"))
			} else {
				_, err = c.List(context.Background(), notebookPath("missing"))
			}
			if until := negativeDeadline(t, err); !until.Equal(w.clock()) {
				t.Fatal("unproven absence was granted kernel cache time", until)
			}
			if len(w.recorded()) != 0 || len(w.credentials.scopes) != 1 || len(w.workspaceIDs) != 0 {
				t.Fatal("deadline attachment retried authentication or resource routing")
			}
		})
	}
}

func TestLookupFailurePreservesExistingDeadlineAndOtherErrors(t *testing.T) {
	w := newWire(t)
	c := w.client()
	until := testNow.Add(time.Second)
	original := &lookupError{cause: fs.ErrNotExist, validUntil: until}
	w.advance(time.Minute)
	err := c.lookupFailure(fmt.Errorf("derived lookup: %w", original), time.Hour, time.Time{})
	if got := negativeDeadline(t, err); !got.Equal(until) {
		t.Fatal("wrapping an old absence reset its deadline", got)
	}
	if err := c.lookupFailure(fs.ErrPermission, time.Hour, time.Time{}); err != fs.ErrPermission {
		t.Fatal("non-absence errors were changed", err)
	}
}
