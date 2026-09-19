package onelake

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"fabric-workspace-fs/internal/fserrors"
)

type uploadFixture struct {
	t                   *testing.T
	mu                  sync.Mutex
	target              Path
	oldETag             string
	oldData             string
	chunkSize           int
	stage               string
	marker              string
	stageExists         bool
	stageETag           string
	data                []byte
	trace               []string
	failAt              string
	failStatus          int
	failureCount        int
	cleanupFails        bool
	changeOwnership     bool
	commitBeforeFailure bool
	cancelAfterAppend   context.CancelFunc
}

func (f *uploadFixture) fail(w http.ResponseWriter, operation string) bool {
	if f.failAt != operation {
		return false
	}
	f.failureCount++
	status := f.failStatus
	if status == 0 {
		status = http.StatusServiceUnavailable
	}
	code := "ServerBusy"
	if status == 412 {
		code = "ConditionNotMet"
	}
	replyError(w, status, code)
	return true
}

func (f *uploadFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	query := r.URL.Query()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		f.t.Errorf("read request: %v", err)
	}
	if r.Method == http.MethodPut && query.Get("resource") == "file" {
		f.trace = append(f.trace, "create")
		requireRequest(f.t, r, "PUT", r.URL.EscapedPath(), url.Values{"resource": {"file"}})
		requireHeader(f.t, r, "If-None-Match", "*")
		requireHeader(f.t, r, "If-Match", "")
		wantPrefix := testPrefix + f.target.Relative[:strings.LastIndexByte(f.target.Relative, '/')+1]
		if !strings.HasPrefix(r.URL.Path, wantPrefix) {
			f.t.Errorf("stage %q is not a sibling of %q", r.URL.Path, f.target.Relative)
		}
		base := r.URL.Path[strings.LastIndexByte(r.URL.Path, '/')+1:]
		if !regexp.MustCompile(`^\.fabric-fs-upload-[a-f0-9]{32}$`).MatchString(base) {
			f.t.Errorf("stage does not have a cryptographic name: %q", base)
		}
		f.stage = r.URL.Path
		f.marker = strings.TrimPrefix(r.Header.Get("x-ms-properties"), stageProperty+"=")
		decoded, err := base64.StdEncoding.DecodeString(f.marker)
		if err != nil || len(decoded) != 32 {
			f.t.Errorf("invalid staging ownership marker")
		}
		for _, value := range decoded {
			if !strings.ContainsRune("0123456789abcdef", rune(value)) {
				f.t.Error("ownership marker is not printable hexadecimal text")
			}
		}
		if len(body) != 0 || r.ContentLength != 0 {
			f.t.Errorf("create had a body")
		}
		if f.failAt == "create-conflict" {
			f.trace = append(f.trace, "collision")
			replyError(w, 412, "ConditionNotMet")
			return
		}
		f.stageExists = true
		f.stageETag = `"created"`
		if f.fail(w, "create") {
			return
		}
		w.Header().Set("ETag", f.stageETag)
		w.WriteHeader(http.StatusCreated)
		return
	}
	if r.URL.Path == f.stage {
		switch r.Method {
		case http.MethodPatch:
			switch query.Get("action") {
			case "append":
				position := len(f.data)
				f.trace = append(f.trace, "append:"+strconv.Itoa(position))
				requireRequest(f.t, r, "PATCH", r.URL.EscapedPath(), url.Values{
					"action": {"append"}, "position": {strconv.Itoa(position)},
				})
				requireHeader(f.t, r, "If-Match", "")
				requireHeader(f.t, r, "If-None-Match", "")
				requireHeader(f.t, r, "Content-Type", "application/octet-stream")
				if len(body) == 0 || len(body) > f.chunkSize || r.ContentLength != int64(len(body)) {
					f.t.Errorf("append body length %d, content length %d, chunk size %d", len(body), r.ContentLength, f.chunkSize)
				}
				if f.fail(w, "append") {
					return
				}
				f.data = append(f.data, body...)
				if f.cancelAfterAppend != nil {
					f.cancelAfterAppend()
				}
				w.WriteHeader(http.StatusAccepted)
			case "flush":
				f.trace = append(f.trace, "flush")
				requireRequest(f.t, r, "PATCH", r.URL.EscapedPath(), url.Values{
					"action": {"flush"}, "position": {strconv.Itoa(len(f.data))}, "close": {"true"},
				})
				requireHeader(f.t, r, "If-Match", `"created"`)
				if len(body) != 0 || r.ContentLength != 0 {
					f.t.Errorf("flush had nonzero content length")
				}
				if f.fail(w, "flush") {
					return
				}
				f.stageETag = `"flushed"`
				w.Header().Set("ETag", f.stageETag)
				w.WriteHeader(http.StatusOK)
			default:
				f.t.Errorf("unexpected staging action %q", query.Get("action"))
				replyError(w, 400, "UnexpectedAction")
			}
		case http.MethodHead:
			conditional := r.Header.Get("If-Match")
			label := "head-stage"
			if conditional == "" {
				label = "cleanup-head"
			}
			f.trace = append(f.trace, label)
			requireRequest(f.t, r, "HEAD", r.URL.EscapedPath(), nil)
			if !f.stageExists {
				replyError(w, 404, "PathNotFound")
				return
			}
			if conditional != "" && conditional != f.stageETag {
				f.t.Errorf("stage HEAD condition %q != %q", conditional, f.stageETag)
			}
			if f.fail(w, label) {
				return
			}
			marker := f.marker
			if f.changeOwnership {
				marker = "different-owner"
			}
			w.Header().Set("x-ms-properties", stageProperty+"="+marker)
			size := int64(len(f.data))
			if f.failAt == "wrong-size" && label == "head-stage" {
				size++
			}
			replyInfo(w, false, size, f.stageETag)
		case http.MethodDelete:
			f.trace = append(f.trace, "cleanup-delete")
			requireRequest(f.t, r, "DELETE", r.URL.EscapedPath(), nil)
			requireHeader(f.t, r, "If-Match", f.stageETag)
			if !f.stageExists {
				f.t.Error("deleting a missing stage")
			}
			if f.cleanupFails {
				replyError(w, 503, "ServerBusy")
				return
			}
			f.stageExists = false
			w.WriteHeader(http.StatusOK)
		default:
			f.t.Errorf("unexpected stage request %s %s", r.Method, r.URL)
			replyError(w, 400, "UnexpectedRequest")
		}
		return
	}
	if r.URL.Path != testPrefix+f.target.Relative {
		f.t.Errorf("unexpected upload target %s", r.URL)
		replyError(w, 400, "UnexpectedTarget")
		return
	}
	switch r.Method {
	case http.MethodHead:
		label := "head-destination"
		if r.Header.Get("If-Match") == `"committed"` {
			label = "verify"
		}
		f.trace = append(f.trace, label)
		requireRequest(f.t, r, "HEAD", r.URL.EscapedPath(), nil)
		requireHeader(f.t, r, "If-Match", f.oldETag)
		if f.fail(w, label) {
			return
		}
		if f.oldETag == "" {
			replyError(w, 404, "PathNotFound")
			return
		}
		replyInfo(w, false, int64(len(f.oldData)), f.oldETag)
	case http.MethodPut:
		f.trace = append(f.trace, "rename")
		requireRequest(f.t, r, "PUT", r.URL.EscapedPath(), url.Values{"mode": {"posix"}})
		requireHeader(f.t, r, "x-ms-source-if-match", `"flushed"`)
		source, err := url.PathUnescape(r.Header.Get("x-ms-rename-source"))
		if err != nil || source != f.stage {
			f.t.Errorf("rename source %q != owned staging path %q", source, f.stage)
		}
		if f.oldETag == "" {
			requireHeader(f.t, r, "If-None-Match", "*")
			requireHeader(f.t, r, "If-Match", "")
		} else {
			requireHeader(f.t, r, "If-Match", f.oldETag)
			requireHeader(f.t, r, "If-None-Match", "")
		}
		if values, ok := r.Header["X-Ms-Properties"]; !ok || len(values) != 1 || values[0] != "" {
			f.t.Errorf("rename must clear private staging metadata: %v", values)
		}
		if f.commitBeforeFailure {
			f.stageExists = false
			f.oldData, f.oldETag = string(f.data), `"committed"`
		}
		if f.fail(w, "rename") {
			return
		}
		f.stageExists = false
		f.oldData, f.oldETag = string(f.data), `"committed"`
		if f.failAt != "missing-rename-etag" {
			w.Header().Set("ETag", f.oldETag)
		}
		w.WriteHeader(http.StatusCreated)
	case http.MethodDelete:
		f.t.Error("upload attempted to delete the destination")
		replyError(w, 400, "UnsafeDelete")
	default:
		f.t.Errorf("unexpected upload request %s", r.Method)
		replyError(w, 400, "UnexpectedRequest")
	}
}

type boundedSpool struct {
	t        *testing.T
	source   *strings.Reader
	maxChunk int
	offsets  []int64
}

func (s *boundedSpool) ReadAt(p []byte, offset int64) (int, error) {
	if len(p) > s.maxChunk {
		s.t.Errorf("spool read allocated %d bytes, max %d", len(p), s.maxChunk)
	}
	s.offsets = append(s.offsets, offset)
	return s.source.ReadAt(p, offset)
}

func TestPutChunkedConditionalCommit(t *testing.T) {
	for _, oldETag := range []string{"", `"old"`} {
		for _, data := range []string{"", "a", "abcdefghij"} {
			t.Run(fmt.Sprintf("overwrite=%v/size=%d", oldETag != "", len(data)), func(t *testing.T) {
				fixture := &uploadFixture{
					t: t, target: testPath("Files/sub %2e%2e/雪#?.txt"), oldETag: oldETag,
					oldData: "old destination", chunkSize: 4,
				}
				c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
				spool := &boundedSpool{t: t, source: strings.NewReader(data), maxChunk: 4}
				info, err := c.Put(context.Background(), fixture.target, spool, int64(len(data)), oldETag)
				if err != nil {
					t.Fatal(err)
				}
				if info.Path != fixture.target.Relative || info.Size != int64(len(data)) || info.IsDir || info.ETag != `"committed"` ||
					info.ModTime.Format(http.TimeFormat) != testDate {
					t.Errorf("committed info = %+v", info)
				}
				fixture.mu.Lock()
				defer fixture.mu.Unlock()
				if fixture.stageExists || fixture.oldData != data {
					t.Errorf("stage remains=%v, destination=%q", fixture.stageExists, fixture.oldData)
				}
				wantTrace := []string{"create"}
				for position := 0; position < len(data); position += 4 {
					wantTrace = append(wantTrace, "append:"+strconv.Itoa(position))
				}
				wantTrace = append(wantTrace, "flush", "head-stage")
				if oldETag != "" {
					wantTrace = append(wantTrace, "head-destination")
				}
				wantTrace = append(wantTrace, "rename", "verify")
				if !reflect.DeepEqual(fixture.trace, wantTrace) {
					t.Errorf("trace = %v, want %v", fixture.trace, wantTrace)
				}
			})
		}
	}
}

func TestPutZeroByteNilSpool(t *testing.T) {
	fixture := &uploadFixture{t: t, target: testPath("Files/zero"), chunkSize: 4}
	c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
	info, err := c.Put(context.Background(), fixture.target, nil, 0, "")
	if err != nil || info.Size != 0 || info.ETag != `"committed"` {
		t.Fatalf("zero Put = %+v, %v", info, err)
	}
}

func TestPutFailureCleansOnlyOwnedStageAndNeverRetriesWrites(t *testing.T) {
	for _, at := range []string{"create", "append", "flush", "wrong-size", "head-destination", "rename", "missing-rename-etag", "verify"} {
		t.Run(at, func(t *testing.T) {
			fixture := &uploadFixture{
				t: t, target: testPath("Files/a"), oldETag: `"old"`, oldData: "old",
				chunkSize: 4, failAt: at, failStatus: 412,
			}
			if at == "create" || at == "append" || at == "flush" {
				fixture.failStatus = 503
			}
			c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
			_, err := c.Put(context.Background(), fixture.target, strings.NewReader("new content"), 11, `"old"`)
			if err == nil {
				t.Fatal("failed upload reported success")
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.stageExists {
				t.Errorf("stage leaked after %s: %v", at, fixture.trace)
			}
			committed := at == "verify" || at == "missing-rename-etag"
			wantData := "old"
			if committed {
				wantData = "new content"
			}
			if fixture.oldData != wantData {
				t.Errorf("destination = %q, want %q", fixture.oldData, wantData)
			}
			if fixture.failureCount > 1 && (at == "create" || at == "append" || at == "flush" || at == "rename") {
				t.Errorf("unsafe write retried %d times", fixture.failureCount)
			}
			if at == "rename" {
				if !errors.Is(err, fserrors.ErrConflict) {
					t.Errorf("rename 412 = %v", err)
				}
				requireHTTPError(t, err, 412)
			}
		})
	}
}

func TestPutAmbiguousRenameNeverReportsSuccessOrDeletesDestination(t *testing.T) {
	fixture := &uploadFixture{
		t: t, target: testPath("Files/a"), oldETag: `"old"`, oldData: "old",
		chunkSize: 4, failAt: "rename", failStatus: 503, commitBeforeFailure: true,
	}
	c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
	_, err := c.Put(context.Background(), fixture.target, strings.NewReader("new"), 3, `"old"`)
	requireHTTPError(t, err, 503)
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.failureCount != 1 || fixture.oldData != "new" || fixture.stageExists {
		t.Errorf("ambiguous commit state: %+v", fixture)
	}
	for _, step := range fixture.trace {
		if step == "cleanup-delete" || step == "verify" {
			t.Errorf("unsafe recovery after ambiguous rename: %v", fixture.trace)
		}
	}
}

func TestPutCleanupFailureIsExplicit(t *testing.T) {
	for _, ownershipChanged := range []bool{false, true} {
		t.Run(fmt.Sprint(ownershipChanged), func(t *testing.T) {
			fixture := &uploadFixture{
				t: t, target: testPath("Files/a"), oldETag: `"old"`, oldData: "old",
				chunkSize: 4, failAt: "rename", failStatus: 412,
				cleanupFails: !ownershipChanged, changeOwnership: ownershipChanged,
			}
			c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
			_, err := c.Put(context.Background(), fixture.target, strings.NewReader("new"), 3, `"old"`)
			if !errors.Is(err, fserrors.ErrConflict) || !strings.Contains(err.Error(), "cleanup OneLake staging file") {
				t.Fatalf("cleanup failure not preserved: %v", err)
			}
			requireHTTPError(t, err, 412)
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if !fixture.stageExists || fixture.oldData != "old" {
				t.Error("cleanup failure mutated the wrong objects")
			}
			deletes := 0
			for _, step := range fixture.trace {
				if step == "cleanup-delete" {
					deletes++
				}
			}
			if (ownershipChanged && deletes != 0) || (!ownershipChanged && deletes != 1) {
				t.Errorf("cleanup delete attempts = %d", deletes)
			}
		})
	}
}

func TestPutCreateConflictDoesNotDeleteUnownedFile(t *testing.T) {
	fixture := &uploadFixture{t: t, target: testPath("Files/a"), chunkSize: 4, failAt: "create-conflict"}
	c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
	_, err := c.Put(context.Background(), fixture.target, nil, 0, "")
	if !errors.Is(err, fserrors.ErrConflict) {
		t.Fatalf("create conflict = %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if !reflect.DeepEqual(fixture.trace, []string{"create", "collision"}) {
		t.Errorf("collision recovery touched an unowned object: %v", fixture.trace)
	}
}

type failingSpool struct{ err error }

func (s failingSpool) ReadAt([]byte, int64) (int, error) { return 0, s.err }

func TestPutShortAndFailedSpoolReads(t *testing.T) {
	underlying := errors.New("offline spool read error")
	for _, tt := range []struct {
		name   string
		source io.ReaderAt
		want   error
	}{
		{"short", strings.NewReader("ab"), io.ErrUnexpectedEOF},
		{"broken ReaderAt", failingSpool{}, io.ErrUnexpectedEOF},
		{"read failure", failingSpool{underlying}, underlying},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := &uploadFixture{t: t, target: testPath("Files/a"), chunkSize: 4}
			c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
			_, err := c.Put(context.Background(), fixture.target, tt.source, 8, "")
			if !errors.Is(err, tt.want) {
				t.Errorf("Put error = %v, want %v", err, tt.want)
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if fixture.stageExists || !reflect.DeepEqual(fixture.trace, []string{"create", "cleanup-head", "cleanup-delete"}) {
				t.Errorf("failed spool trace = %v, stage exists %v", fixture.trace, fixture.stageExists)
			}
		})
	}
}

func TestPutCancelledUploadStillCleansStage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fixture := &uploadFixture{t: t, target: testPath("Files/a"), chunkSize: 4, cancelAfterAppend: cancel}
	c := testClient(t, Options{ChunkSize: 4}, fixture.serve)
	_, err := c.Put(ctx, fixture.target, strings.NewReader("abcdefgh"), 8, "")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("cancelled upload = %v", err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if fixture.stageExists {
		t.Errorf("cancelled context prevented cleanup: %v", fixture.trace)
	}
}

func TestMkdirCreateNewAndNoWriteRetry(t *testing.T) {
	for _, status := range []int{201, 412, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			var calls atomic.Int32
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				requireRequest(t, r, "PUT", testPrefix+"Files/new%20%23%3F", url.Values{"resource": {"directory"}})
				requireHeader(t, r, "If-None-Match", "*")
				requireHeader(t, r, "If-Match", "")
				if status != 201 {
					replyError(w, status, "ConditionNotMet")
					return
				}
				w.WriteHeader(status)
			})
			err := c.Mkdir(context.Background(), testPath("Files/new #?"))
			if (status == 201) != (err == nil) || calls.Load() != 1 {
				t.Errorf("Mkdir = %v, calls = %d", err, calls.Load())
			}
			if status != 201 {
				requireHTTPError(t, err, status)
			}
		})
	}
}

func TestRemoveConditionalAndNonrecursive(t *testing.T) {
	for _, directory := range []bool{false, true} {
		for _, status := range []int{200, 202, 409, 412, 503} {
			t.Run(fmt.Sprintf("directory=%v/status=%d", directory, status), func(t *testing.T) {
				var heads, deletes atomic.Int32
				c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
					requireHeader(t, r, "If-Match", `"old"`)
					if r.Method == "HEAD" {
						heads.Add(1)
						requireRequest(t, r, "HEAD", testPrefix+"Files/a", nil)
						replyInfo(w, directory, 0, `"old"`)
						return
					}
					deletes.Add(1)
					query := make(url.Values)
					if directory {
						query.Set("recursive", "false")
					}
					requireRequest(t, r, "DELETE", testPrefix+"Files/a", query)
					if status >= 400 {
						code := "ConditionNotMet"
						if status == 409 {
							code = "DirectoryNotEmpty"
						}
						replyError(w, status, code)
						return
					}
					w.WriteHeader(status)
				})
				err := c.Remove(context.Background(), testPath("Files/a"), directory, `"old"`)
				if (status < 400) != (err == nil) || heads.Load() != 1 || deletes.Load() != 1 {
					t.Errorf("Remove = %v, HEAD=%d DELETE=%d", err, heads.Load(), deletes.Load())
				}
				if status >= 400 {
					requireHTTPError(t, err, status)
				}
				if status == 409 && !errors.Is(err, fserrors.ErrNotEmpty) {
					t.Errorf("nonempty error = %v", err)
				}
			})
		}
	}
}

func TestRemoveRejectsWrongResourceType(t *testing.T) {
	for _, actualDirectory := range []bool{false, true} {
		c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "HEAD" {
				t.Errorf("type mismatch triggered deletion")
			}
			replyInfo(w, actualDirectory, 0, `"old"`)
		})
		err := c.Remove(context.Background(), testPath("Files/a"), !actualDirectory, `"old"`)
		want := fserrors.ErrNotDir
		if actualDirectory {
			want = fserrors.ErrIsDir
		}
		if !errors.Is(err, want) {
			t.Errorf("Remove = %v, want %v", err, want)
		}
	}
}

func TestDeleteDoesNotFollowUnexpectedContinuation(t *testing.T) {
	var deletes atomic.Int32
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "HEAD" {
			replyInfo(w, true, 0, `"old"`)
			return
		}
		deletes.Add(1)
		requireRequest(t, r, "DELETE", testPrefix+"Files/a", url.Values{"recursive": {"false"}})
		w.Header().Set("x-ms-continuation", "must-not-follow")
		w.WriteHeader(202)
	})
	err := c.Remove(context.Background(), testPath("Files/a"), true, `"old"`)
	if !errors.Is(err, fserrors.ErrUnsupported) || deletes.Load() != 1 {
		t.Errorf("Remove = %v, DELETE requests = %d", err, deletes.Load())
	}
}

func TestRenameRejectsCrossDeviceAndDescendant(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		replyError(w, 500, "UnexpectedRequest")
	})
	for _, field := range []string{"workspace", "item"} {
		p := testPath("Files/b")
		if field == "workspace" {
			p.Workspace = otherID
		} else {
			p.Item = otherID
		}
		if _, err := c.Rename(context.Background(), testPath("Files/a"), p, `"old"`, "", false); !errors.Is(err, fserrors.ErrCrossDevice) {
			t.Errorf("cross-%s Rename = %v", field, err)
		}
	}
	if _, err := c.Rename(context.Background(), testPath("Files/a"), testPath("Files/a/b"), `"old"`, "", false); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("descendant Rename = %v", err)
	}
	if calls.Load() != 0 {
		t.Errorf("unsafe rename reached network %d times", calls.Load())
	}
}

func TestRenameExactConditionalRequests(t *testing.T) {
	for _, destinationETag := range []string{"", `"destination"`} {
		for _, noReplace := range []bool{false, true} {
			t.Run(fmt.Sprintf("existing=%v/noReplace=%v", destinationETag != "", noReplace), func(t *testing.T) {
				var puts atomic.Int32
				source := testPath("Files/雪 %2e%2e#?")
				destination := testPath("Files/new %2f#?")
				c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
					switch {
					case r.Method == "HEAD" && r.URL.Path == testPrefix+source.Relative:
						requireRequest(t, r, "HEAD", testPrefix+"Files/%E9%9B%AA%20%252e%252e%23%3F", nil)
						requireHeader(t, r, "If-Match", `"source"`)
						replyInfo(w, false, 17, `"source"`)
					case r.Method == "HEAD":
						requireRequest(t, r, "HEAD", testPrefix+"Files/new%20%252f%23%3F", nil)
						if puts.Load() == 0 {
							if noReplace || destinationETag == "" {
								t.Error("unexpected destination preflight for absent-only rename")
							}
							requireHeader(t, r, "If-Match", destinationETag)
							replyInfo(w, false, 3, destinationETag)
						} else {
							requireHeader(t, r, "If-Match", `"renamed"`)
							replyInfo(w, false, 17, `"renamed"`)
						}
					case r.Method == "PUT":
						puts.Add(1)
						requireRequest(t, r, "PUT", testPrefix+"Files/new%20%252f%23%3F", url.Values{"mode": {"posix"}})
						requireHeader(t, r, "x-ms-rename-source", testPrefix+"Files/%E9%9B%AA%20%252e%252e%23%3F")
						requireHeader(t, r, "x-ms-source-if-match", `"source"`)
						if destinationETag == "" || noReplace {
							requireHeader(t, r, "If-None-Match", "*")
							requireHeader(t, r, "If-Match", "")
						} else {
							requireHeader(t, r, "If-Match", destinationETag)
							requireHeader(t, r, "If-None-Match", "")
						}
						if _, exists := r.Header["X-Ms-Properties"]; exists {
							t.Error("ordinary rename must preserve file properties")
						}
						w.Header().Set("ETag", `"renamed"`)
						w.WriteHeader(201)
					default:
						t.Errorf("unexpected Rename request %s", r.Method)
						replyError(w, 400, "UnexpectedRequest")
					}
				})
				info, err := c.Rename(context.Background(), source, destination, `"source"`, destinationETag, noReplace)
				if err != nil || info.Path != destination.Relative || info.Size != 17 || info.ETag != `"renamed"` || puts.Load() != 1 {
					t.Errorf("Rename = %+v, %v, PUTs=%d", info, err, puts.Load())
				}
			})
		}
	}
}

func TestRenameDirectoryReplacementIsNeverEmulated(t *testing.T) {
	for _, tt := range []struct {
		sourceDir bool
		targetDir bool
		want      error
	}{
		{true, true, fserrors.ErrUnsupported},
		{true, false, fserrors.ErrNotDir},
		{false, true, fserrors.ErrIsDir},
	} {
		t.Run(fmt.Sprintf("%v-%v", tt.sourceDir, tt.targetDir), func(t *testing.T) {
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "HEAD" {
					t.Errorf("unsafe directory replacement: %s", r.Method)
				}
				if r.URL.Path == testPrefix+"Files/source" {
					replyInfo(w, tt.sourceDir, 0, `"source"`)
				} else {
					replyInfo(w, tt.targetDir, 0, `"target"`)
				}
			})
			_, err := c.Rename(context.Background(), testPath("Files/source"), testPath("Files/target"), `"source"`, `"target"`, false)
			if !errors.Is(err, tt.want) {
				t.Errorf("Rename = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestDirectoryRenameContinuation(t *testing.T) {
	for _, mode := range []string{"complete", "max-pages", "cycle", "second-request-failure"} {
		t.Run(mode, func(t *testing.T) {
			var puts, verifies atomic.Int32
			tokens := []string{"a+b/%?=", "another+token"}
			opts := Options{MaxPages: 5}
			if mode == "max-pages" {
				opts.MaxPages = 1
			}
			c := testClient(t, opts, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == "HEAD" {
					if r.URL.Path == testPrefix+"Files/source" {
						requireHeader(t, r, "If-Match", `"source"`)
						replyInfo(w, true, 0, `"source"`)
					} else {
						verifies.Add(1)
						requireHeader(t, r, "If-Match", `"finished"`)
						replyInfo(w, true, 0, `"finished"`)
					}
					return
				}
				page := int(puts.Add(1))
				query := url.Values{"mode": {"posix"}}
				if page > 1 {
					query.Set("continuation", tokens[page-2])
				}
				requireRequest(t, r, "PUT", testPrefix+"Files/target", query)
				requireHeader(t, r, "If-None-Match", "*")
				requireHeader(t, r, "x-ms-source-if-match", `"source"`)
				requireHeader(t, r, "x-ms-rename-source", testPrefix+"Files/source")
				if mode == "second-request-failure" && page == 2 {
					replyError(w, 503, "ServerBusy")
					return
				}
				if page < 3 {
					token := tokens[page-1]
					if mode == "cycle" {
						token = tokens[0]
					}
					w.Header().Set("x-ms-continuation", token)
				} else {
					w.Header().Set("ETag", `"finished"`)
				}
				w.WriteHeader(201)
			})
			info, err := c.Rename(context.Background(), testPath("Files/source"), testPath("Files/target"), `"source"`, "", false)
			if mode == "complete" {
				if err != nil || !info.IsDir || info.Path != "Files/target" || puts.Load() != 3 || verifies.Load() != 1 {
					t.Fatalf("complete Rename = %+v, %v; PUT=%d HEAD=%d", info, err, puts.Load(), verifies.Load())
				}
			} else {
				if err == nil || verifies.Load() != 0 || info != (Info{}) {
					t.Fatalf("incomplete rename reported completion: %+v %v", info, err)
				}
				if mode == "max-pages" && (!errors.Is(err, fserrors.ErrTooLarge) || puts.Load() != 1) {
					t.Errorf("page-bounded rename = %v, PUT=%d", err, puts.Load())
				}
				if mode == "second-request-failure" {
					requireHTTPError(t, err, 503)
					if puts.Load() != 2 {
						t.Errorf("continuation write retried: PUT=%d", puts.Load())
					}
				}
			}
		})
	}
}

func TestRenameSamePath(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		requireRequest(t, r, "HEAD", testPrefix+"Files/a", nil)
		requireHeader(t, r, "If-Match", `"source"`)
		replyInfo(w, false, 9, `"source"`)
	})
	p := testPath("Files/a")
	if _, err := c.Rename(context.Background(), p, p, `"source"`, "", true); !errors.Is(err, fs.ErrExist) {
		t.Errorf("no-replace same path = %v", err)
	}
	info, err := c.Rename(context.Background(), p, p, `"source"`, "", false)
	if err != nil || info.Size != 9 || calls.Load() != 1 {
		t.Errorf("same path Rename = %+v %v, calls=%d", info, err, calls.Load())
	}
}
