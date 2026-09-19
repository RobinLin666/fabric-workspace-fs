package onelake

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"fabric-workspace-fs/internal/fserrors"
	"fabric-workspace-fs/internal/transport"
)

const (
	workspaceID = "11111111-1111-4111-8111-111111111111"
	itemID      = "22222222-2222-4222-8222-222222222222"
	otherID     = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	testDate    = "Fri, 18 Sep 2026 06:00:00 GMT"
	testPrefix  = "/" + workspaceID + "/" + itemID + "/"
)

type offlineTokens struct{ t *testing.T }

func (s offlineTokens) Token(_ context.Context, scope string) (string, error) {
	if scope != Scope {
		s.t.Errorf("token scope = %q, want storage audience %q", scope, Scope)
	}
	return "offline-storage-token", nil
}

func testPath(relative string) Path {
	return Path{Workspace: workspaceID, Item: itemID, Relative: relative}
}

func testClient(t *testing.T, opts Options, handler http.HandlerFunc) *Client {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer offline-storage-token" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("x-ms-version"); got != serviceVersion {
			t.Errorf("x-ms-version = %q", got)
		}
		for _, name := range []string{"x-ms-acl", "x-ms-permissions", "x-ms-owner", "x-ms-group", "x-ms-access-tier"} {
			if r.Header.Get(name) != "" {
				t.Errorf("unsupported management header %s", name)
			}
		}
		handler(w, r)
	}))
	t.Cleanup(server.Close)
	tc, err := transport.New(transport.Options{
		BaseURL: server.URL, Scope: Scope, Tokens: offlineTokens{t},
		HTTPClient: server.Client(), MaxRetries: 3,
		RetryDelay: time.Millisecond, MaxRetryDelay: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return New(tc, opts)
}

func requireRequest(t *testing.T, r *http.Request, method, path string, query url.Values) {
	t.Helper()
	if r.Method != method || r.URL.EscapedPath() != path {
		t.Errorf("request = %s %s, want %s %s", r.Method, r.URL.EscapedPath(), method, path)
	}
	if query == nil {
		query = make(url.Values)
	}
	if !reflect.DeepEqual(r.URL.Query(), query) {
		t.Errorf("query = %v, want %v", r.URL.Query(), query)
	}
	if r.URL.RawQuery != query.Encode() {
		t.Errorf("raw query = %q, want encoded once %q", r.URL.RawQuery, query.Encode())
	}
}

func requireHeader(t *testing.T, r *http.Request, name, value string) {
	t.Helper()
	if got := r.Header.Get(name); got != value {
		t.Errorf("%s = %q, want %q", name, got, value)
	}
}

func replyInfo(w http.ResponseWriter, directory bool, size int64, etag string) {
	kind := "file"
	if directory {
		kind = "directory"
	}
	w.Header().Set("x-ms-resource-type", kind)
	w.Header().Set("ETag", etag)
	w.Header().Set("Content-Length", fmt.Sprint(size))
	w.Header().Set("Last-Modified", testDate)
	w.WriteHeader(http.StatusOK)
}

func replyError(w http.ResponseWriter, status int, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("x-ms-request-id", "offline-request-id")
	w.Header().Set("x-ms-error-code", code)
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{
		"code": code, "message": "offline failure",
	}})
}

func requireHTTPError(t *testing.T, err error, status int) {
	t.Helper()
	var he *transport.HTTPError
	if !errors.As(err, &he) {
		t.Fatalf("error %v does not retain HTTPError", err)
	}
	if he.StatusCode != status || he.RequestID != "offline-request-id" {
		t.Errorf("HTTPError = %+v, want status %d and request ID", he, status)
	}
}

func TestValidatePath(t *testing.T) {
	tests := []struct {
		relative string
		write    bool
		want     error
	}{
		{"Files", false, nil}, {"Tables", false, nil},
		{"Files/a b/雪%#?.txt", true, nil},
		{"Files/%2e%2e/%2f/%5c/%00", true, nil},
		{"Tables/schema/table/part.parquet", false, nil},
		{"Files", true, fserrors.ErrReadOnly},
		{"Tables", true, fserrors.ErrReadOnly},
		{"Tables/table/a", true, fserrors.ErrReadOnly},
		{".platform", true, fserrors.ErrReadOnly},
		{"Metadata/a", true, fserrors.ErrReadOnly},
		{"", true, fserrors.ErrReadOnly},
		{"", false, fs.ErrInvalid},
		{"files/a", false, fs.ErrInvalid},
		{"Files2/a", false, fs.ErrInvalid},
		{"Files/", false, fs.ErrInvalid},
		{"/Files/a", true, fs.ErrInvalid},
		{"Files//a", true, fs.ErrInvalid},
		{"Files/.", true, fs.ErrInvalid},
		{"Files/../Tables/a", true, fs.ErrInvalid},
		{"Files/a/../../b", false, fs.ErrInvalid},
		{"Files/a\\b", true, fs.ErrInvalid},
		{"Files/a\x00b", true, fs.ErrInvalid},
		{"Files/a\nb", true, fs.ErrInvalid},
		{"Files/\xff", false, fs.ErrInvalid},
		{"Files/\ufffd", false, fs.ErrInvalid},
		{"Files/\ufffe", false, fs.ErrInvalid},
		{"Files/\u0085", false, fs.ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("%q/write=%v", tt.relative, tt.write), func(t *testing.T) {
			err := ValidatePath(testPath(tt.relative), tt.write)
			if !errors.Is(err, tt.want) {
				t.Errorf("ValidatePath = %v, want %v", err, tt.want)
			}
		})
	}
	for _, id := range []string{"", "workspace-name", strings.ReplaceAll(workspaceID, "-", ""), "{" + workspaceID + "}", workspaceID + ".Lakehouse", "z" + workspaceID[1:]} {
		for _, field := range []string{"workspace", "item"} {
			p := testPath("Files/a")
			if field == "workspace" {
				p.Workspace = id
			} else {
				p.Item = id
			}
			if err := ValidatePath(p, false); !errors.Is(err, fs.ErrInvalid) {
				t.Errorf("invalid %s %q accepted: %v", field, id, err)
			}
		}
	}
	p := testPath("Files/a")
	p.Workspace, p.Item = strings.ToUpper(otherID), strings.ToUpper(otherID)
	if err := ValidatePath(p, true); err != nil {
		t.Errorf("canonical uppercase UUIDs: %v", err)
	}
}

func TestEveryWriteProtectsManagedObjects(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		t.Errorf("protected write reached network: %s %s", r.Method, r.URL)
		replyError(w, 500, "UnexpectedRequest")
	})
	ctx := context.Background()
	for _, relative := range []string{"", "Files", "Tables", "Tables/a", ".platform", "Metadata/file"} {
		p := testPath(relative)
		_, putErr := c.Put(ctx, p, strings.NewReader("a"), 1, "")
		mkdirErr := c.Mkdir(ctx, p)
		removeErr := c.Remove(ctx, p, false, `"old"`)
		_, sourceErr := c.Rename(ctx, p, testPath("Files/new"), `"old"`, "", false)
		_, targetErr := c.Rename(ctx, testPath("Files/old"), p, `"old"`, "", false)
		for op, err := range map[string]error{"put": putErr, "mkdir": mkdirErr, "remove": removeErr, "rename-source": sourceErr, "rename-target": targetErr} {
			if !errors.Is(err, fserrors.ErrReadOnly) {
				t.Errorf("%s %q = %v, want ErrReadOnly", op, relative, err)
			}
		}
	}
	if calls.Load() != 0 {
		t.Errorf("%d unsafe requests", calls.Load())
	}
}

func TestStatUsesGUIDsAndSingleEncoding(t *testing.T) {
	p := testPath("Files/a b/雪%2e%2e#?.txt")
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		requireRequest(t, r, "HEAD", testPrefix+"Files/a%20b/%E9%9B%AA%252e%252e%23%3F.txt", nil)
		replyInfo(w, false, 9007199254740993, "0xABC")
	})
	info, err := c.Stat(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	if info.Path != p.Relative || info.IsDir || info.Size != 9007199254740993 || info.ETag != `"0xABC"` {
		t.Errorf("info = %+v", info)
	}
	if info.ModTime.Format(http.TimeFormat) != testDate {
		t.Errorf("modification time = %v", info.ModTime)
	}
}

func TestLiteralPercentPathComponents(t *testing.T) {
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		requireRequest(t, r, "HEAD", testPrefix+"Files/%252e%252e/%252f/%255c/%2500", nil)
		replyInfo(w, false, 0, `"literal"`)
	})
	if _, err := c.Stat(context.Background(), testPath("Files/%2e%2e/%2f/%5c/%00")); err != nil {
		t.Fatal(err)
	}
}

func TestStatErrorsRetainStatusAndRequestID(t *testing.T) {
	for _, status := range []int{400, 401, 403, 404, 409, 412, 429, 500, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				replyError(w, status, "OfflineError")
			})
			_, err := c.Stat(context.Background(), testPath("Files/a"))
			requireHTTPError(t, err, status)
			if status == 412 && !errors.Is(err, fserrors.ErrConflict) {
				t.Errorf("412 = %v, want ErrConflict", err)
			}
		})
	}
}

func TestStatRejectsInvalidMetadata(t *testing.T) {
	for _, tt := range []struct{ name, field, value string }{
		{"missing type", "x-ms-resource-type", ""},
		{"wrong type", "x-ms-resource-type", "blob"},
		{"missing ETag", "ETag", ""},
		{"weak ETag", "ETag", `W/"weak"`},
		{"wildcard ETag", "ETag", "*"},
		{"multiple ETags", "ETag", `"one", "two"`},
		{"date", "Last-Modified", "not-a-date"},
		{"size overflow", "Content-Length", "9223372036854775808"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("x-ms-resource-type", "file")
				w.Header().Set("ETag", `"etag"`)
				w.Header().Set("Content-Length", "0")
				w.Header().Set("Last-Modified", testDate)
				w.Header().Set(tt.field, tt.value)
				w.WriteHeader(200)
			})
			if _, err := c.Stat(context.Background(), testPath("Files/a")); err == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}

func TestListImmediateChildrenAndContinuation(t *testing.T) {
	var calls atomic.Int32
	token := `a+b/%25?=" &`
	parent := testPath("Files/a b%#?")
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		page := calls.Add(1)
		query := url.Values{
			"resource": {"filesystem"}, "directory": {itemID + "/" + parent.Relative},
			"recursive": {"false"}, "maxResults": {"5000"},
		}
		if page == 2 {
			query.Set("continuation", token)
		}
		requireRequest(t, r, "GET", "/"+workspaceID, query)
		requireHeader(t, r, "Accept", "application/json")
		name := itemID + "/" + parent.Relative
		if page == 1 {
			w.Header().Set("x-ms-continuation", token)
			_ = json.NewEncoder(w).Encode(map[string]any{"paths": []any{
				map[string]any{"name": name + "/雪%.txt", "isDirectory": "false", "contentLength": "9007199254740993", "etag": "0x123", "lastModified": testDate},
				map[string]any{"name": name + "/folder", "isDirectory": true, "etag": `"directory"`},
			}})
		} else {
			_ = json.NewEncoder(w).Encode(map[string]any{"paths": []any{
				map[string]any{"name": name + "/a", "isDirectory": false, "contentLength": 7, "etag": `"a"`, "lastModified": "2026-09-18T06:00:00Z"},
			}})
		}
	})
	infos, err := c.List(context.Background(), parent)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || len(infos) != 3 {
		t.Fatalf("requests = %d, result = %+v", calls.Load(), infos)
	}
	if infos[0].Path != parent.Relative+"/a" || infos[0].Size != 7 ||
		!infos[1].IsDir || infos[1].Size != 0 ||
		infos[2].Size != 9007199254740993 || infos[2].ETag != `"0x123"` {
		t.Errorf("infos = %+v", infos)
	}
}

func TestListRejectsForeignAndUnsafeReturnedNames(t *testing.T) {
	for _, name := range []string{
		otherID + "/Files/parent/a",
		itemID + "/Files/parent2/a",
		itemID + "/Files/parent",
		itemID + "/Tables/a",
		itemID + "/Files/parent/../a",
		itemID + "/Files/parent/.",
		itemID + "/Files/parent//a",
		itemID + "/Files/parent/folder/child",
		itemID + "/Files/parent/",
		itemID + "/Files/parent/a\\b",
		itemID + "/Files/parent/a\x00b",
		"/" + itemID + "/Files/parent/a",
		"https://foreign.example/" + itemID + "/Files/parent/a",
	} {
		t.Run(fmt.Sprintf("%q", name), func(t *testing.T) {
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{"paths": []any{
					map[string]any{"name": name, "contentLength": 0, "etag": `"e"`},
				}})
			})
			infos, err := c.List(context.Background(), testPath("Files/parent"))
			if !errors.Is(err, fs.ErrInvalid) || infos != nil {
				t.Fatalf("unsafe listing = %+v, %v", infos, err)
			}
		})
	}
}

func TestListBoundsAndMalformedResponses(t *testing.T) {
	valid := fmt.Sprintf(`{"paths":[{"name":%q,"contentLength":"1","etag":"e"}]}`, itemID+"/Files/a")
	for _, tt := range []struct {
		name  string
		body  string
		token string
		opts  Options
		want  error
	}{
		{"page cap", valid, "next", Options{MaxPages: 1}, fserrors.ErrTooLarge},
		{"token cycle", `{"paths":[]}`, "same-token", Options{MaxPages: 5}, nil},
		{"oversized token", `{"paths":[]}`, strings.Repeat("x", maxContinuation+1), Options{}, fserrors.ErrTooLarge},
		{"duplicate", strings.Replace(valid, `]}`, `,{"name":"`+itemID+`/Files/a","contentLength":1,"etag":"e"}]}`, 1), "", Options{}, fserrors.ErrConflict},
		{"oversized response", `{"paths":[],"extra":"` + strings.Repeat("x", maxListBytes) + `"}`, "", Options{}, fserrors.ErrTooLarge},
		{"bad json", `{`, "", Options{}, nil},
		{"trailing json", valid + `{}`, "", Options{}, nil},
		{"missing paths", `{}`, "", Options{}, nil},
		{"null paths", `{"paths":null}`, "", Options{}, nil},
		{"bad utf8", `{"paths":[],"extra":"` + "\xff" + `"}`, "", Options{}, nil},
		{"negative size", strings.Replace(valid, `"1"`, `"-1"`, 1), "", Options{}, fs.ErrInvalid},
		{"overflow size", strings.Replace(valid, `"1"`, `"9223372036854775808"`, 1), "", Options{}, fserrors.ErrTooLarge},
		{"fractional size", strings.Replace(valid, `"1"`, `1.5`, 1), "", Options{}, fs.ErrInvalid},
		{"null directory flag", strings.Replace(valid, `"contentLength"`, `"isDirectory":null,"contentLength"`, 1), "", Options{}, nil},
		{"surrogate name", strings.Replace(valid, "/Files/a", `/Files/\ud800`, 1), "", Options{}, fs.ErrInvalid},
		{"bad etag", strings.Replace(valid, `"etag":"e"`, `"etag":"*"`, 1), "", Options{}, fs.ErrInvalid},
		{"bad time", strings.Replace(valid, `"contentLength"`, `"lastModified":"wrong","contentLength"`, 1), "", Options{}, nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var calls atomic.Int32
			c := testClient(t, tt.opts, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tt.token != "" {
					w.Header().Set("x-ms-continuation", tt.token)
				}
				_, _ = io.WriteString(w, tt.body)
			})
			infos, err := c.List(context.Background(), testPath("Files"))
			if err == nil || infos != nil || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("List = %+v, %v, want error %v", infos, err, tt.want)
			}
			if tt.name == "token cycle" && calls.Load() != 2 {
				t.Errorf("cycle requests = %d, want 2", calls.Load())
			}
		})
	}
}

func TestReadRangeAndEOF(t *testing.T) {
	for _, tt := range []struct {
		name    string
		offset  int64
		length  int
		status  int
		body    string
		crange  string
		wantErr error
	}{
		{"range", 5, 4, 206, "fghi", "bytes 5-8/10", nil},
		{"EOF", 8, 4, 206, "ij", "bytes 8-9/10", io.EOF},
		{"exact end", 8, 2, 206, "ij", "bytes 8-9/10", nil},
		{"beyond end", 10, 2, 416, "", "", io.EOF},
		{"zero file", 0, 2, 200, "", "", io.EOF},
		{"full response", 0, 5, 200, "abc", "", io.EOF},
		{"bounded full response", 0, 3, 200, "abcdefghijkl", "", nil},
		{"short not EOF", 0, 4, 206, "ab", "bytes 0-1/20", io.ErrUnexpectedEOF},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				requireRequest(t, r, "GET", testPrefix+"Files/a%20b", nil)
				requireHeader(t, r, "Range", fmt.Sprintf("bytes=%d-%d", tt.offset, tt.offset+int64(tt.length)-1))
				requireHeader(t, r, "If-Match", `"read-tag"`)
				requireHeader(t, r, "Accept-Encoding", "identity")
				if tt.status == 416 {
					replyError(w, 416, "InvalidRange")
					return
				}
				w.Header().Set("ETag", `"read-tag"`)
				w.Header().Set("Content-Length", fmt.Sprint(len(tt.body)))
				if tt.crange != "" {
					w.Header().Set("Content-Range", tt.crange)
				}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			})
			buffer := make([]byte, tt.length)
			n, err := c.Read(context.Background(), testPath("Files/a b"), tt.offset, buffer, `"read-tag"`)
			wantN := min(tt.length, len(tt.body))
			if n != wantN || !errors.Is(err, tt.wantErr) || string(buffer[:n]) != tt.body[:wantN] {
				t.Errorf("Read = %d %q %v, want %d %q %v", n, buffer[:n], err, wantN, tt.body[:wantN], tt.wantErr)
			}
		})
	}
}

func TestReadRejectsInvalidRangesAndTruncation(t *testing.T) {
	for _, tt := range []struct {
		name      string
		offset    int64
		status    int
		crange    string
		length    string
		extraName string
		extra     string
		want      error
	}{
		{"ignored nonzero range", 1, 200, "", "2", "", "", nil},
		{"missing range", 0, 206, "", "2", "", "", nil},
		{"wrong start", 0, 206, "bytes 1-2/3", "2", "", "", nil},
		{"wrong end", 0, 206, "bytes 0-9/10", "2", "", "", nil},
		{"invalid total", 0, 206, "bytes 0-1/1", "2", "", "", nil},
		{"unknown total", 0, 206, "bytes 0-1/*", "2", "", "", nil},
		{"wrong length", 0, 206, "bytes 0-2/3", "2", "", "", nil},
		{"truncated", 0, 206, "bytes 0-2/3", "3", "", "", io.ErrUnexpectedEOF},
		{"compressed", 0, 206, "bytes 0-1/2", "2", "Content-Encoding", "gzip", nil},
		{"directory", 0, 206, "bytes 0-1/2", "2", "x-ms-resource-type", "directory", fserrors.ErrIsDir},
		{"etag mismatch", 0, 206, "bytes 0-1/2", "2", "ETag", `"different"`, fserrors.ErrConflict},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("ETag", `"expected"`)
				w.Header().Set("Content-Length", tt.length)
				w.Header().Set("Content-Range", tt.crange)
				if tt.extraName != "" {
					w.Header().Set(tt.extraName, tt.extra)
				}
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, "ab")
			})
			_, err := c.Read(context.Background(), testPath("Files/a"), tt.offset, make([]byte, 3), `"expected"`)
			if err == nil || (tt.want != nil && !errors.Is(err, tt.want)) {
				t.Fatalf("Read error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestReadConditionalFailure(t *testing.T) {
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		requireHeader(t, r, "If-Match", `"old"`)
		replyError(w, 412, "ConditionNotMet")
	})
	n, err := c.Read(context.Background(), testPath("Tables/a"), 0, make([]byte, 10), `"old"`)
	if n != 0 || !errors.Is(err, fserrors.ErrConflict) {
		t.Errorf("Read = %d, %v", n, err)
	}
	requireHTTPError(t, err, 412)
}

func TestInvalidInputsNeverReachNetwork(t *testing.T) {
	var calls atomic.Int32
	c := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		replyError(w, 500, "UnexpectedRequest")
	})
	ctx := context.Background()
	for _, tt := range []struct {
		offset int64
		dest   []byte
		etag   string
	}{
		{-1, nil, ""}, {math.MaxInt64, make([]byte, 2), ""},
		{0, make([]byte, 1), "*"}, {0, make([]byte, 1), `W/"weak"`},
		{0, make([]byte, 1), "a\r\nb"},
	} {
		if _, err := c.Read(ctx, testPath("Files/a"), tt.offset, tt.dest, tt.etag); !errors.Is(err, fs.ErrInvalid) {
			t.Errorf("Read invalid input = %v", err)
		}
	}
	if n, err := c.Read(ctx, testPath("Files/a"), math.MaxInt64, nil, ""); n != 0 || err != nil {
		t.Errorf("empty Read = %d, %v", n, err)
	}
	if _, err := c.Put(ctx, testPath("Files/a"), nil, 1, ""); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("nil spool = %v", err)
	}
	if _, err := c.Put(ctx, testPath("Files/a"), nil, -1, ""); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("negative size = %v", err)
	}
	if err := c.Remove(ctx, testPath("Files/a"), false, ""); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("missing delete ETag = %v", err)
	}
	if _, err := c.Rename(ctx, testPath("Files/a"), testPath("Files/b"), "", "", false); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("missing rename source ETag = %v", err)
	}
	if calls.Load() != 0 {
		t.Errorf("invalid inputs sent %d requests", calls.Load())
	}
}

func TestInvalidOptions(t *testing.T) {
	base := testClient(t, Options{}, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("invalid options sent request")
		replyError(w, 500, "UnexpectedRequest")
	})
	for _, opts := range []Options{{ChunkSize: -1}, {MaxPages: -1}, {ChunkSize: maxChunkSize + 1}} {
		c := New(base.http, opts)
		if _, err := c.Stat(context.Background(), testPath("Files/a")); err == nil {
			t.Errorf("invalid options %+v accepted", opts)
		}
	}
	if _, err := New(nil, Options{}).Stat(context.Background(), testPath("Files/a")); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("nil transport = %v", err)
	}
	var c *Client
	if _, err := c.Stat(context.Background(), testPath("Files/a")); !errors.Is(err, fs.ErrInvalid) {
		t.Errorf("nil client = %v", err)
	}
}
