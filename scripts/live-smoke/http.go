package main

import (
	"errors"
	"io/fs"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"

	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/transport"
)

type requestCounter struct {
	mu     sync.Mutex
	totals counts
}

func (c *requestCounter) add(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.totals == nil {
		c.totals = make(counts)
	}
	c.totals[key]++
}

func (c *requestCounter) snapshot() counts {
	c.mu.Lock()
	defer c.mu.Unlock()
	result := make(counts, len(c.totals))
	for key, value := range c.totals {
		result[key] = value
	}
	return result
}

func countDelta(before, after counts) counts {
	result := make(counts)
	for key, value := range after {
		if value > before[key] {
			result[key] = value - before[key]
		}
	}
	return result
}

func totalCounts(values counts) uint64 {
	var total uint64
	for _, count := range values {
		total += count
	}
	return total
}

// The allowlist is defense in depth: even a regression test cannot mutate an
// existing resource or anything outside the explicitly owned fixture scopes.
type scopeGuard struct {
	mu                    sync.Mutex
	workspace             string
	lakehouse             string
	directory             string
	notebook              string
	createSent            bool
	lakeOwned             bool
	readOnly              bool
	denied                uint64
	operations            map[string]bool
	plan                  fixturePlan
	managedAttempts       map[string]bool
	managedReceipts       map[string]managedResource
	managedVerified       map[string]managedResource
	managedDeletes        map[string]bool
	managedOperations     map[string]string
	managedNotebookUpdate bool
}

func newScopeGuard(opts options, plan fixturePlan) *scopeGuard {
	return &scopeGuard{
		workspace: opts.Workspace, lakehouse: opts.Lakehouse,
		directory: plan.LakeDirectory, operations: make(map[string]bool), plan: plan,
		managedAttempts: make(map[string]bool), managedReceipts: make(map[string]managedResource),
		managedVerified: make(map[string]managedResource), managedDeletes: make(map[string]bool),
		managedOperations: make(map[string]string),
	}
}

func (g *scopeGuard) setNotebook(id string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.notebook = id
}

func (g *scopeGuard) setReadOnly(value bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.readOnly = value
}

func (g *scopeGuard) state() (bool, uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.lakeOwned, g.denied
}

func (g *scopeGuard) authorize(req *http.Request) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	key, allowed := g.allowedLocked(req)
	if !allowed {
		g.denied++
		return "", fail("outbound scope guard rejected a request")
	}
	return key, nil
}

func (g *scopeGuard) allowedLocked(req *http.Request) (string, bool) {
	u := req.URL
	if u == nil || u.Scheme != "https" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return "", false
	}
	for _, part := range strings.Split(u.Path, "/") {
		if part == "." || part == ".." || strings.ContainsAny(part, "\\\x00\r\n") {
			return "", false
		}
	}
	switch u.Host {
	case "api.fabric.microsoft.com":
		base := "/v1/workspaces/" + g.workspace
		if req.Method == http.MethodGet {
			switch {
			case u.Path == base:
				return "fabric|GET|workspace", true
			case u.Path == base+"/items":
				return "fabric|GET|items-list", true
			case u.Path == base+"/folders":
				return "fabric|GET|folders-list", true
			case strings.HasPrefix(u.Path, base+"/items/") && fabric.ValidateID(strings.TrimPrefix(u.Path, base+"/items/")) == nil:
				return "fabric|GET|item", true
			case strings.HasPrefix(u.Path, base+"/folders/") && fabric.ValidateID(strings.TrimPrefix(u.Path, base+"/folders/")) == nil:
				return "fabric|GET|folder", true
			case strings.HasPrefix(u.Path, "/v1/operations/"):
				parts := strings.Split(strings.TrimPrefix(u.Path, "/v1/operations/"), "/")
				if len(parts) > 2 || fabric.ValidateID(parts[0]) != nil || !g.operations[strings.ToLower(parts[0])] {
					return "", false
				}
				if len(parts) == 1 {
					return "fabric|GET|operation-poll", true
				}
				if parts[1] == "result" {
					return "fabric|GET|operation-result", true
				}
			}
		}
		if key, allowed := g.allowCreateLocked(req); allowed {
			return key, true
		}
		if key, allowed := g.allowManagedRequestLocked(req); allowed {
			return key, true
		}
		if g.notebook != "" {
			item := base + "/notebooks/" + g.notebook
			if req.Method == http.MethodPost && u.Path == item+"/getDefinition" {
				return "fabric|POST|definition-read", true
			}
			if !g.readOnly && u.RawQuery == "" {
				if req.Method == http.MethodPost && u.Path == item+"/updateDefinition" {
					return "fabric|POST|definition-update", true
				}
				if req.Method == http.MethodDelete && u.Path == base+"/notebooks/"+g.notebook {
					return "fabric|DELETE|notebook-delete", true
				}
			}
		}
	case "onelake.dfs.fabric.microsoft.com":
		if key, allowed := g.allowManagedLakeReadLocked(req); allowed {
			return key, true
		}
		base := "/" + g.workspace + "/" + g.lakehouse + "/"
		query := u.Query()
		if req.Method == http.MethodGet && u.Path == "/"+g.workspace && query.Get("resource") == "filesystem" &&
			strings.HasPrefix(query.Get("directory"), g.lakehouse+"/") {
			relative := strings.TrimPrefix(query.Get("directory"), g.lakehouse+"/")
			if path.Clean(relative) == relative && !strings.ContainsAny(relative, "\\\x00\r\n") &&
				(relative == "Files" || (g.lakeOwned && below(g.directory, relative))) && query.Get("recursive") == "false" {
				return "onelake|GET|list", true
			}
		}
		if !strings.HasPrefix(u.Path, base) {
			return "", false
		}
		relative := strings.TrimPrefix(u.Path, base)
		if !below("Files", relative) && !below("Tables", relative) {
			return "", false
		}
		if req.Method == http.MethodHead {
			return "onelake|HEAD|stat", true
		}
		if req.Method == http.MethodGet {
			return "onelake|GET|read", g.lakeOwned && below(g.directory, relative)
		}
		if g.readOnly || !below(g.directory, relative) {
			return "", false
		}
		if req.Method == http.MethodPut && relative == g.directory && !g.lakeOwned &&
			query.Get("resource") == "directory" && req.Header.Get("If-None-Match") == "*" {
			return "onelake|PUT|directory-create", true
		}
		if !g.lakeOwned {
			return "", false
		}
		switch req.Method {
		case http.MethodPut:
			if source := req.Header.Get("x-ms-rename-source"); source != "" {
				decoded, err := url.PathUnescape(source)
				sourceETag := req.Header.Get("x-ms-source-if-match")
				destinationETag := req.Header.Get("If-Match")
				if err != nil || path.Clean(decoded) != decoded || strings.ContainsAny(decoded, "\\?#\x00\r\n") ||
					!below(base+g.directory, decoded) ||
					sourceETag == "" || sourceETag == "*" ||
					((destinationETag == "" || destinationETag == "*") && req.Header.Get("If-None-Match") != "*") {
					return "", false
				}
				return "onelake|PUT|rename", true
			}
			if req.Header.Get("If-None-Match") != "*" {
				return "", false
			}
			switch query.Get("resource") {
			case "directory":
				return "onelake|PUT|directory-create", true
			case "file":
				return "onelake|PUT|file-create", true
			}
		case http.MethodPatch:
			switch query.Get("action") {
			case "append":
				return "onelake|PATCH|append", true
			case "flush":
				return "onelake|PATCH|flush", true
			}
		case http.MethodDelete:
			recursive := query.Get("recursive")
			tag := req.Header.Get("If-Match")
			if (recursive == "" || recursive == "false") && tag != "" && tag != "*" {
				return "onelake|DELETE|nonrecursive-delete", true
			}
		}
	}
	return "", false
}

func (g *scopeGuard) observe(req *http.Request, resp *http.Response) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if req.URL.Host == "api.fabric.microsoft.com" {
		id := strings.ToLower(strings.TrimSpace(resp.Header.Get("x-ms-operation-id")))
		if fabric.ValidateID(id) == nil {
			g.operations[id] = true
			if req.Method == http.MethodPost {
				name, folder, parent, _, _, valid := createRequestIdentity(req)
				base := "/v1/workspaces/" + g.workspace
				if valid && req.URL.Path == base+"/folders" && name == g.plan.Managed.FolderName && parent == "" {
					g.managedOperations["Folder"] = id
				}
				for _, spec := range g.plan.Managed.Items {
					if valid && req.URL.Path == base+"/"+strings.ToLower(spec.Type)+"s" &&
						name == spec.DisplayName && folder == g.managedVerified["Folder"].ID && folder != "" {
						g.managedOperations[spec.Type] = id
					}
				}
			}
		}
	}
	root := "/" + g.workspace + "/" + g.lakehouse + "/" + g.directory
	if req.URL.Host == "onelake.dfs.fabric.microsoft.com" && req.URL.Path == root &&
		req.Method == http.MethodPut && req.URL.Query().Get("resource") == "directory" &&
		req.Header.Get("If-None-Match") == "*" && resp.StatusCode == http.StatusCreated {
		g.lakeOwned = true
	}
}

type countedTransport struct {
	base    http.RoundTripper
	counter *requestCounter
	guard   *scopeGuard
}

func (t *countedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	key, err := t.guard.authorize(req)
	if err != nil {
		return nil, err
	}
	t.counter.add(key)
	resp, err := t.base.RoundTrip(req)
	if resp != nil {
		t.guard.observe(req, resp)
	}
	return resp, err
}

func isNotFound(err error) bool {
	var httpErr *transport.HTTPError
	return errors.Is(err, fs.ErrNotExist) || (errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound)
}
