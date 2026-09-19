package mwc

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"fabric-workspace-fs/internal/cachepolicy"
	"fabric-workspace-fs/internal/fabric"
	"fabric-workspace-fs/internal/resources"
	"fabric-workspace-fs/internal/transport"
)

const (
	grantRefreshBefore = 2 * time.Minute
	maxGrants          = 128
)

type capturedToken struct {
	value, scope string
	expiry       time.Time
	now          func() time.Time
}

func (t capturedToken) Token(ctx context.Context, scope string) (string, error) {
	if err := ready(ctx); err != nil {
		return "", err
	}
	if scope != t.scope {
		return "", errors.New("MWC credential audience mismatch")
	}
	if !t.expiry.IsZero() && !t.now().Before(t.expiry) {
		return "", errors.New("MWC grant expired before the request")
	}
	return t.value, nil
}

type grantKey struct {
	origin      string
	fingerprint [sha256.Size]byte
	workspace   string
	item        string
	workload    string
	capacity    string
}

type grant struct {
	origin, capacity string
	expires          time.Time
	http             *transport.Client
}

type retainedGrant struct {
	key   grantKey
	value *grant
}

type grantFlight struct {
	done  chan struct{}
	value *grant
	err   error
}

type grantCache struct {
	mu      sync.Mutex
	entries map[grantKey]*list.Element
	flights map[grantKey]*grantFlight
	lru     list.List
}

type route struct {
	grant       *grant
	basePath    string
	cachePrefix string
	policy      cachepolicy.Values
}

func (c *Client) route(ctx context.Context, target resources.Target) (route, error) {
	pbi, err := c.tokens.Token(ctx, powerBIScope)
	if err != nil {
		return route{}, safeError(ctx, "Power BI token acquisition failed", err)
	}
	if !validToken(pbi) {
		return route{}, errors.New("identity provider returned an invalid Power BI token")
	}
	fingerprint := sha256.Sum256([]byte(pbi))
	identity := c.origin + "/" + hex.EncodeToString(fingerprint[:])
	catalogTTL := c.cachePolicy.CatalogTTL(target.WorkspaceID)
	workspace, err := c.capacities.GetWithTTL(ctx, identity+"/"+strings.ToLower(target.WorkspaceID), catalogTTL, func(ctx context.Context) (fabric.Workspace, error) {
		ws, err := c.workspaces.GetWorkspace(ctx, target.WorkspaceID)
		if err != nil {
			return fabric.Workspace{}, safeError(ctx, "resource workspace lookup failed", err)
		}
		if !strings.EqualFold(ws.ID, target.WorkspaceID) || fabric.ValidateID(ws.ID) != nil ||
			fabric.ValidateID(ws.CapacityID) != nil {
			return fabric.Workspace{}, invalidResponse("workspace or capacity identity is missing or mismatched")
		}
		return fabric.Workspace{ID: ws.ID, CapacityID: ws.CapacityID}, nil
	})
	if err != nil {
		return route{}, err
	}
	workload := "Notebook"
	if target.Kind == "Environment" {
		workload = "SparkCore"
	}
	key := grantKey{
		origin: c.origin, fingerprint: fingerprint, workload: workload,
		workspace: strings.ToLower(target.WorkspaceID), item: strings.ToLower(target.ItemID),
		capacity: strings.ToLower(workspace.CapacityID),
	}
	g, err := c.getGrant(ctx, key, pbi)
	if err != nil {
		return route{}, err
	}
	base := "/webapi/capacities/" + g.capacity + "/workloads/" + workload
	if workload == "Notebook" {
		base += "/Data/Direct/api/workspaces/" + key.workspace + "/artifacts/" + key.item + "/filesystem/workdir/"
	} else {
		base += "/SparkCoreService/Automatic/v1/Environments/workspaces/" + key.workspace +
			"/artifacts/" + key.item + "/filesystem/workdir/"
	}
	surface := "builtin"
	if target.Kind == "Environment" {
		surface = "resources"
	}
	return route{
		grant: g, basePath: base,
		cachePrefix: targetPrefix(target) + identity + "/" + key.capacity + "/" + g.origin + "/",
		policy: c.cachePolicy.Resolve(cachepolicy.Selector{
			Workspace: target.WorkspaceID, Item: target.ItemID, Type: target.Kind, Surface: surface,
		}),
	}, nil
}

func validToken(token string) bool {
	if token == "" {
		return false
	}
	for _, r := range token {
		if r <= 0x20 || r >= 0x7f {
			return false
		}
	}
	return true
}

func (c *Client) getGrant(ctx context.Context, key grantKey, pbi string) (*grant, error) {
	if err := ready(ctx); err != nil {
		return nil, err
	}
	s := &c.grants
	s.mu.Lock()
	now := c.now()
	for k, element := range s.entries {
		if !now.Before(element.Value.(retainedGrant).value.expires) {
			delete(s.entries, k)
			s.lru.Remove(element)
		}
	}
	if element := s.entries[key]; element != nil {
		value := element.Value.(retainedGrant).value
		if now.Add(grantRefreshBefore).Before(value.expires) {
			s.lru.MoveToFront(element)
			s.mu.Unlock()
			return c.checkGrant(ctx, value)
		}
		delete(s.entries, key)
		s.lru.Remove(element)
	}
	if flight := s.flights[key]; flight != nil {
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-flight.done:
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if flight.err != nil {
				return nil, flight.err
			}
			return c.checkGrant(ctx, flight.value)
		}
	}
	flight := &grantFlight{done: make(chan struct{})}
	if s.flights == nil {
		s.flights = make(map[grantKey]*grantFlight)
	}
	s.flights[key] = flight
	s.mu.Unlock()

	finished := false
	defer func() {
		if !finished {
			c.finishGrant(key, flight, nil, errors.New("MWC grant acquisition did not complete"))
		}
	}()
	value, err := c.loadGrant(ctx, key, pbi)
	if err == nil {
		value, err = c.checkGrant(ctx, value)
	}
	c.finishGrant(key, flight, value, err)
	finished = true
	if err != nil {
		return nil, err
	}
	return c.checkGrant(ctx, value)
}

func (c *Client) checkGrant(ctx context.Context, g *grant) (*grant, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if g == nil || !c.now().Before(g.expires) {
		return nil, errors.New("MWC grant expired during acquisition")
	}
	return g, nil
}

func (c *Client) finishGrant(key grantKey, f *grantFlight, g *grant, err error) {
	s := &c.grants
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.flights, key)
	f.value, f.err = g, err
	if err == nil {
		if s.entries == nil {
			s.entries = make(map[grantKey]*list.Element)
		}
		for len(s.entries) >= maxGrants {
			oldest := s.lru.Back()
			delete(s.entries, oldest.Value.(retainedGrant).key)
			s.lru.Remove(oldest)
		}
		s.entries[key] = s.lru.PushFront(retainedGrant{key: key, value: g})
	}
	close(f.done)
}

func (c *Client) loadGrant(ctx context.Context, key grantKey, pbi string) (*grant, error) {
	catalogTTL := c.cachePolicy.CatalogTTL(key.workspace)
	cluster, err := c.clusters.GetWithTTL(ctx, key.origin+"/"+hex.EncodeToString(key.fingerprint[:]), catalogTTL, func(ctx context.Context) (string, error) {
		client, err := c.boundHTTP(key.origin, powerBIScope, "Bearer", pbi, time.Time{})
		if err != nil {
			return "", err
		}
		resp, err := client.Request(ctx, http.MethodGet, client.URL("/metadata/cluster", nil),
			http.Header{"Accept": {"application/json"}}, nil, true)
		if err != nil {
			return "", err
		}
		if err := requireOK(resp); err != nil {
			return "", err
		}
		body, err := readBody(ctx, resp, maxControlBytes)
		if err != nil {
			return "", err
		}
		var wire struct {
			BackendURL string `json:"backendUrl"`
		}
		if json.Unmarshal(body, &wire) != nil {
			return "", invalidResponse("cluster discovery schema")
		}
		return strictOrigin(wire.BackendURL, true)
	})
	if err != nil {
		return nil, err
	}
	client, err := c.boundHTTP(cluster, powerBIScope, "Bearer", pbi, time.Time{})
	if err != nil {
		return nil, err
	}
	request := struct {
		Capacity  string   `json:"capacityObjectId"`
		Workspace string   `json:"workspaceObjectId"`
		Workload  string   `json:"workloadType"`
		Artifacts []string `json:"artifactObjectIds"`
	}{key.capacity, key.workspace, key.workload, []string{key.item}}
	body, _ := json.Marshal(request)
	resp, err := client.Request(ctx, http.MethodPost, client.URL("/metadata/v201606/generatemwctoken", nil),
		http.Header{"Accept": {"application/json"}, "Content-Type": {"application/json"}}, body, false)
	if err != nil {
		return nil, err
	}
	if err := requireOK(resp); err != nil {
		return nil, err
	}
	body, err = readBody(ctx, resp, maxControlBytes)
	if err != nil {
		return nil, err
	}
	var wire struct {
		Token    string          `json:"Token"`
		Host     string          `json:"TargetUriHost"`
		Capacity string          `json:"CapacityObjectId"`
		Expiry   json.RawMessage `json:"Expiry"`
	}
	if json.Unmarshal(body, &wire) != nil || !validToken(wire.Token) ||
		fabric.ValidateID(wire.Capacity) != nil || !strings.EqualFold(wire.Capacity, key.capacity) {
		return nil, invalidResponse("resource grant identity or token")
	}
	origin, err := strictOrigin(wire.Host, true)
	if err != nil {
		return nil, err
	}
	expiry, err := grantExpiry(wire.Expiry, wire.Token)
	if err != nil || !c.now().Before(expiry) {
		return nil, invalidResponse("resource grant expiry")
	}
	resourceHTTP, err := c.boundHTTP(origin, resourceScope, "MwcToken", wire.Token, expiry)
	if err != nil {
		return nil, err
	}
	return &grant{origin: origin, capacity: key.capacity, expires: expiry, http: resourceHTTP}, nil
}

func grantExpiry(explicit json.RawMessage, token string) (time.Time, error) {
	if len(explicit) != 0 {
		return parseExpiry(explicit, true)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return time.Time{}, invalidResponse("missing resource grant expiry")
	}
	decoded := make([][]byte, 3)
	for i, part := range parts {
		value, err := base64.RawURLEncoding.Strict().DecodeString(part)
		if err != nil || len(value) == 0 {
			return time.Time{}, invalidResponse("malformed resource grant JWT")
		}
		decoded[i] = value
	}
	var header struct {
		Algorithm string `json:"alg"`
	}
	var claims struct {
		Expiry json.RawMessage `json:"exp"`
	}
	if json.Unmarshal(decoded[0], &header) != nil || header.Algorithm == "" || header.Algorithm == "none" ||
		json.Unmarshal(decoded[1], &claims) != nil || len(claims.Expiry) == 0 {
		return time.Time{}, invalidResponse("malformed resource grant JWT expiry")
	}
	return parseExpiry(claims.Expiry, false)
}

func parseExpiry(raw json.RawMessage, allowDate bool) (time.Time, error) {
	text := strings.TrimSpace(string(raw))
	if strings.HasPrefix(text, `"`) {
		if !allowDate {
			return time.Time{}, invalidResponse("resource grant JWT expiry must be numeric")
		}
		if json.Unmarshal(raw, &text) != nil {
			return time.Time{}, invalidResponse("resource grant expiry")
		}
		if parsed, err := time.Parse(time.RFC3339Nano, text); err == nil {
			return parsed, nil
		}
	}
	seconds, err := strconv.ParseInt(text, 10, 64)
	if err != nil || seconds <= 0 || seconds > 253402300799 {
		return time.Time{}, invalidResponse("resource grant expiry")
	}
	return time.Unix(seconds, 0), nil
}
