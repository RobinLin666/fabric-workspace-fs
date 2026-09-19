// Package auth adapts Azure Identity without saving or logging access tokens.
package auth

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"sync"
	"time"

	"fabric-workspace-fs/internal/transport"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
)

const (
	FabricScope  = "https://api.fabric.microsoft.com/.default"
	OneLakeScope = "https://storage.azure.com/.default"
	PowerBIScope = "https://analysis.windows.net/powerbi/api/.default"
)

// NewDefault constructs the standard Azure credential chain without requesting
// a token. The audience-isolated memory cache also covers AzureCLICredential, whose
// GetToken otherwise starts a new CLI process on every HTTP request.
func NewDefault() (transport.TokenSource, error) {
	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, &identityError{cause: err}
	}
	return &tokenSource{credential: credential}, nil
}

type tokenSource struct {
	credential azcore.TokenCredential
	mu         sync.Mutex
	entries    map[string]*tokenEntry
	now        func() time.Time
}

type tokenEntry struct {
	token   azcore.AccessToken
	refresh *refresh
}

type refresh struct {
	done  chan struct{}
	token azcore.AccessToken
	err   error
}

func (s *tokenSource) clock() time.Time {
	if s.now != nil {
		return s.now()
	}
	return time.Now()
}

func (s *tokenSource) Token(ctx context.Context, scope string) (string, error) {
	if scope != FabricScope && scope != OneLakeScope && scope != PowerBIScope {
		return "", fmt.Errorf("unsupported token audience: %w", fs.ErrInvalid)
	}
	if ctx == nil {
		return "", fmt.Errorf("missing token context: %w", fs.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.mu.Lock()
	if s.entries == nil {
		s.entries = make(map[string]*tokenEntry, 3)
	}
	entry := s.entries[scope]
	if entry == nil {
		entry = &tokenEntry{}
		s.entries[scope] = entry
	}
	if entry.token.Token != "" && s.clock().Add(2*time.Minute).Before(entry.token.ExpiresOn) {
		token := entry.token.Token
		s.mu.Unlock()
		return token, nil
	}
	if running := entry.refresh; running != nil {
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-running.done:
			if err := ctx.Err(); err != nil {
				return "", err
			}
			if running.err != nil {
				return "", running.err
			}
			if !s.clock().Before(running.token.ExpiresOn) {
				return "", errors.New("Azure Identity access token expired while waiting for refresh")
			}
			return running.token.Token, nil
		}
	}
	running := &refresh{done: make(chan struct{})}
	entry.refresh = running
	s.mu.Unlock()

	token, err := s.acquire(ctx, scope)
	s.mu.Lock()
	running.token, running.err = token, err
	if err == nil {
		entry.token = token
	}
	entry.refresh = nil
	close(running.done)
	s.mu.Unlock()
	return token.Token, err
}

func (s *tokenSource) acquire(ctx context.Context, scope string) (azcore.AccessToken, error) {
	token, err := s.credential.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{scope}})
	if ctx.Err() != nil {
		return azcore.AccessToken{}, ctx.Err()
	}
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return azcore.AccessToken{}, context.Canceled
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return azcore.AccessToken{}, context.DeadlineExceeded
		}
		return azcore.AccessToken{}, &identityError{cause: err}
	}
	if token.Token == "" || !s.clock().Before(token.ExpiresOn) {
		return azcore.AccessToken{}, errors.New("Azure Identity returned an empty or expired access token")
	}
	return token, nil
}

type identityError struct {
	cause error
}

func (e *identityError) Error() string { return "Azure Identity authentication failed" }
func (e *identityError) Unwrap() error { return e.cause }
