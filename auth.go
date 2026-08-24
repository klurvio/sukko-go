package sukko

import (
	"fmt"
	"net/url"
	"sync"
)

// Dial-time credential material. The names match the contract's auth bindings and
// both sibling SDKs (§I, §XVIII): a JWT rides Authorization: Bearer, an API key
// rides X-API-Key, and WithQueryParamAuth moves both to query params.
const (
	headerAuthorization = "Authorization"
	authBearerPrefix    = "Bearer "
	//nolint:gosec // G101: this is the HTTP header NAME the gateway reads the API key from, not a credential value — the key itself comes from WithAPIKey at runtime.
	headerAPIKey     = "X-API-Key"
	queryParamToken  = "token"
	queryParamAPIKey = "api_key"
)

// credentialStore holds the live credential — the JWT and/or API key — behind a
// mutex so the dial path reads the current value (picking up rotation from
// UpdateToken/Escalate) while callers mutate it. It is the state the
// supervisor-lifetime auth-owner will own once refresh lands; for now the client
// reads and writes it directly for the pre-connect and static-credential paths.
type credentialStore struct {
	mu     sync.Mutex
	token  string
	apiKey string
}

func newCredentialStore(token, apiKey string) *credentialStore {
	return &credentialStore{token: token, apiKey: apiKey}
}

// snapshot returns the current credential pair for a dial.
func (s *credentialStore) snapshot() (token, apiKey string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token, s.apiKey
}

// setToken stores a JWT (UpdateToken/Escalate). Storing a non-empty token also
// flips the credential class to "has JWT" — an API-key-only client becomes
// publish-capable — because the class is exactly "a JWT is present".
func (s *credentialStore) setToken(token string) {
	s.mu.Lock()
	s.token = token
	s.mu.Unlock()
}

// authQueryURL returns base with the credential appended as query parameters —
// the WithQueryParamAuth path. Existing query values are preserved.
func authQueryURL(base, token, apiKey string) (string, error) {
	u, err := url.Parse(base)
	if err != nil {
		return "", fmt.Errorf("sukko: parsing the dial URL: %w", err)
	}
	q := u.Query()
	if token != "" {
		q.Set(queryParamToken, token)
	}
	if apiKey != "" {
		q.Set(queryParamAPIKey, apiKey)
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}
