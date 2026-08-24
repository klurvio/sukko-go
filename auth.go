package sukko

import (
	"context"
	"encoding/json"
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

// runAuthOwner is the supervisor-lifetime credential goroutine: the sole owner of
// the refresh flow. It single-flights `auth` sends via two flags:
//
//   - inFlight — an `auth` was sent on the current epoch and no answer
//     (auth_ack/auth_error) has arrived. A refresh requested now is DEFERRED, not
//     dropped, because the select races the answering poke against a fresh
//     command and would otherwise coalesce a legitimate refresh whenever it
//     picked the command before consuming the poke.
//   - pending — a refresh is wanted but could not be sent yet (a flight was
//     outstanding, or there was no live socket). It is sent the moment the flight
//     completes.
//
// An epoch ending before its flight is answered clears inFlight — the answer will
// never come on the new connection — but keeps a still-wanted refresh (pending)
// and retries it on the reconnect. Without the inFlight clear, a drop mid-refresh
// would wedge inFlight true forever and silently no-op every future RefreshToken;
// clearing pending too would race a post-reconnect RefreshToken and drop it.
//
// (Proactive/reactive refresh arming and TokenSource invocation land on this loop
// next.)
func (c *Client) runAuthOwner(ownerCtx context.Context) {
	defer c.recoverAuthOwner()

	var inFlight, pending bool
	advance := func() {
		if inFlight || !pending {
			return
		}
		if c.sendAuthRefresh(ownerCtx) {
			inFlight = true
			pending = false
		}
		// A no-op send (no live socket) leaves pending set; the next epoch-reset
		// clears it (the reconnect re-auths via the dial), and a later command
		// retries — a refresh while disconnected is moot.
	}

	for {
		select {
		case <-ownerCtx.Done():
			return
		case <-c.authRefreshCmd:
			pending = true
			advance()
		case <-c.authPoke:
			// The in-flight refresh was answered; send a deferred one if pending.
			inFlight = false
			advance()
		case <-c.authEpochReset:
			// The epoch ended before an answer arrived: abandon the outstanding
			// flight — its answer will never come on the new connection. A refresh
			// still WANTED (pending) survives and is retried; clearing it here would
			// race a post-reconnect RefreshToken and drop it.
			inFlight = false
			advance()
		}
	}
}

// sendAuthRefresh sends an `auth` frame carrying the current JWT on the live
// connection, reporting whether it was sent. It is a no-op (false) when there is
// no JWT to refresh with or no live socket — a reconnect re-authenticates via the
// dial credential, so a refresh while disconnected is moot.
func (c *Client) sendAuthRefresh(ownerCtx context.Context) bool {
	token, _ := c.creds.snapshot()
	if token == "" {
		return false
	}
	conn := c.currentConn()
	if conn == nil {
		return false
	}
	frame, err := json.Marshal(wireAuth{Type: typeAuth, Data: authPayload{Token: token}})
	if err != nil {
		return false // unreachable: a struct of strings always marshals
	}
	if err := conn.Send(ownerCtx, frame); err != nil {
		return false // the socket is going away; the read side classifies it
	}
	return true
}

// pokeAuthOwner signals the auth-owner that a refresh answer arrived. Non-blocking
// (buffered 1): a prior unconsumed poke already covers this answer.
func (c *Client) pokeAuthOwner() {
	select {
	case c.authPoke <- struct{}{}:
	default:
	}
}

// resetAuthOwner tells the auth-owner an epoch ended, so it abandons any refresh
// outstanding on the connection that just died. Non-blocking (buffered 1).
func (c *Client) resetAuthOwner() {
	select {
	case c.authEpochReset <- struct{}{}:
	default:
	}
}

// recoverAuthOwner is the auth-owner's first defer (§V/§VII: no bare recover).
// The owner-panic → terminal routing lands with the refresh logic that can
// actually panic (TokenSource, frame sends); until then the owner only waits, so
// a recovered panic is logged and the goroutine exits.
func (c *Client) recoverAuthOwner() {
	if r := recover(); r != nil {
		c.cfg.logger.Error("sukko: auth-owner panic", "value", fmt.Sprint(r))
	}
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
