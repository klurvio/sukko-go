package sukko

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"
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

// authInbox coalesces the answers the decode loop hands the auth-owner: the
// latest auth_ack expiry and whether an auth_error arrived. The owner drains it
// on each poke. Coalescing (not a queue) is deliberate — the owner needs the
// latest state, and the buffered-1 poke must never drop the answer that both
// completes a flight and re-arms the schedule.
type authInbox struct {
	mu       sync.Mutex
	hasAck   bool
	ackExp   int64
	hasError bool
}

// putAck records an auth_ack's expiry (unix seconds; 0 = never expires).
func (b *authInbox) putAck(exp int64) {
	b.mu.Lock()
	b.hasAck = true
	b.ackExp = exp
	b.mu.Unlock()
}

// putError records that an auth_error arrived.
func (b *authInbox) putError() {
	b.mu.Lock()
	b.hasError = true
	b.mu.Unlock()
}

// drain returns and clears the coalesced state.
func (b *authInbox) drain() (hasAck bool, ackExp int64, hasError bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	hasAck, ackExp, hasError = b.hasAck, b.ackExp, b.hasError
	b.hasAck, b.hasError = false, false
	return hasAck, ackExp, hasError
}

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

	var (
		inFlight, pending bool
		exp               int64     // last known credential expiry (unix seconds; 0 = never)
		lastAuthSent      time.Time // for the RefreshMinInterval floor
		refreshTimer      Timer     // the proactive/reactive refresh schedule; nil = disarmed
		refreshC          <-chan time.Time
	)

	disarmRefresh := func() {
		if refreshTimer != nil {
			refreshTimer.Stop()
			refreshTimer = nil
			refreshC = nil
		}
	}
	armRefresh := func(after time.Duration) {
		disarmRefresh()
		refreshTimer = c.clock.NewTimer(max(after, 0), purposeRefresh)
		refreshC = refreshTimer.C()
	}
	advance := func() {
		if inFlight || !pending {
			return
		}
		if c.sendAuthRefresh(ownerCtx) {
			inFlight = true
			pending = false
			lastAuthSent = c.clock.Now()
		}
		// A no-op send (no live socket) leaves pending set; the epoch-up signal
		// retries it on the new connection.
		//
		// The RefreshMinInterval floor is enforced at ARM time (armProactive /
		// armReactive), so the automatic schedule respects it. A narrow bypass
		// remains: a proactive timer that fires mid-flight sets pending, and this
		// send runs when the flight completes without re-checking the floor — a
		// too-soon send the server rate-limits (answered with auth_error →
		// reactive re-refresh, self-limiting). A floor-at-send check lands with the
		// TokenSource increment, where the send path is reworked anyway.
	}
	// armProactive arms the next refresh from the known expiry, floored so it never
	// schedules within RefreshMinInterval of the last auth. exp == 0 (never
	// expires) disarms — nothing to refresh ahead of.
	armProactive := func() {
		if exp == 0 {
			disarmRefresh()
			return
		}
		refreshAt := time.Unix(exp, 0).Add(-c.cfg.refreshLead)
		if floor := lastAuthSent.Add(c.cfg.refreshMinInterval); refreshAt.Before(floor) {
			refreshAt = floor
		}
		armRefresh(refreshAt.Sub(c.clock.Now()))
	}
	// armReactive schedules a refresh after an auth_error, floored so an
	// auth_error → refresh storm is impossible.
	armReactive := func() {
		armRefresh(lastAuthSent.Add(c.cfg.refreshMinInterval).Sub(c.clock.Now()))
	}

	for {
		select {
		case <-ownerCtx.Done():
			disarmRefresh()
			return
		case <-c.authRefreshCmd:
			pending = true
			advance()
		case <-c.authPoke:
			hasAck, ackExp, hasError := c.authInbox.drain()
			// Clear the flight ONLY on a real answer: a spurious poke (the inbox
			// already drained by a prior poke, or by an epoch reset) must not clear
			// a live flight and let a second auth go out.
			if hasAck || hasError {
				inFlight = false
			}
			if hasAck {
				exp = ackExp
				armProactive()
			}
			if hasError {
				armReactive()
			}
			advance()
		case <-c.authEpochReset:
			// The epoch ended before an answer arrived: abandon the outstanding
			// flight — its answer will never come on the new connection. A refresh
			// still WANTED (pending) survives and is retried on the reconnect;
			// clearing it here would race a post-reconnect RefreshToken and drop it.
			// Drain the inbox so a stale answer from the dead epoch cannot later
			// clear a fresh flight. The refresh SCHEDULE persists (the expiry is
			// unchanged by a reconnect), so the timer is left running.
			inFlight = false
			c.authInbox.drain()
		case <-c.authEpochUp:
			// A new epoch is connected: retry a refresh wanted while disconnected
			// (e.g. the proactive timer fired during backoff and could not send).
			advance()
		case <-refreshC:
			// The proactive/reactive schedule fired: time to refresh.
			pending = true
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

// upAuthOwner tells the auth-owner a new epoch is connected, so it retries a
// refresh wanted while disconnected. Non-blocking (buffered 1).
func (c *Client) upAuthOwner() {
	select {
	case c.authEpochUp <- struct{}{}:
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
