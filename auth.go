package sukko

import (
	"context"
	"encoding/json"
	"errors"
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
		exp               int64     // last auth_ack expiry (unix seconds; 0 = never/unknown)
		ownerExpiry       time.Time // last Token.Expiry from a fetch (zero = unknown)
		lastAuthSent      time.Time // for the RefreshMinInterval floor
		refreshTimer      Timer     // the proactive/reactive refresh schedule; nil = disarmed
		refreshC          <-chan time.Time
		tokenFailCount    int       // consecutive TokenSource fetch failures; reset on any success
		lastFetchAt       time.Time // last TokenSource fetch attempt, for fetch pacing
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
	// armProactive arms the next refresh from the EARLIER of the two known
	// expiries (ADR-0004): the server's auth_ack.exp and the caller's Token.Expiry.
	// Either alone suffices; Token.Expiry is what gives a TokenSource client a
	// schedule before any auth_ack (the handshake sends none). Neither known → no
	// proactive timer. Floored so it never schedules within RefreshMinInterval of
	// the last auth.
	armProactive := func() {
		var refreshAt time.Time
		if exp != 0 {
			refreshAt = time.Unix(exp, 0).Add(-c.cfg.refreshLead)
		}
		if !ownerExpiry.IsZero() {
			fromToken := ownerExpiry.Add(-c.cfg.refreshLead)
			if refreshAt.IsZero() || fromToken.Before(refreshAt) {
				refreshAt = fromToken
			}
		}
		if refreshAt.IsZero() {
			disarmRefresh()
			return
		}
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
	advance := func() {
		if inFlight || !pending {
			return
		}
		conn := c.currentConn()
		if conn == nil {
			return // no live socket; the epoch-up signal retries on the reconnect
		}
		// Honor the RefreshMinInterval floor at SEND time, not only at arm time: a
		// proactive timer that fired mid-flight, or a caller RefreshToken, must not
		// bypass the server's 1-per-interval rate limit. The first auth (zero
		// lastAuthSent) is not floored.
		if !lastAuthSent.IsZero() {
			if wait := lastAuthSent.Add(c.cfg.refreshMinInterval).Sub(c.clock.Now()); wait > 0 {
				armRefresh(wait)
				return
			}
		}

		// Obtain the credential to present. A TokenSource client fetches a fresh
		// one (owner-only invocation, bounded by the token_source timer); a static
		// client reads the stored token.
		var token string
		if c.cfg.tokenSource != nil {
			// Pace fetches on the last fetch attempt, not the last successful send:
			// while fetches fail no auth is sent, so lastAuthSent never advances, and
			// a RefreshToken burst — the natural reaction to *TokenSourceError — would
			// hammer the token endpoint straight to the exhaustion terminal.
			if !lastFetchAt.IsZero() {
				if wait := lastFetchAt.Add(c.cfg.refreshMinInterval).Sub(c.clock.Now()); wait > 0 {
					armRefresh(wait)
					return
				}
			}
			lastFetchAt = c.clock.Now()
			tok, err := c.fetchToken(ownerCtx)
			if err != nil {
				if ownerCtx.Err() != nil {
					return // a fetch aborted by owner shutdown is not a credential failure
				}
				tokenFailCount++
				attempt := tokenFailCount
				// Record the connected-refresh terminal BEFORE emitting the attempt
				// error, so a parked emit cannot delay it (the epoch record cancels
				// the epoch, which drives teardown and unparks the emit). Between
				// epochs the record no-ops; the retry armed below re-terminates once
				// connected, so the owner can never wedge at Max with no timer.
				if attempt >= MaxTokenSourceAttempts {
					c.terminateTokenSourceExhausted()
				}
				c.ownerSurface(ownerCtx, &TokenSourceError{Attempt: attempt, Cause: err})
				armRefresh(max(computeBackoffDelay(c.cfg.backoff, attempt-1, c.cfg.rand), c.cfg.refreshMinInterval))
				return
			}
			tokenFailCount = 0
			c.creds.setToken(tok.Value)
			if !tok.Expiry.IsZero() {
				// A caller-authoritative expiry arms the proactive schedule now,
				// without waiting for an auth_ack the handshake never sends.
				ownerExpiry = tok.Expiry
				armProactive()
			}
			token = tok.Value
		} else {
			token, _ = c.creds.snapshot()
			if token == "" {
				return // API-key-only: nothing to refresh with
			}
		}

		if c.sendAuthFrame(ownerCtx, conn, token) {
			inFlight = true
			pending = false
			lastAuthSent = c.clock.Now()
		}
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
			advance() // a still-wanted refresh: no-ops now (conn cleared), retried on epoch-up
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

// sendAuthFrame marshals and sends an `auth` frame carrying token on conn,
// reporting whether it was sent. A send error means the socket is going away; the
// read side classifies it.
func (c *Client) sendAuthFrame(ownerCtx context.Context, conn Conn, token string) bool {
	frame, err := json.Marshal(wireAuth{Type: typeAuth, Data: authPayload{Token: token}})
	if err != nil {
		return false // unreachable: a struct of strings always marshals
	}
	return conn.Send(ownerCtx, frame) == nil
}

// tokenResult carries a TokenSource outcome from its detached goroutine.
type tokenResult struct {
	tok Token
	err error
}

// fetchToken invokes the caller's TokenSource, bounded by the injectable
// token_source timer. The callback runs in its OWN goroutine, not inline, for two
// reasons: a callback that ignores its context or blocks cannot hang the owner
// (and thus Close/terminal, which wait on the owner to exit), and a panicking
// callback is contained here rather than crashing the caller's process (§VI). On a
// timeout or owner cancellation the goroutine is abandoned — it exits when the
// callback finally returns, sending into a buffered channel that never blocks. The
// detached goroutine cannot be a tracked wg.Go (a blocking Wait would defeat the
// abandon), and its inline recover is the required §VI containment for the
// untrusted caller code it runs.
func (c *Client) fetchToken(ownerCtx context.Context) (Token, error) {
	fetchCtx, cancel := context.WithCancel(ownerCtx)
	defer cancel()

	timer := c.clock.NewTimer(c.cfg.tokenSourceTimeout, purposeTokenSource)
	defer timer.Stop()

	res := make(chan tokenResult, 1) // buffered: an abandoned goroutine never blocks
	go func() {
		defer func() {
			if r := recover(); r != nil {
				res <- tokenResult{err: fmt.Errorf("sukko: token source panicked: %v", r)}
			}
		}()
		tok, err := c.cfg.tokenSource(fetchCtx)
		res <- tokenResult{tok: tok, err: err}
	}()

	select {
	case r := <-res:
		if r.err != nil {
			return Token{}, fmt.Errorf("sukko: token source: %w", r.err)
		}
		if r.tok.Value == "" {
			return Token{}, errEmptyTokenSource
		}
		return r.tok, nil
	case <-timer.C():
		return Token{}, errTokenSourceTimeout // abandon the goroutine (defer cancels its ctx)
	case <-ownerCtx.Done():
		return Token{}, fmt.Errorf("sukko: token source: %w", ownerCtx.Err())
	}
}

var (
	// errEmptyTokenSource is returned when a TokenSource yields an empty
	// credential — a failure like any other, retried and counted toward exhaustion.
	errEmptyTokenSource = errors.New("sukko: token source returned an empty token")
	// errTokenSourceTimeout is the bound WithTokenSourceTimeout enforces on a
	// single fetch.
	errTokenSourceTimeout = errors.New("sukko: token source timed out")
)

// ownerSurface emits an auth-owner event on the delivery channel using the OWNER
// context (not root) for the epoch slot: an owner parked here on a full channel
// must unpark when the owner is torn down (ownerCancel), or ownerWg.Wait() — and
// thus Close/terminal — would hang. *TokenSourceError is the may-block region.
func (c *Client) ownerSurface(ownerCtx context.Context, ev Event) {
	c.delivery.send(c.rootCtx, ownerCtx, ev)
}

// terminateTokenSourceExhausted records the connected-refresh TokenSource
// exhaustion terminal into the current epoch's first-cause slot and cancels it —
// the same mechanism the heartbeat timeout uses. The owner cannot call
// terminalSequence or cancel root itself. Between epochs the reference is nil and
// this no-ops; the reconnect path's fetch failures then carry the client
// (non-terminal), which is spec-coherent (FR-005).
func (c *Client) terminateTokenSourceExhausted() {
	ep := c.currentEpochRef()
	if ep == nil {
		return
	}
	out, ok := lookupInternalCausePolicy(causeTokenSourceExhausted, c.cfg.reconnect)
	if !ok {
		return
	}
	ep.record(terminationOutcome{
		class:   out.class,
		cause:   ErrTokenSourceFailed,
		trigger: triggerForClass(out.class),
	})
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
