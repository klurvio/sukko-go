package sukko

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"runtime/debug"
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

// authDialReq is a supervisor→owner request for a pre-dial credential fetch (B1).
// reply is buffered(1) so the owner never blocks answering even if the supervisor
// abandoned the wait on a root/connect-context cancellation.
type authDialReq struct {
	reply chan error
}

// escalateBox holds the latest caller-supplied escalation JWT awaiting send, with
// the override generation captured at the Escalate CALL (so the commit can tell
// whether a fresher UpdateToken intervened). Escalate (caller goroutine) writes it;
// the auth-owner drains it on the authEscalateCmd signal. Latest-wins: a second
// Escalate before the first sends overwrites both.
type escalateBox struct {
	mu  sync.Mutex
	jwt string
	gen uint64
	has bool
}

func (b *escalateBox) put(jwt string, gen uint64) {
	b.mu.Lock()
	b.jwt, b.gen, b.has = jwt, gen, true
	b.mu.Unlock()
}

// take returns and clears the pending escalation JWT and its captured generation.
func (b *escalateBox) take() (jwt string, gen uint64, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	jwt, gen, ok = b.jwt, b.gen, b.has
	b.jwt, b.gen, b.has = "", 0, false
	return jwt, gen, ok
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
	// overridePending marks a CALLER-set JWT (via UpdateToken) that must win the
	// next auth over a configured TokenSource. The owner peeks it before fetching
	// and clears it only once the presenting auth is ANSWERED — an auth_ack/
	// auth_error for a connected refresh, or a completed handshake for a dial — not
	// merely attempted, so a send/dial failure never loses it to a resumed fetch.
	// overrideGen bumps on every caller write, so a clear guarded by the peeked
	// generation cannot discard a fresher UpdateToken that raced the presentation.
	overridePending bool
	overrideGen     uint64
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

// setToken stores a CALLER-set JWT (UpdateToken) and marks it override-pending, so
// it wins the next auth over a TokenSource fetch. Storing a non-empty token also
// flips the credential class to "has JWT" — an API-key-only client becomes
// publish-capable — because the class is exactly "a JWT is present".
func (s *credentialStore) setToken(token string) {
	s.mu.Lock()
	s.token = token
	s.overridePending = true
	s.overrideGen++
	s.mu.Unlock()
}

// setTokenFromSource stores a JWT resolved by a TokenSource fetch WITHOUT claiming
// override precedence, so a TokenSource resumes normally afterward. It no-ops when
// a caller override is pending: a concurrent UpdateToken that raced an in-flight
// fetch wins, and its credential is not clobbered by the fetch's result.
func (s *credentialStore) setTokenFromSource(token string) {
	s.mu.Lock()
	if !s.overridePending {
		s.token = token
	}
	s.mu.Unlock()
}

// generation returns the current override generation, for a caller to capture at
// the moment it acts (e.g. Escalate) so a later commit can tell whether a fresher
// UpdateToken has intervened.
func (s *credentialStore) generation() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.overrideGen
}

// commitEscalationIfGen stores a successfully-acked escalation JWT and clears any
// pending override — but ONLY if no caller write has bumped the generation since
// the escalation was requested (gen captured at Escalate call time). This makes
// the two caller actions latest-wins even though the ack lands a round-trip later:
// UpdateToken-then-Escalate commits (the escalation is newer, and a static client's
// pre-existing stale flag is cleared here — the F1 fix); Escalate-then-UpdateToken
// no-ops (the UpdateToken is newer, and its credential + override survive). A
// TokenSource resumes after a committed escalation.
func (s *credentialStore) commitEscalationIfGen(token string, gen uint64) {
	s.mu.Lock()
	if s.overrideGen == gen {
		s.token = token
		s.overridePending = false
	}
	s.mu.Unlock()
}

// peekOverride reports the stored token, whether a caller override is pending, and
// its generation — WITHOUT consuming it. The owner's fetch paths present the token
// and skip the fetch on override, then clear it via clearOverrideIfGen only once
// the token is successfully presented.
func (s *credentialStore) peekOverride() (token string, override bool, gen uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.token, s.overridePending, s.overrideGen
}

// clearOverrideIfGen clears the pending override only if no newer caller write has
// bumped the generation since it was peeked — so a presentation success consumes
// exactly the override it presented, never a fresher UpdateToken that raced it.
func (s *credentialStore) clearOverrideIfGen(gen uint64) {
	s.mu.Lock()
	if s.overrideGen == gen {
		s.overridePending = false
	}
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
// The loop also owns proactive/reactive refresh arming, TokenSource invocation
// (connected refresh and B1 pre-dial fetch), and escalation: an Escalate is a
// caller-supplied JWT that takes precedence over a pending refresh, is held in
// flight, and is committed to the credential store only on its ack.
func (c *Client) runAuthOwner(ownerCtx context.Context) {
	defer c.recoverAuthOwner()

	var (
		inFlight, pending bool
		pendingEscalation bool      // an Escalate is wanted but not yet sent
		escalationJWT     string    // the JWT a pending escalation will send
		escalationGen     uint64    // the override generation captured at that Escalate call
		inFlightJWT       string    // the JWT of an in-flight escalation; non-empty ⇒ the flight is an escalation, committed to the store only on its ack
		inFlightJWTGen    uint64    // the captured generation of the in-flight escalation
		flightOverride    bool      // the in-flight REFRESH presented a caller override, awaiting its answer to consume it
		flightOverrideGen uint64    // the generation of that presented override
		exp               int64     // last auth_ack expiry (unix seconds; 0 = never/unknown)
		ownerExpiry       time.Time // last Token.Expiry from a fetch (zero = unknown)
		lastAuthSent      time.Time // for the RefreshMinInterval floor
		refreshTimer      Timer     // the proactive/reactive refresh schedule; nil = disarmed
		refreshC          <-chan time.Time
		tokenFailCount    int       // consecutive TokenSource fetch failures; reset on any success
		lastFetchAt       time.Time // last TokenSource fetch attempt, for fetch pacing
		dialOverride      bool      // the last pre-dial fetch presented a caller override, awaiting handshake success to consume it
		dialOverrideGen   uint64    // the generation of that override, for the gen-guarded clear
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
		if inFlight {
			return
		}
		if !pending && !pendingEscalation {
			return
		}
		conn := c.currentConn()
		if conn == nil {
			return // no live socket; the epoch-up signal retries on the reconnect
		}
		// Honor the RefreshMinInterval floor at SEND time, not only at arm time: a
		// proactive timer that fired mid-flight, or a caller RefreshToken/Escalate,
		// must not bypass the server's 1-per-interval auth rate limit (which applies
		// to both modes). The first auth (zero lastAuthSent) is not floored.
		if !lastAuthSent.IsZero() {
			if wait := lastAuthSent.Add(c.cfg.refreshMinInterval).Sub(c.clock.Now()); wait > 0 {
				armRefresh(wait)
				return
			}
		}

		// Escalation takes precedence over a pending refresh: it carries a fresher,
		// caller-supplied credential, so a pending refresh is mooted. The JWT is held
		// in-flight and committed to the store only on its ack — a rejected escalation
		// must not flip the credential class. setFlightMode before the send so a fast
		// ack cannot read a stale mode.
		if pendingEscalation {
			c.setFlightMode(AuthEscalation)
			if c.sendAuthFrame(ownerCtx, conn, escalationJWT) {
				inFlight = true
				pendingEscalation = false
				pending = false
				inFlightJWT = escalationJWT
				inFlightJWTGen = escalationGen
				lastAuthSent = c.clock.Now()
			}
			return
		}

		// A refresh. Obtain the credential to present: a TokenSource client fetches a
		// fresh one (owner-only invocation, bounded by the token_source timer); a
		// static client reads the stored token.
		var token string
		var usedOverride bool
		var overrideGen uint64
		if c.cfg.tokenSource != nil {
			if callerTok, override, gen := c.creds.peekOverride(); override {
				// A caller override (UpdateToken) wins exactly this refresh over a
				// TokenSource fetch; the TokenSource resumes on the next one. No fetch,
				// so no fetch-pacing and no expiry arm (the caller supplied none). The
				// override is cleared only on send SUCCESS below, so a failed send does
				// not lose the caller's credential to a resumed fetch.
				tokenFailCount = 0
				token = callerTok
				usedOverride = true
				overrideGen = gen
			} else {
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
				c.creds.setTokenFromSource(tok.Value)
				if !tok.Expiry.IsZero() {
					// A caller-authoritative expiry arms the proactive schedule now,
					// without waiting for an auth_ack the handshake never sends.
					ownerExpiry = tok.Expiry
					armProactive()
				}
				token = tok.Value
			}
		} else {
			token, _ = c.creds.snapshot()
			if token == "" {
				return // API-key-only: nothing to refresh with
			}
		}

		c.setFlightMode(AuthRefresh)
		if c.sendAuthFrame(ownerCtx, conn, token) {
			inFlight = true
			pending = false
			lastAuthSent = c.clock.Now()
			if usedOverride {
				// The override credential was PRESENTED, not yet answered. Record it so
				// the poke handler consumes it only once the server answers (ack or
				// auth_error) — a send whose epoch dies unanswered must not consume it,
				// or the caller's credential is lost with no completed auth.
				flightOverride = true
				flightOverrideGen = overrideGen
			}
		}
	}

	// dialFetch answers a supervisor pre-dial ensure-token request (B1). It runs
	// ONLY here in the owner goroutine, so it shares the same tokenFailCount /
	// lastFetchAt / ownerExpiry state as the connected refresh with no lock — the
	// reason the fetch is a reply-chan request and not a direct supervisor call
	// (ADR-0009, T107). Unlike the connected path it is NON-TERMINAL: a failure,
	// even the 5th-or-later consecutive, is returned as the dial error and flows
	// through classifyDial's non-HandshakeError fallthrough to a reconnect-class
	// backoff — exactly FR-005 line 63, "never manufacture a terminal state" on
	// the reconnect path. It does NOT gate on the RefreshMinInterval floor: the
	// supervisor's backoff already paces reconnect dials and the first dial must
	// fetch immediately. It only RECORDS lastFetchAt so a post-connect refresh
	// floors correctly.
	dialFetch := func() error {
		if _, override, gen := c.creds.peekOverride(); override {
			// A caller override (UpdateToken) wins this dial over a TokenSource fetch:
			// the store already holds the caller's credential, so skip the fetch and
			// let the dial read it via snapshot. This discharges B1's deferred clobber.
			// The override is NOT consumed here — a dial failure would then lose it to
			// a resumed fetch — but on the handshake-success signal (authEpochUp).
			tokenFailCount = 0
			lastFetchAt = c.clock.Now() // pace the next fetch as if we had fetched now
			dialOverride = true
			dialOverrideGen = gen
			return nil
		}
		dialOverride = false
		lastFetchAt = c.clock.Now()
		tok, err := c.fetchToken(ownerCtx)
		if err != nil {
			if ownerCtx.Err() != nil {
				return err // a fetch aborted by owner shutdown is not a counted failure
			}
			tokenFailCount++
			attempt := tokenFailCount
			// Emit BEFORE replying so *TokenSourceError lands ahead of the
			// StateChange→reconnecting the classified dial error will cause. No
			// terminateTokenSourceExhausted here — the dial path is non-terminal.
			c.ownerSurface(ownerCtx, &TokenSourceError{Attempt: attempt, Cause: err})
			return err
		}
		// Reset on ANY success (either context), matching FR-005's "a credential it
		// knows is dead" framing and the connected path: a successful dial fetch
		// clears the strikes, so ADR-0009's "sit at Max across a reconnect then
		// terminate on the next connected failure" is unreachable by construction —
		// the successful fetch that enabled the reconnect already reset the counter.
		tokenFailCount = 0
		// A TokenSource-resolved credential is a non-override write: it does not claim
		// precedence over a later UpdateToken, and it no-ops if a caller override
		// raced this fetch (that credential wins).
		c.creds.setTokenFromSource(tok.Value)
		if !tok.Expiry.IsZero() {
			// Token.Expiry arms the proactive schedule now, without waiting for an
			// auth_ack the handshake never sends — a TokenSource client's dial has no
			// other initial schedule.
			ownerExpiry = tok.Expiry
			armProactive()
		}
		return nil
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
				// A refresh that PRESENTED a caller override is now answered (ack =
				// accepted, auth_error = rejected on the merits): consume the override so
				// TokenSource resumes, gen-guarded so a newer UpdateToken racing the
				// answer survives. Consuming at answer-time — not at send — is what keeps
				// an unanswered epoch death from losing the caller's credential.
				if flightOverride {
					c.creds.clearOverrideIfGen(flightOverrideGen)
					flightOverride = false
				}
			}
			if hasAck {
				exp = ackExp
				// At-ack commit: a non-empty inFlightJWT means the answered flight was
				// an escalation, so commit its JWT — gen-guarded at the Escalate CALL, so
				// a later UpdateToken (whose ack-latency window this spans) is not
				// clobbered (latest caller wins). A committed escalation clears any stale
				// override; a refresh leaves inFlightJWT empty and commits nothing here.
				if inFlightJWT != "" {
					c.creds.commitEscalationIfGen(inFlightJWT, inFlightJWTGen)
					// A successful escalation re-issues `subscribe` for the newly-permitted
					// delta (the retained denial subset): signal the serializer to
					// re-subscribe its pending set. Unconditional — even when the
					// gen-guarded commit above no-ops (a newer UpdateToken raced the ack),
					// the SERVER acked this escalation, so its permissions changed regardless
					// of the local store race; the resubscribe tracks server state, not store
					// state. The serializer recomputes pending (desired − granted) at service
					// time, so with no retained denials the resume is a vacuous no-op.
					c.resumeSubscribeSerializer()
				}
				inFlightJWT = ""
				armProactive()
			}
			if hasError {
				inFlightJWT = "" // a rejected escalation: discard the uncommitted JWT
				armReactive()
			}
			advance()
		case <-c.authEpochReset:
			// The epoch ended before an answer arrived: abandon the outstanding
			// flight — its answer will never come on the new connection. A refresh
			// still WANTED (pending) survives and is retried on the reconnect;
			// clearing it here would race a post-reconnect caller call and drop it.
			//
			// An IN-FLIGHT escalation needs more than a refresh: its JWT is committed
			// only at-ack (never reaching the store), so unlike a refresh the reconnect
			// dial does NOT re-present it — it would be silently lost. Restore it as a
			// wanted escalation so epoch-up re-sends it on the new connection (a server
			// re-ack of an already-escalated connection is idempotent). Guard on
			// !pendingEscalation so a NEWER Escalate's JWT (latest-wins) is never
			// overwritten by this stale in-flight one.
			//
			// Drain the inbox so a stale answer from the dead epoch cannot later clear
			// a fresh flight — and, for an escalation, so a genuine same-epoch ack that
			// races this reset is superseded by the idempotent re-send above rather
			// than silently dropped. The refresh SCHEDULE persists (the expiry is
			// unchanged by a reconnect), so the timer runs on.
			inFlight = false
			// A refresh that presented a caller override was never answered → do NOT
			// consume the override (it survives and is re-presented on the reconnect);
			// just drop the per-flight tracking flag.
			flightOverride = false
			if inFlightJWT != "" && !pendingEscalation {
				pendingEscalation = true
				escalationJWT = inFlightJWT
				escalationGen = inFlightJWTGen
			}
			inFlightJWT = ""
			c.authInbox.drain()
			advance() // a still-wanted auth: no-ops while conn is nil, retried on epoch-up (or sends here if a fast reconnect already installed a live epoch)
		case <-c.authEpochUp:
			// A new epoch is connected (handshake success). If the dial presented a
			// caller override, that credential is now successfully presented — consume
			// the override so the TokenSource resumes, gen-guarded so a newer
			// UpdateToken racing the handshake is not lost.
			if dialOverride {
				c.creds.clearOverrideIfGen(dialOverrideGen)
				dialOverride = false
			}
			// Retry a refresh wanted while disconnected (e.g. the proactive timer fired
			// during backoff and could not send).
			advance()
		case req := <-c.authDialCred:
			// A supervisor pre-dial ensure-token request (B1). Between epochs the
			// conn is nil, so no connected refresh is in flight to race; the owner is
			// the sole TokenSource caller. Reply on the buffered chan so this never
			// blocks even if the supervisor abandoned the wait on a cancel.
			//
			// Drain a STALE epoch-up from the previous epoch before dialing (F2): the
			// supervisor is the sole authEpochUp sender and is parked here in the
			// rendezvous, so any queued epoch-up provably predates this dial and must
			// not later be mis-attributed to it (which would prematurely consume this
			// dial's override). Its advance-retry role is safe to drop — conn is nil
			// now so advance would no-op, and the new handshake sends a fresh epoch-up.
			// The drain MUST precede the reply, before the supervisor can send a fresh
			// epoch-up from the new dial's handshake.
			select {
			case <-c.authEpochUp:
			default:
			}
			req.reply <- dialFetch()
		case <-c.authEscalateCmd:
			// A caller Escalate: take the latest JWT and its captured generation
			// (latest-wins) and try to send it.
			if jwt, gen, ok := c.escalateBox.take(); ok {
				pendingEscalation = true
				escalationJWT = jwt
				escalationGen = gen
			}
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

// recoverAuthOwner is the auth-owner's first defer (§V/§VII: no bare recover). A
// panic on this goroutine — from a frame send, a timer arm, or a delivery send
// that runs inline here — leaves the credential/refresh subsystem dead. Since B1
// the supervisor's pre-dial ensure-token depends on the owner being alive, so a
// silently-dead owner would wedge every future dial in StateReconnecting forever.
// Route the panic to a terminal: record the cause, then cancel the client
// lifetime. The owner never calls terminalSequence itself (single-terminator
// rule); canceling root unblocks the supervisor's CURRENT wait — the ensure-token
// rendezvous, a backoff, a dial, or an epoch read — exactly as Close does, and the
// supervisor's exit runs terminalSequence with the cause already set, so *Terminal
// and Err() carry the *InternalError rather than a false clean stop.
func (c *Client) recoverAuthOwner() {
	r := recover()
	if r == nil {
		return
	}
	ie := &InternalError{
		Op:    "auth-owner",
		Value: fmt.Sprint(r),
		Stack: string(debug.Stack()),
	}
	c.cfg.logger.Error("sukko: auth-owner panic", "value", fmt.Sprint(r))
	c.setErrIfNil(ie)
	c.setTerminalCauseIfNil(ie)
	c.rootCancel()
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
