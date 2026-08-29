package sukko

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// restPublishPath is the gateway's REST publish endpoint (gateway.openapi.yaml).
const restPublishPath = "/api/v1/publish"

// restMaxErrorBody bounds how much of a response body is read — enough for a 200
// {status, channel} or an error {code, message}, capped so a misbehaving proxy cannot
// stream unboundedly into a decoded value or an error string.
const restMaxErrorBody = 64 * 1024

// PublishResult is the outcome of a successful RESTPublish. The gateway's 200 body also
// carries a const status ("accepted") which is dropped — the caller cannot act on it.
//
// Mid is the stable message identity the gateway assigned to the published
// message — the same value subscribers receive on the delivered Message.Mid, so
// a publisher can correlate its publish with the delivery. Empty when the publish
// fanned out to multiple topics (each produced message gets its own mid) or on a
// gateway that predates the field.
type PublishResult struct {
	Channel string
	Mid     string
}

// restPublishResponse decodes the gateway's 200 publish body. The body also carries
// a const status ("accepted") which is not decoded — the caller cannot act on it.
type restPublishResponse struct {
	Channel string `json:"channel"`
	Mid     string `json:"mid"`
}

// errorEnvelope is the gateway's {code, message} error body, shared by every non-2xx.
type errorEnvelope struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// RESTPublish publishes over HTTP and returns a genuine synchronous result — HTTP is
// request/response, so unlike the fire-and-forget WebSocket Publish this call returns
// the outcome (or a typed error) directly and never enqueues anything on Messages()
// (FR-007). It needs no WebSocket connection; a closed client returns ErrClosed. The
// shared local pre-check (object, size, credential class) runs first, before any
// network I/O, so a bad payload never reaches the gateway.
//
// Non-200 responses map to typed errors: 403 EDITION_LIMIT ⇒ *EditionRequiredError, 429
// ⇒ *RateLimitError (with any Retry-After parsed), 413 ⇒ ErrPayloadTooLarge (the server
// backstop for the local size check), and every other coded response ⇒ *PublishError
// carrying the gateway's code.
func (c *Client) RESTPublish(ctx context.Context, channel string, data any) (*PublishResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("sukko: rest publish: %w", err)
	}
	payload, err := c.prepublishCheck(data)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	closed := c.closed || c.state == StateClosed || c.state == StateError
	c.mu.Unlock()
	if closed {
		return nil, ErrClosed
	}

	reqURL, header, err := c.restRequestTarget(restPublishPath)
	if err != nil {
		return nil, err
	}
	body, err := json.Marshal(publishPayload{Channel: channel, Data: payload})
	if err != nil {
		return nil, fmt.Errorf("sukko: rest publish: %w", err) // unreachable: string + raw JSON
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, reqURL, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("sukko: rest publish: %w", c.redactor.redactError(err))
	}
	req.Header = header
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.cfg.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sukko: rest publish: %w", c.redactor.redactError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	// A read error is left to the mapping below: a truncated 200 then fails the JSON
	// decode, and a truncated error body still maps by HTTP status — both surface a
	// useful error rather than a panic (§III).
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, restMaxErrorBody))

	if resp.StatusCode == http.StatusOK {
		var r restPublishResponse
		if err := json.Unmarshal(respBody, &r); err != nil {
			return nil, fmt.Errorf("sukko: rest publish: decoding the response: %w", err)
		}
		return &PublishResult{Channel: r.Channel, Mid: r.Mid}, nil
	}
	return nil, c.restPublishError(resp.StatusCode, respBody, resp.Header)
}

// restRequestTarget builds the request URL and auth headers for a REST path, mirroring
// the WebSocket dial's credential application (transport_ws.go applyCredential): query
// params under WithQueryParamAuth, otherwise Authorization / X-API-Key headers.
func (c *Client) restRequestTarget(path string) (string, http.Header, error) {
	base, err := restBaseURL(c.url)
	if err != nil {
		return "", nil, err
	}
	reqURL := base + path
	if c.cfg.noAuth {
		// Auth-disabled deployment: send bare, mirroring the WS transport's nil-credentials
		// gate (applyCredential), so a stray UpdateToken can't diverge the two surfaces.
		return reqURL, http.Header{}, nil
	}
	token, apiKey := c.creds.snapshot()
	if c.cfg.queryParamAuth {
		reqURL, err = authQueryURL(reqURL, token, apiKey)
		if err != nil {
			return "", nil, err
		}
		return reqURL, http.Header{}, nil
	}
	h := http.Header{}
	if token != "" {
		h.Set(headerAuthorization, authBearerPrefix+token)
	}
	if apiKey != "" {
		h.Set(headerAPIKey, apiKey)
	}
	return reqURL, h, nil
}

// restBaseURL derives the HTTP origin from the client's ws(s):// URL — the inverse of
// the http→ws normalization done at construction — so REST calls reach the same origin
// the WebSocket dials. Path, query, and fragment are dropped; the endpoint path is
// appended by the caller.
func restBaseURL(wsURL string) (string, error) {
	u, err := url.Parse(wsURL)
	if err != nil {
		return "", fmt.Errorf("sukko: parsing the client URL: %w", err)
	}
	switch u.Scheme {
	case "wss":
		u.Scheme = "https"
	case "ws":
		u.Scheme = "http"
	}
	u.Path, u.RawQuery, u.Fragment = "", "", ""
	return u.String(), nil
}

// restPublishError maps a non-200 publish response to its typed error (FR-010). It keys
// on the body `code` so the two 403s (FORBIDDEN vs EDITION_LIMIT) are discriminated by
// struct type. A body that is missing or unparseable still maps by HTTP status.
func (c *Client) restPublishError(status int, body []byte, header http.Header) error {
	var env errorEnvelope
	_ = json.Unmarshal(body, &env)
	// A JSON-speaking proxy can echo the request target — which under WithQueryParamAuth
	// carries the token — into the error body, so route the server-echoed strings through
	// the redactor before they land in a returned error (§IX/§V).
	env.Code = c.redactor.redact(env.Code)
	env.Message = c.redactor.redact(env.Message)
	switch {
	case env.Code == "EDITION_LIMIT":
		// Legacy-gateway compatibility: REST publish is ungated post-remap, so a
		// current gateway never returns EDITION_LIMIT here. An older gateway that
		// still gates publish gated it at Pro, and the 403 body carries only
		// {code, message}, so the required edition is supplied from that static
		// knowledge rather than read from the wire.
		return &EditionRequiredError{Code: env.Code, RequiredEdition: EditionPro}
	case status == http.StatusTooManyRequests || env.Code == "RATE_LIMITED":
		// Retry-After is parsed from any 429, including a proxy/CDN in front of the
		// gateway; nil when absent, a non-nil zero when the header says "0" (retry now).
		return &RateLimitError{RetryAfter: parseRetryAfter(header.Get("Retry-After"), c.clock.Now())}
	case status == http.StatusRequestEntityTooLarge || env.Code == "BODY_TOO_LARGE":
		return ErrPayloadTooLarge
	default:
		code := env.Code
		if code == "" {
			code = fmt.Sprintf("HTTP_%d", status)
		}
		return &PublishError{Code: code, Message: env.Message}
	}
}
