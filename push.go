package sukko

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Push subscription management over the gateway's REST surface (`/api/v1/push/*`).
//
// This SDK does not *receive* push — a device does. The use-case is registering a
// browser's Web Push subscription or a mobile app's device token on the user's behalf,
// then unregistering it. There is no list endpoint, so a caller persists the device id
// PushSubscribe returns.
//
// Editions: Web Push registration requires Pro; registering an android/ios device
// requires Enterprise. A gateway running with GATEWAY_PUSH_ENABLED=false exposes no push
// routes at all (ErrPushDisabled).
//
// Everything here reuses the same HTTP plumbing as RESTPublish — origin derivation and
// credential application via restRequestTarget, the injectable http client, and the
// shared closed-state gate — so there is one auth surface, not two.

const (
	pushSubscribePath = "/api/v1/push/subscribe"
	pushVAPIDPath     = "/api/v1/push/vapid-key"
)

// PushSubscribeOptions describes a device to register. The required fields depend on
// Platform: web needs Endpoint/P256dhKey/AuthSecret, android and ios need Token.
type PushSubscribeOptions struct {
	// Platform is the device platform (required).
	Platform PushPlatform
	// Channels are the tenant-prefixed channels the device should receive push for
	// (required, non-empty). Treated as opaque — not validated for tenant prefix here.
	Channels []string
	// Token is the FCM/APNs device token (required for android/ios).
	Token string
	// Endpoint is the Web Push endpoint URL (required for web).
	Endpoint string
	// P256dhKey is the Web Push P-256 DH key (required for web).
	P256dhKey string
	// AuthSecret is the Web Push auth secret (required for web).
	AuthSecret string
}

// pushSubscribeBody is the wire request. Platform-specific fields are omitempty so an
// android registration sends no empty web keys and vice versa.
type pushSubscribeBody struct {
	Platform   PushPlatform `json:"platform"`
	Channels   []string     `json:"channels"`
	Token      string       `json:"token,omitempty"`
	Endpoint   string       `json:"endpoint,omitempty"`
	P256dhKey  string       `json:"p256dh_key,omitempty"`
	AuthSecret string       `json:"auth_secret,omitempty"`
}

// pushSubscribeResponse decodes the 201 body. device_id is int64; Go decodes it exactly,
// so the value is safe above 2^53 without the string round-trip a JS client needs.
type pushSubscribeResponse struct {
	DeviceID int64 `json:"device_id"`
}

// pushVAPIDResponse decodes the VAPID public key.
type pushVAPIDResponse struct {
	PublicKey string `json:"public_key"`
}

// pushUnsubscribeBody is the DELETE request body. device_id rides as an unquoted int64.
type pushUnsubscribeBody struct {
	DeviceID int64 `json:"device_id"`
}

// PushSubscribe registers a device for push on the given channels and returns the
// gateway-assigned device id (persist it — there is no list endpoint to recover it).
//
// The option set is validated locally first (platform, channels, platform-specific
// fields); a bad set returns ErrInvalidPushOptions without any network I/O. A 403
// edition gate returns *EditionRequiredError naming the edition this platform needed
// (Pro for web, Enterprise for android/ios); a gateway without a push surface returns
// ErrPushDisabled; an outage ErrPushUnavailable.
func (c *Client) PushSubscribe(ctx context.Context, opts PushSubscribeOptions) (int64, error) {
	if err := validatePushOptions(opts); err != nil {
		return 0, err
	}
	//nolint:gosec // G117: auth_secret (and the device token) are Web Push / FCM
	// registration fields the gateway requires to register the device — sending
	// them IS the operation, not a credential leak. The redactor covers dial
	// credentials in errors (see pushError); these caller-supplied per-request
	// fields never enter a returned error because the gateway's error envelope is
	// {code, message} and does not echo the request body.
	body, err := json.Marshal(pushSubscribeBody(opts))
	if err != nil {
		return 0, fmt.Errorf("sukko: push subscribe: %w", err) // unreachable: plain fields
	}

	status, respBody, err := c.restDo(ctx, http.MethodPost, pushSubscribePath, body)
	if err != nil {
		return 0, err
	}
	if status == http.StatusCreated {
		var r pushSubscribeResponse
		if err := json.Unmarshal(respBody, &r); err != nil {
			return 0, fmt.Errorf("sukko: push subscribe: decoding the response: %w", err)
		}
		return r.DeviceID, nil
	}
	// The required edition is a property of the platform the caller chose, not read
	// from the 403 body (which carries only {code, message}): web ⇒ Pro, mobile ⇒
	// Enterprise. A Community caller trips the Pro route gate first, but the honest
	// answer is the edition the operation ultimately needs.
	return 0, c.pushError(status, respBody, editionForPlatform(opts.Platform))
}

// PushUnsubscribe removes a previously registered device by its id.
func (c *Client) PushUnsubscribe(ctx context.Context, deviceID int64) error {
	body, err := json.Marshal(pushUnsubscribeBody{DeviceID: deviceID})
	if err != nil {
		return fmt.Errorf("sukko: push unsubscribe: %w", err) // unreachable
	}
	status, respBody, err := c.restDo(ctx, http.MethodDelete, pushSubscribePath, body)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		return nil
	}
	return c.pushError(status, respBody, EditionPro)
}

// PushVAPIDKey fetches the tenant's Web Push VAPID public key, which a browser needs to
// create a Web Push subscription.
func (c *Client) PushVAPIDKey(ctx context.Context) (string, error) {
	status, respBody, err := c.restDo(ctx, http.MethodGet, pushVAPIDPath, nil)
	if err != nil {
		return "", err
	}
	if status == http.StatusOK {
		var r pushVAPIDResponse
		if err := json.Unmarshal(respBody, &r); err != nil {
			return "", fmt.Errorf("sukko: push vapid-key: decoding the response: %w", err)
		}
		return r.PublicKey, nil
	}
	return "", c.pushError(status, respBody, EditionPro)
}

// validatePushOptions runs the local pre-check. Channels are treated as opaque (the SDK
// never validates tenant prefixes; the gateway is the authority).
func validatePushOptions(opts PushSubscribeOptions) error {
	switch opts.Platform {
	case PlatformWeb:
		if opts.Endpoint == "" || opts.P256dhKey == "" || opts.AuthSecret == "" {
			return fmt.Errorf("%w: web requires endpoint, p256dh_key, and auth_secret", ErrInvalidPushOptions)
		}
	case PlatformAndroid, PlatformIOS:
		if opts.Token == "" {
			return fmt.Errorf("%w: %s requires a device token", ErrInvalidPushOptions, opts.Platform)
		}
	default:
		return fmt.Errorf("%w: platform must be web, android, or ios", ErrInvalidPushOptions)
	}
	if len(opts.Channels) == 0 {
		return fmt.Errorf("%w: at least one channel is required", ErrInvalidPushOptions)
	}
	return nil
}

// editionForPlatform is the edition a registration for the platform requires: web is
// Web Push (Pro), android/ios are mobile push (Enterprise).
func editionForPlatform(p PushPlatform) Edition {
	if p == PlatformAndroid || p == PlatformIOS {
		return EditionEnterprise
	}
	return EditionPro
}

// restDo executes one REST request against the gateway, applying credentials the same
// way RESTPublish does, and returns the status, a bounded body read, and the response
// header. A transport error is redacted (§IX). Shared by the push operations.
func (c *Client) restDo(ctx context.Context, method, path string, body []byte) (status int, respBody []byte, err error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, fmt.Errorf("sukko: push: %w", err)
	}
	c.mu.Lock()
	closed := c.closed || c.state == StateClosed || c.state == StateError
	c.mu.Unlock()
	if closed {
		return 0, nil, ErrClosed
	}

	reqURL, reqHeader, err := c.restRequestTarget(path)
	if err != nil {
		return 0, nil, err
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, reqURL, reader)
	if err != nil {
		return 0, nil, fmt.Errorf("sukko: push: %w", c.redactor.redactError(err))
	}
	req.Header = reqHeader
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.cfg.httpClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("sukko: push: %w", c.redactor.redactError(err))
	}
	defer func() { _ = resp.Body.Close() }()
	respBody, _ = io.ReadAll(io.LimitReader(resp.Body, restMaxErrorBody))
	return resp.StatusCode, respBody, nil
}

// pushError maps a non-success push response to its typed error. requiredEdition is
// supplied by the caller (per operation/platform) because the 403 body carries only a
// code, not the edition.
//
// §IX: the {code, message} envelope is routed through the redactor, masking the client's
// token if a proxy echoed the request target into the error. The per-request credential
// fields a caller passes to PushSubscribe (auth_secret, device token) are NOT registered
// with the shared redactor — it is immutable and seeded with the dial credentials — but
// the gateway's error envelope is {code, message} and does not echo the request body, so
// those fields have no path into a returned error here.
func (c *Client) pushError(status int, body []byte, requiredEdition Edition) error {
	// A 404 is the disabled-surface case (GATEWAY_PUSH_ENABLED=false): its body is Go's
	// mux plain text, not the JSON envelope, so it is handled before any parse.
	if status == http.StatusNotFound {
		return ErrPushDisabled
	}
	var env errorEnvelope
	_ = json.Unmarshal(body, &env)
	env.Code = c.redactor.redact(env.Code)
	env.Message = c.redactor.redact(env.Message)
	switch {
	case env.Code == "EDITION_LIMIT":
		return &EditionRequiredError{Code: env.Code, RequiredEdition: requiredEdition}
	case status == http.StatusServiceUnavailable || env.Code == "SERVICE_UNAVAILABLE":
		return ErrPushUnavailable
	default:
		code := env.Code
		if code == "" {
			code = fmt.Sprintf("HTTP_%d", status)
		}
		return &PushError{Code: code, Message: env.Message}
	}
}
