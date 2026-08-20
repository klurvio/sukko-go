// Package sukko is the Go client SDK for the Sukko real-time WebSocket platform.
//
// It provides a single, context-driven Client for real-time market-data and event
// subscription: WebSocket and SSE transports, header-default JWT / API-key auth with
// automatic single-flight refresh, full gap recovery (reconnect-with-replay, advisory
// gap to live replay, and history), bounded back-pressure with no silent drops, REST
// publish, and push subscription management.
//
// The public surface and behavior derive from Sukko's authoritative contracts — the
// AsyncAPI v1.4.0 client-ws protocol and the gateway OpenAPI — never from a sibling
// SDK's current behavior. Channels are "{tenant}.{suffix}".
//
// This package is under active development; see the specs directory for the design.
package sukko
