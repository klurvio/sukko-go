# Vendored contracts

These are the authoritative Sukko API contracts, copied verbatim from the
platform repository. They are the referent the SDK's conformance tests verify
against.

| File | Source |
|---|---|
| `client-ws.asyncapi.yaml` | `../sukko/ws/docs/asyncapi/client-ws.asyncapi.yaml` |
| `gateway.openapi.yaml` | `../sukko/ws/docs/openapi/gateway.openapi.yaml` |

## Why these are vendored rather than read in place

This SDK is released independently and its CI checks out no other repository, so
"test against the contract" needs a referent that lives inside this module. A
test that reached across a module boundary would fail here whenever an unrelated
edit landed in the platform's docs, and would not run at all in a clean clone.

Vendoring the *published contract document* is not the copying the SDK's
contract rules forbid. That prohibition targets the server-internal protocol
package — server implementation detail, unexported by Go's own `internal/` rule.
These files are the public interface description, which is precisely what a
client is supposed to build against.

## Why the checksums

`CHECKSUMS` is verified at the start of the conformance test run. Without it a
stale vendored copy is invisible: the fixtures still round-trip, the coverage
test still passes, and the SDK quietly conforms to a contract the server no
longer speaks.

The checksum converts that silence into a loud failure — the same idea as
`go.sum`, applied to a document.

## Refreshing

When the platform's contracts change:

1. Copy both files from `../sukko/ws/docs/` into this directory.
2. Regenerate the checksums:
   ```sh
   cd testdata/contracts && shasum -a 256 client-ws.asyncapi.yaml gateway.openapi.yaml > CHECKSUMS
   ```
3. Run `go test -race ./...`. The completeness test will name any message type
   the SDK does not yet handle — that is the point of it, and a new type is
   feature work rather than a test to silence.
4. Commit the contracts and the checksums together. A checksum committed without
   its contract, or the reverse, leaves the tree in a state that fails for a
   reason unrelated to the change.
