# Compatibility

Quarry is a public v1.0.0 release with a deliberately small API surface. Compatibility here means keeping the library predictable for Go callers who rely on explicit SQL composition.

## Supported Go Versions

- The repository targets the Go version declared in `go.mod`.
- CI also exercises the current stable Go toolchain.

## Supported SQL Dialects

- Postgres
- MySQL
- SQLite

Those are the only dialects Quarry promises to render today. Other dialect names are treated as unsupported.

## Semantic Versioning Intent

Quarry uses semantic versioning for the public module path. In practice, that means:

- patch releases should fix bugs without changing documented behavior
- minor releases may add features while preserving existing behavior
- major releases may change public API or documented contracts

The public API should remain small. New surface area should be intentional, documented, and tested.

## What Counts As Breaking

The following are treated as breaking changes when they affect the documented public API:

- removing or renaming exported identifiers
- changing the return type or error behavior of exported methods
- changing placeholder order or argument order
- changing dialect support or unsupported-feature errors
- changing identifier quoting rules
- changing `RETURNING`, `ILIKE`, `ANY`, or raw placeholder rewriting behavior

## Guaranteed SQL Behavior

Quarry guarantees:

- placeholder order
- argument order
- documented dialect behavior
- identifier quoting behavior for supported dialects
- explicit errors for unsupported features

Quarry does not guarantee cosmetic SQL whitespace unless a behavior is covered by a golden test. Quarry does guarantee placeholder order, argument order, and documented dialect behavior.

## Not Guaranteed

Quarry does not guarantee:

- exact whitespace in SQL beyond what golden tests cover
- pretty formatting of raw fragments supplied by callers
- equivalence between dialects that behave differently in SQL
- unsupported features silently falling back to something else

## Notes

- `Page(page, perPage)` is part of the documented API behavior and normalizes invalid inputs.
- `OrderBySafe` and `OrderBySafeDefault` are intentionally strict about trusted sort fragments.
- Raw SQL fragments remain caller responsibility. Quarry binds arguments and rewrites placeholders, but it does not sanitize arbitrary SQL text.
