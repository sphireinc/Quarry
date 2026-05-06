# Quarry Status

Quarry is a public v1.0.0 release. The documented API is intentionally small, explicit, and meant to stay that way unless changes are intentional and versioned.

## Implemented

- Repository hygiene baseline:
  - `Makefile`
  - CI workflow
  - `CONTRIBUTING.md`
  - `docs/status.md`
- Root `quarry` package with dialect selection.
- Safe table and column identifier helpers with dialect-aware quoting.
- Raw SQL placeholder parsing with comment, string, and dollar-quote awareness.
- Core `Select`, `InsertInto`, `Update`, and `DeleteFrom` builders with joins, grouping, maps, and multi-row inserts.
- Dynamic predicates, optional filter helpers, tuple `IN`, `ANY`, `BETWEEN`, and `EXISTS` / `NOT EXISTS`.
- Separate `quarry/scan` execution helpers with forgiving struct hydration, tag fallback, pointer/null support, and nil guards.
- Separate `quarry/hydra` compatibility wrapper.
- Separate `quarry/codex` registry for raw queries and recipes.
- Hardened named query store with deterministic lookup, name validation, and optional strict parameter checking.
- README, integration guide, examples, comparison notes, package map, and Squirrel migration notes.
- A GitHub Pages-friendly docs site under `docs/` with landing, getting-started, guide, reference, and examples pages.
- Golden SQL tests, a CI matrix across OS and Go versions, and a Linux verification job with `make check`, `make examples`, `make distcheck`, `make staticcheck`, `make vulncheck`, and `make test-race`.
- Public API polish for the v1.0.0 release, including broader documentation coverage and safer codex recipe behavior.
- Unit tests for the implemented builder, scan, and codex behavior.

## Partial

- None currently known.

## Planned

- Any broader SQL feature work that would push Quarry toward ORM behavior.

## What This Means

- Quarry is useful today for explicit SQL composition and optional scanning helpers.
- Quarry is a v1.0.0 public release with an intentionally small API surface.
- Contributors should expect future changes only if new features or API refinements are intentionally added.
