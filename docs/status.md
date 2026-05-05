# Quarry Status

Quarry is under active implementation. This file is intentionally honest about what exists today versus what is still planned.

## Implemented

- Root `quarry` package with dialect selection.
- Core `Select`, `InsertInto`, `Update`, and `DeleteFrom` builders.
- Dynamic predicates and optional filter helpers.
- Separate `quarry/scan` execution helpers.
- Separate `quarry/hydra` compatibility wrapper.
- Separate `quarry/codex` registry for raw queries and recipes.
- README and integration examples.
- Unit tests for the implemented builder, scan, and codex behavior.

## Partial

- Identifier safety and quoting.
- Raw SQL placeholder parsing beyond the simple `?` and `:name` rules.
- Builder feature breadth such as joins, group by, having, maps, and richer inserts.
- Advanced predicate forms such as tuple `IN`, `ANY`, `BETWEEN`, `EXISTS`, and `NOT EXISTS`.
- Dialect policy centralization.
- Nil-safety and error-handling cleanup.
- Public API polish and naming consistency.

## Planned

- Repository hygiene follow-up work tracked in the numbered planning files.
- Golden test expansion and CI matrix hardening.
- Documentation expansion for migration and coexistence guides.
- Any broader SQL feature work that would push Quarry toward ORM behavior.

## What This Means

- Quarry is useful today for explicit SQL composition and optional scanning helpers.
- Quarry is not yet finished as a polished v0.1.0 library.
- Contributors should expect the repository to keep changing as the remaining planning files are completed.
