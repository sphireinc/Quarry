# Contributing to Quarry

Quarry is a Go SQL composition toolkit for people who like raw SQL, but want cleaner dynamic query assembly. The project is intentionally small and explicit: SQL stays visible, values stay bound, and integrations live outside the core builder when possible.

## Project Positioning

- Quarry is not an ORM.
- Quarry is not codegen-first.
- Quarry should stay readable in code review.
- Quarry should keep the core builder independent from scan, hydration, and recipe helpers.

## Local Workflow

Run the standard checks with:

```bash
make check
```

The main targets are:

- `make fmt`
- `make fmt-write`
- `make vet`
- `make test`
- `make test-race`
- `make examples`
- `make staticcheck`
- `make vulncheck`
- `make tidy`
- `make distcheck`

`make fmt` checks formatting and fails on drift. Use `make fmt-write` if you want
to rewrite the files in place.

`make vet` runs on the non-integration package set so it stays lightweight and
does not chase the SQLite-only test dependencies used by the scan and hydra
packages. `make test` still covers those packages.

## Adding Dialect Behavior

- Keep dialect decisions centralized.
- Make placeholder rendering explicit and test it for every supported dialect.
- Add tests that show the exact SQL for PostgreSQL, MySQL, and SQLite when the behavior differs.
- Prefer small helper methods over implicit magic.

## Adding Builder Behavior

- Keep query output stable and easy to diff.
- Preserve the raw SQL escape hatch.
- Add tests for generated SQL and args.
- Keep identifiers and values conceptually separate.
- If a new builder feature can surprise a reader, document the rule in code comments and tests.

## Golden Tests

- Treat SQL strings as API output.
- Add golden coverage when a query shape becomes large enough that simple inline assertions become hard to read.
- Keep the expected SQL exact, including placeholder style and whitespace.
- Do not rely on a live database for core builder behavior unless the feature truly needs one.

## Status

See [docs/status.md](docs/status.md) for the current implementation state.
