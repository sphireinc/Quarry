<p align="center">
  <img src="docs/assets/working_logo.png" alt="Quarry logo" width="400" />
</p>

<p align="center">
  <a href="https://github.com/sphireinc/quarry/actions/workflows/ci.yml">
    <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sphireinc/quarry/ci.yml?branch=main" />
  </a>
  <a href="LICENSE">
    <img alt="License" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" />
  </a>
  <a href="go.mod">
    <img alt="Go" src="https://img.shields.io/badge/go-1.23%2B-blue.svg" />
  </a>
  <a href="https://sphireinc.github.io/Quarry/">
    <img alt="Docs" src="https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg" />
  </a>
</p>

# Quarry

Quarry is a SQL composition toolkit for Go.

It lets you write SQL-shaped Go, compose filters safely, bind args predictably, and scan results cleanly.

No magic ORM. No forced codegen. No string-concat sadness.

## Why Quarry Exists

Quarry is for Go developers who want to keep SQL explicit without hand-rolling fragile query strings.

Use it to compose queries, add dynamic filters, map user-facing sort options safely, include raw SQL fragments with bound arguments, and scan results when you want a lightweight helper instead of a full ORM.

The API is intentionally small. Quarry stays explicit and hard to misuse. Every change must make SQL composition clearer, safer, or more reliable, not more magical.

For more detail, see:

- [docs/status.md](docs/status.md) for the current implementation snapshot
- [CHANGELOG.md](CHANGELOG.md) for release history
- [ROADMAP.md](ROADMAP.md) for project direction
- [docs/compatibility.md](docs/compatibility.md) for compatibility guarantees
- [INTEGRATION.md](INTEGRATION.md) for an end-to-end walkthrough

The docs site lives under [docs/](https://sphireinc.github.io/Quarry/).

## What Quarry Does

Quarry helps you build SQL with explicit Go code instead of brittle string concatenation.

- Fluent builders for `SELECT`, `INSERT`, `UPDATE`, and `DELETE`
- Safe helpers for tables, columns, aliases, and sort expressions
- Dialect-aware placeholder rendering for Postgres, MySQL, and SQLite
- Dynamic predicates for optional filters and conditional clauses
- Raw SQL fragments with bound arguments when you need to drop lower
- Optional scanning and Codex helpers for lightweight result handling and reusable query recipes

## What Quarry Does Not Do

Quarry is intentionally narrow. It helps compose SQL; it does not try to become your entire database layer.

Quarry has boundaries. Glorious, load-bearing boundaries.

- Not an ORM
- Not a code generator
- Not a full `sqlc` replacement
- Not a migration tool
- Not a schema modeling system
- Not a dialect abstraction for every database ever shipped

## When to Use Quarry

Use Quarry when raw SQL is still the mental model, but the query needs to be assembled safely.

Good fits:

- Dynamic filters
- Safe user-facing sorting
- Dialect-aware placeholders
- Explicit `SELECT`, `INSERT`, `UPDATE`, and `DELETE` builders
- Lightweight result scanning

Bad fits:

- Entity tracking
- Migrations
- Relationship loading
- Generated query code
- Automatic schema modeling
- Hiding SQL from the developer

## Safety Model

Quarry keeps SQL safety boring and explicit: values are bound, identifiers are trusted, and raw SQL stays your responsibility.

- Bind SQL values as args.
- Treat identifiers as code, not data.
- Never pass user-controlled identifiers directly into raw SQL.
- Use identifier helpers for trusted tables, columns, and aliases.
- Use `OrderBySafe` or `OrderBySafeDefault` for user-facing sort options.
- Use `Raw(...)` only when you need to drop to SQL directly.
- Do not treat `Raw(...)` as a sanitizer.
- Quarry does not make arbitrary SQL fragments safe automatically.

## Installation

```bash
go get github.com/sphireinc/quarry
```

## Quick Start

```go
package main

import "github.com/sphireinc/quarry"

func main() {
	qq := quarry.New(quarry.Postgres)

	query := qq.Select("id", "email").From("users").Where(quarry.Eq("status", "active"))

	sql, args, err := query.ToSQL()
	if err != nil {
		panic(err)
	}
}

// var:sql  = SELECT id, email FROM users WHERE status = $1
// var:args = []any{"active"}
```

## Dynamic Filters

Use optional predicates for search forms, API filters, and any query condition that should only appear when a value is present.

```go
type UserSearch struct {
	TenantID int
	Search   string
	Status   *string
	Page     int
	PerPage  int
}

q := qq.Select("id", "email", "created_at").
	From("users").
	Where(
		quarry.Eq("tenant_id", params.TenantID),
		quarry.Or(
			quarry.OptionalILike("email", params.Search),
			quarry.OptionalILike("name", params.Search),
		),
		quarry.OptionalEq("status", params.Status),
	).
	OrderBySafeDefault("newest", quarry.SortMap{
		"newest": "created_at DESC",
		"email":  "email ASC",
	}, "newest").
	Page(params.Page, params.PerPage)
```

## Safe Sorting

`OrderBySafe` and `OrderBySafeDefault` map user-facing sort options to trusted SQL fragments. User input selects from the map; it never becomes SQL directly.

```go
q := qq.Select("id", "email").
	From("users").
	OrderBySafeDefault("newest", quarry.SortMap{
		"newest": "created_at DESC",
		"email":  "email ASC",
	}, "newest")
```

## Partial Updates

Use `SetOptional` and `SetIf` to build explicit `UPDATE` statements from optional values without falling back to ad hoc SQL fragments.

```go
q := qq.Update("users").
	SetOptional("name", params.Name).
	SetOptional("email", params.Email).
	SetIf(params.Enabled != nil, "enabled", *params.Enabled).
	Where(quarry.Eq("id", params.ID))
```

## Raw SQL Escape Hatch

When raw SQL is the clearest option, use `Raw(...)` and keep values bound as args.

```go
q := qq.Select(quarry.Raw("COUNT(*) FILTER (WHERE status = ?)", "active")).
	From("users").
	Where(quarry.Raw("created_at >= ?", since))
```

Raw `?` placeholders are rewritten for the target dialect. The placeholder scanner skips strings, comments, quoted identifiers, and dollar-quoted bodies.

## Codex Reusable Query Store

Codex is Quarry's optional registry for reusable named queries and SQL-shaped recipes.

It stays close to SQL, keeps arguments bound, and is unrelated to OpenAI Codex. Different Codex. Fewer lawyers, hopefully.

```go
cx := codex.New()

if err := cx.AddRawNamed("users.by_id", `SELECT id, email FROM users WHERE id = :id`); err != nil {
	panic(err)
}

if err := cx.AddRecipe("users.search", codex.NewRecipe(func(qq *quarry.Quarry, p UserSearchParams) quarry.SQLer {
	return qq.Select("id", "email", "created_at").
		From("users").
		Where(
			quarry.OptionalILike("email", p.Search),
			quarry.OptionalEq("status", p.Status),
		)
})); err != nil {
	panic(err)
}

q, err := cx.MustRecipe("users.search").Build(qq, UserSearchParams{
	Search: "%bob%",
})
if err != nil {
	panic(err)
}
```

## Scanning

Quarry includes a small optional `scan` package for scanning query results into Go structs, slices, and scalar values.

```go
users, err := scan.All[User](ctx, db, q)
```

It supports:

- `db` tags
- `json` tag fallback
- `snake_case` fallback
- Pointer and nullable values
- Forgiving handling of unknown columns

For the scan contract, see [docs/scan.md](docs/scan.md).

For richer hydration workflows, use the standalone [`github.com/sphireinc/Hydra`](https://github.com/sphireinc/Hydra) project. See [docs/hydra.md](docs/hydra.md) for how Hydra fits alongside Quarry.

Quarry does not infer schemas, generate CRUD operations, track entities, or manage relationships.

## Dialects

Quarry currently supports:

- `quarry.Postgres`
- `quarry.MySQL`
- `quarry.SQLite`

Dialect handling includes:

- Placeholder rendering
- Identifier quoting
- `RETURNING` behavior
- `ILIKE` fallback behavior
- Postgres-only `ANY` support

See [docs/dialects.md](docs/dialects.md) for the full dialect matrix.

See [docs/compatibility.md](docs/compatibility.md) for versioning and compatibility policy, and [docs/reference/packages/](docs/reference/packages/) for package maps and import paths.

## Squirrel Migration Notes

Squirrel proved that explicit SQL composition is useful. Quarry follows that same general shape, but it is not built on Squirrel.

If you know Squirrel, Quarry should feel familiar: fluent builders, visible SQL, and predictable `sql, args` output.

### Basic code comparison

```go
// Squirrel:
q := sq.Select("id", "email").From("users").Where(sq.Eq{"status": "active"})

// Quarry:
q := qq.Select("id", "email").From("users").Where(quarry.Eq("status", "active"))
```


| Area              | Squirrel                                    | Quarry                                                  |
|-------------------|---------------------------------------------|---------------------------------------------------------|
| Core identity     | Fluent SQL generator                        | SQL composition toolkit                                 | 
| ORM?              | No                                          | No                                                      |
| Dynamic filters   | Possible, but user assembles the patterns   | First-class optional predicates                         |
| Safe sorting      | Mostly user-managed                         | `OrderBySafe` and `OrderBySafeDefault`                  |
| Dialect behavior  | Placeholder formats exist                   | Explicit dialect policy and docs                        |
| Identifier safety | Less central to the positioning             | Core part of the safety model                           |
| Raw SQL           | Supported through expressions and fragments | Explicit escape hatch with bound args and scanner rules |
| Scanning          | Not the main thing                          | Optional lightweight `scan` package                     |                   
| Named recipes     | Not the main thing                          | Optional `Codex` layer                                  |                      
| Project posture   | Mature ecosystem staple                     | Newer, docs-first, safety-first toolkit                 |


Quarry is conceptually close to Squirrel, but the differences are intentional:

- Quarry emphasizes dialect policy and identifier safety.
- Quarry has first-class optional predicates for dynamic filters.
- Quarry maps user-facing sort options to trusted SQL fragments.
- Quarry keeps raw SQL explicit and available.
- Quarry treats scanning and Codex as optional layers, not core magic.

The point is not to pretend Quarry invented fluent SQL composition. It did not. The point is to make the safety boundaries, dialect behavior, dynamic assembly patterns, and optional helper layers clearer from the start.

For a broader comparison with raw SQL, Squirrel, sqlc, GORM, and sqlx, see [docs/comparison.md](docs/comparison.md).

## Roadmap / Status

Quarry is useful today for explicit SQL composition, dynamic filters, safe sorting, raw SQL fragments with bound arguments, and optional scanning helpers.

See [docs/status.md](docs/status.md) for the current implementation snapshot, [CHANGELOG.md](CHANGELOG.md) for release history, and [ROADMAP.md](ROADMAP.md) for where Quarry is headed, and where it intentionally is not.

## Examples

The examples in `examples/` compile and show the intended API shapes:

- [`examples/basic_select`](examples/basic_select)
- [`examples/dynamic_filters`](examples/dynamic_filters)
- [`examples/partial_update`](examples/partial_update)
- [`examples/raw_sql_codex`](examples/raw_sql_codex)
- [`examples/scan_one`](examples/scan_one)
- [`examples/scan_many`](examples/scan_many)
- [`examples/scan_with_quarry_query`](examples/scan_with_quarry_query)

## License

Quarry is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
