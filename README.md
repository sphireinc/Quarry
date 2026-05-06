<p align="center">
  <img src="docs/assets/working_logo.png" alt="Quarry logo" width="104" />
</p>

<p align="center">
  <a href="https://github.com/sphireinc/Quarry/actions/workflows/ci.yml">
    <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/sphireinc/Quarry/ci.yml?branch=main" />
  </a>
  <a href="LICENSE">
    <img alt="License" src="https://img.shields.io/badge/license-Apache%202.0-blue.svg" />
  </a>
  <a href="go.mod">
    <img alt="Go" src="https://img.shields.io/badge/go-1.23%2B-blue.svg" />
  </a>
  <a href="docs/index.html">
    <img alt="Docs" src="https://img.shields.io/badge/docs-GitHub%20Pages-blue.svg" />
  </a>
</p>

# Quarry

Quarry is a SQL Composition Kit for Go.

> Write SQL-shaped Go. Compose filters safely. Bind args predictably. Scan results cleanly. No magic ORM. No forced codegen. No string-concat sadness.

See [docs/status.md](docs/status.md) for an honest implementation snapshot and [INTEGRATION.md](INTEGRATION.md) for an end-to-end walkthrough.

The GitHub Pages-style docs site lives under [docs/](docs/index.html).

## What Quarry Is

Quarry helps you build SQL with explicit Go code instead of brittle string concatenation.

- fluent builders for `SELECT`, `INSERT`, `UPDATE`, and `DELETE`
- safe identifier helpers for tables, columns, and aliases
- dialect-aware placeholder rendering
- optional dynamic predicates and raw SQL escape hatches
- optional scanning and codex helpers

## What Quarry Is Not

- not an ORM
- not a code generator
- not a full `sqlc` replacement
- not a dialect abstraction for every database ever shipped

## Installation

```bash
go get github.com/sphireinc/quarry
```

## Quick Start

```go
qq := quarry.New(quarry.Postgres)

q := qq.Select("id", "email").
	From("users").
	Where(quarry.Eq("status", "active"))

sql, args, err := q.ToSQL()
if err != nil {
	panic(err)
}

// SELECT id, email FROM users WHERE status = $1
// []any{"active"}
```

## Dynamic Filters

Optional predicates are the default way to assemble search forms and API filters.

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

`OrderBySafe` and `OrderBySafeDefault` only accept trusted fragments from a lookup table.

```go
q := qq.Select("id", "email").
	From("users").
	OrderBySafeDefault("newest", quarry.SortMap{
		"newest": "created_at DESC",
		"email":  "email ASC",
	}, "newest")
```

## Partial Updates

`SetOptional` and `SetIf` keep update statements explicit without forcing callers to build ad hoc SQL fragments.

```go
q := qq.Update("users").
	SetOptional("name", params.Name).
	SetOptional("email", params.Email).
	SetIf(params.Enabled != nil, "enabled", *params.Enabled).
	Where(quarry.Eq("id", params.ID))
```

## Raw SQL Escape Hatch

When a query is clearer as raw SQL, use `Raw(...)` and keep the values bound.

```go
q := qq.Select(quarry.Raw("COUNT(*) FILTER (WHERE status = ?)", "active")).
	From("users").
	Where(quarry.Raw("created_at >= ?", since))
```

Raw `?` placeholders are rewritten per dialect, and the scanner ignores strings, comments, quoted identifiers, and dollar-quoted bodies.

## Codex Reusable Query Store

Codex is for reusable named queries and recipes that stay close to SQL.

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

## Scanning / Hydration

The scan layer is optional.

```go
users, err := scan.All[User](ctx, db, q)
```

It supports:

- `db` tags
- `json` tag fallback
- snake_case fallback
- pointers and nullable values
- forgiving unknown columns

## Dialects

Quarry currently supports:

- `quarry.Postgres`
- `quarry.MySQL`
- `quarry.SQLite`

Dialect behavior covers:

- placeholder rendering
- identifier quoting
- `RETURNING` support
- `ILIKE` fallback behavior
- `ANY` support on Postgres only

## Squirrel Migration Notes

Squirrel showed that explicit SQL composition is useful. Quarry is inspired by that workflow, not built on Squirrel.

If you know Squirrel, the shape will feel familiar:

```go
// Squirrel-style sketch:
// q := sq.Select("id", "email").From("users").Where(sq.Eq{"status": "active"})

// Quarry:
q := qq.Select("id", "email").
	From("users").
	Where(quarry.Eq("status", "active"))
```

The differences matter:

- Quarry puts more emphasis on dialect policy and identifier safety.
- Quarry has first-class optional predicates for dynamic filters.
- Quarry keeps raw SQL explicit and available.
- Quarry documents scanning and codex as optional layers, not core magic.

## Roadmap / Status

Quarry is useful today for explicit SQL composition and optional scanning helpers, but it is still under active implementation.

See [docs/status.md](docs/status.md) for the current truth.

## Examples

The examples in `examples/` compile and show the intended API shapes:

- [`examples/basic_select`](examples/basic_select)
- [`examples/dynamic_filters`](examples/dynamic_filters)
- [`examples/partial_update`](examples/partial_update)
- [`examples/raw_sql_codex`](examples/raw_sql_codex)
- [`examples/scanning`](examples/scanning)

## License

Quarry is licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for the full text.
