# Dialect Matrix

Quarry supports three dialects today:

- Postgres
- MySQL
- SQLite

This page is the blunt version of the contract. The builders do not pretend these dialects are equivalent.

## Matrix

| Behavior | Postgres | MySQL | SQLite |
| --- | --- | --- | --- |
| Placeholder style | `$1`, `$2`, ... | `?` | `?` |
| Identifier quote style | `"` | `` ` `` | `"` |
| `RETURNING` support | Supported | Unsupported | Supported |
| `ILIKE` behavior | Native `ILIKE` | `LOWER(lhs) LIKE LOWER(rhs)` fallback | `LOWER(lhs) LIKE LOWER(rhs)` fallback |
| `ANY` behavior | Native `= ANY(...)` | Unsupported | Unsupported |
| `LIMIT` / `OFFSET` behavior | Rendered as `LIMIT n OFFSET m` | Rendered as `LIMIT n OFFSET m` | Rendered as `LIMIT n OFFSET m` |
| Multi-row insert behavior | Supported with placeholder numbering | Supported with `?` placeholders | Supported with `?` placeholders |
| Raw SQL placeholder rewriting | `?` becomes `$n` outside quoted/commented regions | `?` stays `?` outside quoted/commented regions | `?` stays `?` outside quoted/commented regions |

## Notes

- Placeholder rewriting ignores single-quoted strings, double-quoted identifiers, line comments, block comments, and PostgreSQL dollar-quoted bodies.
- `ILIKE` is a PostgreSQL feature in Quarry. MySQL and SQLite fall back to a case-insensitive `LOWER(...) LIKE LOWER(...)` comparison.
- `ANY` is only supported on Postgres. Quarry returns an explicit error on other dialects.
- `RETURNING` is supported on Postgres and SQLite, and rejected on MySQL.
- `Page(page, perPage)` normalizes non-positive values to `Page(1, 50)`, which becomes `LIMIT 50 OFFSET 0`.
- `LimitDefault` and `OffsetDefault` only apply positive / non-negative values, otherwise they fall back to the provided fallback values.

## Where To Look In Code

- Dialect constants and feature flags: [`quarry.go`](../quarry.go)
- Placeholder rewriting: [`sqlbuilder.go`](../sqlbuilder.go) and [`internal/rawsql/rawsql.go`](../internal/rawsql/rawsql.go)
- `ILIKE` / `ANY` behavior: [`expr.go`](../expr.go)
- `RETURNING` behavior: [`builder_insert.go`](../builder_insert.go), [`builder_update.go`](../builder_update.go), and [`builder_delete.go`](../builder_delete.go)
- Pagination helpers: [`dynamic.go`](../dynamic.go)
