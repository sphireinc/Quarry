# Scan

`github.com/sphireinc/quarry/scan` is Quarry's optional lightweight result-scanning package.

It is intentionally small. It is not Hydra, and it is not an ORM.

## What it does

- renders a Quarry query and executes it through `database/sql`
- scans one row, many rows, or a maybe-one row result set
- maps columns to struct fields with `db` tags, `json` tag fallback, and snake_case fallback
- supports pointer fields and `sql.Null*` values
- ignores unknown columns by default

## What it does not do

- it does not infer schemas
- it does not generate CRUD statements
- it does not load relationships
- it does not track entity identity
- it does not manage lifecycle hooks

## Row helpers

- `scan.One[T]` returns exactly one row or an error
- `scan.MaybeOne[T]` returns `nil` when no rows are present
- `scan.All[T]` returns every row as a slice
- `scan.Exec` and `scan.Query` execute a Quarry query through `database/sql`

## Destination rules

- destination types must be pointers, structs, or scalar values supported by `database/sql`
- pointer type parameters are rejected
- nil database handles, nil queries, and nil contexts return explicit errors
- unknown columns are ignored, and missing destination fields are left untouched

## When to use Hydra instead

Use standalone [Hydra](https://github.com/sphireinc/Hydra) when you want richer hydration behavior outside Quarry.

Hydra is the larger companion project. Quarry does not depend on it.
