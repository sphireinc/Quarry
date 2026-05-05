# Quarry Integration Guide

This file shows Quarry working end to end: core SQL building, scanning, and codex recipes.

```go
import (
	"context"
	"database/sql"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/codex"
	"github.com/sphireinc/quarry/scan"
)
```

Example types:

```go
type User struct {
	ID        int    `db:"id"`
	Email     string `db:"email"`
	CreatedAt string `db:"created_at"`
}

type UserSearchParams struct {
	Search string
	Status *string
}
```

## Core Builder

```go
qq := quarry.New(quarry.Postgres)

q := qq.Select("id", "email", "created_at").
	From("users").
	Where(
		quarry.Eq("tenant_id", 42),
		quarry.OptionalILike("email", "%bob%"),
	).
	OrderBySafeDefault("newest", quarry.SortMap{
		"newest": "created_at DESC",
		"email":  "email ASC",
	}, "newest").
	Page(1, 50)

sql, args, err := q.ToSQL()
if err != nil {
	panic(err)
}
```

## Scanning

```go
type User struct {
	ID        int    `db:"id"`
	Email     string `db:"email"`
	CreatedAt string `db:"created_at"`
}

users, err := scan.All[User](ctx, db, q)
if err != nil {
	panic(err)
}
```

## Codex

```go
cx := codex.New()

if err := cx.AddRawNamed("users.by_id", `SELECT id, email, created_at FROM users WHERE id = :id`); err != nil {
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

q := cx.MustRecipe("users.search").Build(qq, UserSearchParams{
	Search: "%bob%",
})

users, err := scan.All[User](ctx, db, q)
if err != nil {
	panic(err)
}
```

## Raw Query Binding

```go
raw := cx.MustRaw("users.by_id").With(qq).BindMap(map[string]any{
	"id": 10,
})

sql, args, err := raw.ToSQL()
if err != nil {
	panic(err)
}
```
