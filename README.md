# Quarry

Quarry is a Go SQL composition toolkit for people who like raw SQL, but want safer dynamic query assembly.

## Basic Use

```go
qq := quarry.New(quarry.Postgres)

q := qq.Select("id", "email").
	From("users").
	Where(quarry.Eq("status", "active"))

sql, args, err := q.ToSQL()
if err != nil {
	panic(err)
}

// SQL:  SELECT id, email FROM users WHERE status = $1
// Args: []any{"active"}
```

## Dynamic Filters

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

## Scanning

```go
users, err := scan.All[User](ctx, db, q)
```

The scan layer stays separate from the core builder so you can still use Quarry as SQL generation only.
