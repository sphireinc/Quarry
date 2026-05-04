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
