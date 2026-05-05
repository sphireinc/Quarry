package codex

import (
	"fmt"
	"strings"
	"testing"

	"github.com/sphireinc/quarry"
)

type userStatusParams struct {
	TenantID int    `db:"tenant_id"`
	Status   string `db:"status"`
	Extra    string `db:"ignored"`
}

type searchParams struct {
	TenantID int     `db:"tenant_id"`
	Search   string  `db:"search"`
	Status   *string `db:"status"`
}

func TestRawQueryStorageAndRetrieval(t *testing.T) {
	cx := New()
	if err := cx.AddRaw("users.active_count", "SELECT COUNT(*) FROM users WHERE tenant_id = ? AND status = ?"); err != nil {
		t.Fatalf("raw: %v", err)
	}

	got, ok := cx.Get("users.active_count")
	if !ok {
		t.Fatal("expected stored query")
	}
	if got.Name() != "users.active_count" {
		t.Fatalf("name mismatch: %s", got.Name())
	}

	raw := cx.Must("users.active_count")
	if raw.Name() != "users.active_count" {
		t.Fatalf("name mismatch: %s", raw.Name())
	}
}

func TestPositionalRewrite(t *testing.T) {
	for _, tc := range []struct {
		name string
		d    quarry.Dialect
		sql  string
	}{
		{name: "postgres", d: quarry.Postgres, sql: "SELECT COUNT(*) FROM users WHERE tenant_id = $1 AND status = $2"},
		{name: "mysql", d: quarry.MySQL, sql: "SELECT COUNT(*) FROM users WHERE tenant_id = ? AND status = ?"},
		{name: "sqlite", d: quarry.SQLite, sql: "SELECT COUNT(*) FROM users WHERE tenant_id = ? AND status = ?"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cx := New()
			if err := cx.AddRaw("users.active_count", "SELECT COUNT(*) FROM users WHERE tenant_id = ? AND status = ?"); err != nil {
				t.Fatalf("raw: %v", err)
			}

			sqlText, args, err := cx.MustRaw("users.active_count").With(quarry.New(tc.d)).Bind(10, "active").ToSQL()
			if err != nil {
				t.Fatalf("to sql: %v", err)
			}
			if sqlText != tc.sql {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", tc.sql, sqlText)
			}
			if fmt.Sprint(args) != fmt.Sprint([]any{10, "active"}) {
				t.Fatalf("args mismatch: %#v", args)
			}
		})
	}
}

func TestNamedRewriteAndBinding(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.by_status", `SELECT id, email
FROM users
WHERE tenant_id = :tenant_id
  AND status = :status`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	sqlText, args, err := cx.MustRaw("users.by_status").With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"tenant_id": 42,
		"status":    "active",
		"ignored":   "extra",
	}).ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT id, email\nFROM users\nWHERE tenant_id = $1\n  AND status = $2" {
		t.Fatalf("sql mismatch:\n%s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{42, "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestNamedRewriteIgnoresPostgresCast(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.cast", `SELECT now()::date AS today, tenant_id
FROM users
WHERE tenant_id = :tenant_id`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	sqlText, args, err := cx.MustRaw("users.cast").With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"tenant_id": 42,
	}).ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT now()::date AS today, tenant_id\nFROM users\nWHERE tenant_id = $1" {
		t.Fatalf("sql mismatch:\n%s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{42}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestNamedMissingParameter(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.by_status", `SELECT * FROM users WHERE tenant_id = :tenant_id AND status = :status`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	_, _, err := cx.MustRaw("users.by_status").With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"tenant_id": 42,
	}).ToSQL()
	if err == nil || !strings.Contains(err.Error(), `named parameter "status" missing`) {
		t.Fatalf("expected missing parameter error, got %v", err)
	}
}

func TestStructBinding(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.by_status", `SELECT * FROM users WHERE tenant_id = :tenant_id AND status = :status`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	sqlText, args, err := cx.MustRaw("users.by_status").With(quarry.New(quarry.Postgres)).BindStruct(userStatusParams{
		TenantID: 10,
		Status:   "active",
		Extra:    "ignored",
	}).ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT * FROM users WHERE tenant_id = $1 AND status = $2" {
		t.Fatalf("sql mismatch: %s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{10, "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestRecipeBuild(t *testing.T) {
	recipe := NewRecipe(func(qq *quarry.Quarry, p searchParams) quarry.SQLer {
		return qq.Select("id", "email").From("users").Where(
			quarry.Eq("tenant_id", p.TenantID),
			quarry.OptionalILike("email", p.Search),
			quarry.OptionalEq("status", p.Status),
		)
	})

	active := "active"
	sqler := recipe.Build(quarry.New(quarry.Postgres), searchParams{
		TenantID: 42,
		Search:   "%bob%",
		Status:   &active,
	})
	sqlText, args, err := sqler.ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT id, email FROM users WHERE tenant_id = $1 AND email ILIKE $2 AND status = $3" {
		t.Fatalf("sql mismatch: %s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{42, "%bob%", "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestCodexStoresRawAndRecipe(t *testing.T) {
	cx := New()
	if err := cx.AddRecipe("users.search", func(qq *quarry.Quarry, p searchParams) quarry.SQLer {
		return qq.Select("id", "email").From("users").Where(
			quarry.Eq("tenant_id", p.TenantID),
			quarry.OptionalILike("email", p.Search),
			quarry.OptionalEq("status", p.Status),
		)
	}); err != nil {
		t.Fatalf("recipe: %v", err)
	}
	if err := cx.AddRawNamed("users.by_id", `SELECT id, email, created_at FROM users WHERE id = :id`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	recipe := cx.MustRecipe("users.search")
	active := "active"
	sqler := recipe.Build(quarry.New(quarry.Postgres), searchParams{
		TenantID: 42,
		Search:   "bob",
		Status:   &active,
	})
	sqlText, args, err := sqler.ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT id, email FROM users WHERE tenant_id = $1 AND email ILIKE $2 AND status = $3" {
		t.Fatalf("sql mismatch: %s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{42, "bob", "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	raw := cx.MustRaw("users.by_id").With(quarry.New(quarry.SQLite)).BindMap(map[string]any{"id": 10})
	sqlText, args, err = raw.ToSQL()
	if err != nil {
		t.Fatalf("raw to sql: %v", err)
	}
	if sqlText != "SELECT id, email, created_at FROM users WHERE id = ?" {
		t.Fatalf("sql mismatch: %s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{10}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}
