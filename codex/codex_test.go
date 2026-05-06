package codex

import (
	"fmt"
	"reflect"
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

func TestStoreAddGetNames(t *testing.T) {
	store := NewStore()
	store.MustAdd("users.by_id", "SELECT * FROM users WHERE id = :id")
	store.MustAdd("users.by_status", "SELECT * FROM users WHERE status = :status")

	got, ok := store.Get("users.by_id")
	if !ok {
		t.Fatal("expected stored template")
	}
	if got.Name() != "users.by_id" {
		t.Fatalf("name mismatch: %s", got.Name())
	}

	if names := store.Names(); !reflect.DeepEqual(names, []string{"users.by_id", "users.by_status"}) {
		t.Fatalf("names mismatch: %#v", names)
	}
}

func TestStoreValidation(t *testing.T) {
	store := NewStore()

	if err := store.Add("users.by_id", "SELECT * FROM users WHERE id = :id"); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := store.Add("users.by_id", "SELECT * FROM users WHERE id = :id"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if err := store.Add("bad name", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "invalid query name") {
		t.Fatalf("expected invalid name error, got %v", err)
	}
	if err := store.Add("", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "query name is required") {
		t.Fatalf("expected missing name error, got %v", err)
	}
	if err := store.Add("users.empty", "   "); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("expected empty sql error, got %v", err)
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

func TestNamedRewriteRepeatsParameters(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.by_name", `SELECT * FROM users WHERE first_name = :name OR last_name = :name`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	sqlText, args, err := cx.MustRaw("users.by_name").With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"name": "bob",
	}).ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT * FROM users WHERE first_name = $1 OR last_name = $2" {
		t.Fatalf("sql mismatch:\n%s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"bob", "bob"}) {
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

func TestNamedRewriteIgnoresQuotedText(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.named", `SELECT ':status' AS literal, now()::date AS today, id
FROM users
WHERE status = :status
  AND id = :id`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	sqlText, args, err := cx.MustRaw("users.named").With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"status": "active",
		"id":     10,
	}).ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT ':status' AS literal, now()::date AS today, id\nFROM users\nWHERE status = $1\n  AND id = $2" {
		t.Fatalf("sql mismatch:\n%s", sqlText)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"active", 10}) {
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

func TestNamedInvalidSyntax(t *testing.T) {
	cx := New()
	if err := cx.AddRawNamed("users.bad", `SELECT * FROM users WHERE id = :1bad`); err != nil {
		t.Fatalf("raw named: %v", err)
	}

	_, _, err := cx.MustRaw("users.bad").With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"1bad": 42,
	}).ToSQL()
	if err == nil || !strings.Contains(err.Error(), "invalid named parameter syntax") {
		t.Fatalf("expected invalid syntax error, got %v", err)
	}
}

func TestStoreStrictModeRejectsUnusedParams(t *testing.T) {
	store := NewStore().SetStrict(true)
	store.MustAdd("users.by_status", `SELECT * FROM users WHERE status = :status`)

	tmpl, ok := store.Get("users.by_status")
	if !ok {
		t.Fatal("expected stored template")
	}

	_, _, err := tmpl.With(quarry.New(quarry.Postgres)).BindMap(map[string]any{
		"status": "active",
		"unused": "extra",
	}).ToSQL()
	if err == nil || !strings.Contains(err.Error(), `named parameter "unused" unused`) {
		t.Fatalf("expected strict unused error, got %v", err)
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
	sqler, err := recipe.Build(quarry.New(quarry.Postgres), searchParams{
		TenantID: 42,
		Search:   "%bob%",
		Status:   &active,
	})
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
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

func TestRecipeBuildErrors(t *testing.T) {
	t.Run("nil_recipe", func(t *testing.T) {
		var recipe Recipe[searchParams]
		if _, err := recipe.Build(quarry.New(quarry.Postgres), searchParams{}); err == nil || !strings.Contains(err.Error(), "recipe is nil") {
			t.Fatalf("expected nil recipe error, got %v", err)
		}
	})

	t.Run("nil_quarry", func(t *testing.T) {
		recipe := NewRecipe(func(qq *quarry.Quarry, p searchParams) quarry.SQLer {
			return qq.Select("id").From("users")
		})
		sqler, err := recipe.Build(nil, searchParams{})
		if err == nil || !strings.Contains(err.Error(), "returned nil query") && !strings.Contains(err.Error(), "nil quarry") {
			t.Fatalf("expected nil quarry error, got %v", err)
		}
		if sqler != nil {
			t.Fatalf("expected nil sqler, got %#v", sqler)
		}
	})

	t.Run("wrong_param_type", func(t *testing.T) {
		cx := New()
		if err := cx.AddRecipe("users.search", func(qq *quarry.Quarry, p searchParams) quarry.SQLer {
			return qq.Select("id").From("users")
		}); err != nil {
			t.Fatalf("recipe: %v", err)
		}
		if _, err := cx.MustRecipe("users.search").Build(quarry.New(quarry.Postgres), "wrong"); err == nil || !strings.Contains(err.Error(), "received string") {
			t.Fatalf("expected type mismatch error, got %v", err)
		}
	})
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
	sqler, err := recipe.Build(quarry.New(quarry.Postgres), searchParams{
		TenantID: 42,
		Search:   "bob",
		Status:   &active,
	})
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
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
