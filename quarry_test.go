package quarry

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestSelectBuilder(t *testing.T) {
	dialects := []struct {
		name string
		d    Dialect
		p    string
	}{
		{name: "postgres", d: Postgres, p: "$"},
		{name: "mysql", d: MySQL, p: "?"},
		{name: "sqlite", d: SQLite, p: "?"},
	}

	for _, tc := range dialects {
		t.Run(tc.name, func(t *testing.T) {
			qq := New(tc.d)

			t.Run("basic", func(t *testing.T) {
				sql, args, err := qq.Select("id", "email").From("users").ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				wantSQL := "SELECT id, email FROM users"
				if sql != wantSQL {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})

			t.Run("no_columns", func(t *testing.T) {
				sql, args, err := qq.Select().From("users").ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if sql != "SELECT * FROM users" {
					t.Fatalf("sql mismatch: %s", sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})

			t.Run("where_grouped", func(t *testing.T) {
				sql, args, err := qq.Select("id", "email", "created_at").
					From("users").
					Where(
						Eq("tenant_id", 42),
						Or(
							ILike("email", "%bob%"),
							ILike("name", "%bob%"),
						),
					).
					OrderBy("created_at DESC").
					Limit(50).
					ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT id, email, created_at FROM users WHERE tenant_id = $1 AND (email ILIKE $2 OR name ILIKE $3) ORDER BY created_at DESC LIMIT 50",
					MySQL:    "SELECT id, email, created_at FROM users WHERE tenant_id = ? AND (LOWER(email) LIKE LOWER(?) OR LOWER(name) LIKE LOWER(?)) ORDER BY created_at DESC LIMIT 50",
					SQLite:   "SELECT id, email, created_at FROM users WHERE tenant_id = ? AND (LOWER(email) LIKE LOWER(?) OR LOWER(name) LIKE LOWER(?)) ORDER BY created_at DESC LIMIT 50",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if wantArgs := []any{42, "%bob%", "%bob%"}; fmt.Sprint(args) != fmt.Sprint(wantArgs) {
					t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", wantArgs, args)
				}
			})

			t.Run("raw_and_in", func(t *testing.T) {
				sql, args, err := qq.Select(Raw("COUNT(*) FILTER (WHERE status = ?)", "active")).
					From("users").
					Where(In("id", []int{1, 2, 3})).
					ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT COUNT(*) FILTER (WHERE status = $1) FROM users WHERE id IN ($2, $3, $4)",
					MySQL:    "SELECT COUNT(*) FILTER (WHERE status = ?) FROM users WHERE id IN (?, ?, ?)",
					SQLite:   "SELECT COUNT(*) FILTER (WHERE status = ?) FROM users WHERE id IN (?, ?, ?)",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if wantArgs := []any{"active", 1, 2, 3}; fmt.Sprint(args) != fmt.Sprint(wantArgs) {
					t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", wantArgs, args)
				}
			})

			t.Run("optional_filters_omitted", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					Eq("tenant_id", 42),
					OptionalEq("status", ""),
					OptionalILike("email", ""),
					And(
						OptionalILike("name", ""),
						OptionalIn("id", []int{}),
					),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if sql != "SELECT * FROM users WHERE tenant_id = $1" && tc.d == Postgres {
					t.Fatalf("sql mismatch: %s", sql)
				}
				if tc.d != Postgres {
					if sql != "SELECT * FROM users WHERE tenant_id = ?" {
						t.Fatalf("sql mismatch: %s", sql)
					}
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{42}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("optional_filters_included", func(t *testing.T) {
				active := "active"
				sql, args, err := qq.Select("*").From("users").Where(
					Eq("tenant_id", 42),
					OptionalEq("status", &active),
					OptionalEq("enabled", false),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT * FROM users WHERE tenant_id = $1 AND status = $2 AND enabled = $3",
					MySQL:    "SELECT * FROM users WHERE tenant_id = ? AND status = ? AND enabled = ?",
					SQLite:   "SELECT * FROM users WHERE tenant_id = ? AND status = ? AND enabled = ?",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{42, "active", false}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("grouped_optional_children_collapse", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					And(
						Eq("tenant_id", 42),
						Or(
							OptionalILike("email", ""),
							OptionalILike("name", ""),
						),
					),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT * FROM users WHERE tenant_id = $1",
					MySQL:    "SELECT * FROM users WHERE tenant_id = ?",
					SQLite:   "SELECT * FROM users WHERE tenant_id = ?",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{42}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("safe_sort_and_page", func(t *testing.T) {
				sql, args, err := qq.Select("id", "email").From("users").
					OrderBySafeDefault("evil", SortMap{
						"newest": "created_at DESC",
						"email":  "email ASC",
					}, "newest").
					Page(0, 0).
					ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT id, email FROM users ORDER BY created_at DESC LIMIT 50 OFFSET 0",
					MySQL:    "SELECT id, email FROM users ORDER BY created_at DESC LIMIT 50 OFFSET 0",
					SQLite:   "SELECT id, email FROM users ORDER BY created_at DESC LIMIT 50 OFFSET 0",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})
		})
	}
}

func TestInsertBuilder(t *testing.T) {
	qq := New(Postgres)
	sql, args, err := qq.InsertInto("users").
		Columns("email", "status").
		Values("a@example.com", "active").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "INSERT INTO users (email, status) VALUES ($1, $2)" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"a@example.com", "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sql, args, err = qq.InsertInto("users").
		Columns("email", "status").
		Values("a@example.com", "active").
		Returning("id").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "INSERT INTO users (email, status) VALUES ($1, $2) RETURNING id" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"a@example.com", "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sql, args, err = qq.InsertInto("users").
		Columns("email", "name").
		Values("a@example.com", "A").
		Values("b@example.com", "B").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "INSERT INTO users (email, name) VALUES ($1, $2), ($3, $4)" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"a@example.com", "A", "b@example.com", "B"}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sql, args, err = qq.InsertInto("users").
		Columns("email", "name").
		Rows([]any{"c@example.com", "C"}, []any{"d@example.com", "D"}).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "INSERT INTO users (email, name) VALUES ($1, $2), ($3, $4)" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"c@example.com", "C", "d@example.com", "D"}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sql, args, err = qq.InsertInto("users").
		SetMap(map[string]any{"name": "A", "email": "a@example.com"}).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "INSERT INTO users (email, name) VALUES ($1, $2)" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"a@example.com", "A"}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestUpdateBuilder(t *testing.T) {
	qq := New(Postgres)
	sql, args, err := qq.Update("users").
		SetOptional("name", "").
		SetOptional("email", "a@example.com").
		SetIf(true, "status", "active").
		Where(Eq("id", 10)).
		Returning("id").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "UPDATE users SET email = $1, status = $2 WHERE id = $3 RETURNING id" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"a@example.com", "active", 10}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	_, _, err = qq.Update("users").Where(Eq("id", 10)).ToSQL()
	if err == nil {
		t.Fatal("expected error")
	}
	if err.Error() != "quarry: update requires at least one SET value" {
		t.Fatalf("unexpected error: %v", err)
	}

	sql, args, err = qq.Update("users").
		SetMap(map[string]any{"status": "active", "email": "a@example.com"}).
		Where(Eq("id", 10)).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "UPDATE users SET email = $1, status = $2 WHERE id = $3" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{"a@example.com", "active", 10}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestIdentifierQuotingAndValidation(t *testing.T) {
	dialects := []struct {
		name  string
		d     Dialect
		quote string
	}{
		{name: "postgres", d: Postgres, quote: `"`},
		{name: "mysql", d: MySQL, quote: "`"},
		{name: "sqlite", d: SQLite, quote: `"`},
	}

	for _, tc := range dialects {
		t.Run(tc.name, func(t *testing.T) {
			qq := New(tc.d)
			quote := func(s string) string {
				return tc.quote + s + tc.quote
			}

			users := T("users").As("u")
			sql, args, err := qq.Select(users.Col("id"), users.Col("email"), Col("status").As("user_status")).
				From(users).
				ToSQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantSQL := "SELECT " + quote("u") + "." + quote("id") + ", " + quote("u") + "." + quote("email") + ", " + quote("status") + " AS " + quote("user_status") + " FROM " + quote("users") + " AS " + quote("u")
			if sql != wantSQL {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sql)
			}
			if len(args) != 0 {
				t.Fatalf("expected no args, got %#v", args)
			}

			sql, args, err = qq.InsertInto(T("users")).
				Columns(Col("id"), Col("email"), Col("status")).
				Values(1, "a@example.com", "active").
				ToSQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantSQL = "INSERT INTO " + quote("users") + " (" + quote("id") + ", " + quote("email") + ", " + quote("status") + ") VALUES (" + placeholderForDialect(tc.d, 1) + ", " + placeholderForDialect(tc.d, 2) + ", " + placeholderForDialect(tc.d, 3) + ")"
			if sql != wantSQL {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sql)
			}
			if fmt.Sprint(args) != fmt.Sprint([]any{1, "a@example.com", "active"}) {
				t.Fatalf("args mismatch: %#v", args)
			}

			sql, args, err = qq.Update(T("users")).
				Set(Col("name"), "Bob").
				Where(Eq("id", 10)).
				ToSQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			wantSQL = "UPDATE " + quote("users") + " SET " + quote("name") + " = " + placeholderForDialect(tc.d, 1) + " WHERE id = " + placeholderForDialect(tc.d, 2)
			if sql != wantSQL {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sql)
			}
			if fmt.Sprint(args) != fmt.Sprint([]any{"Bob", 10}) {
				t.Fatalf("args mismatch: %#v", args)
			}
		})
	}

	qq := New(Postgres)
	cases := []struct {
		name string
		q    SQLer
	}{
		{name: "table name", q: qq.Select(T("bad name")).From("users")},
		{name: "table alias", q: qq.Select(T("users").As("bad alias")).From(T("users"))},
		{name: "bare column", q: qq.Select(Col("bad name")).From("users")},
		{name: "insert column", q: qq.InsertInto(T("users")).Columns(Col("good"), Col("bad name")).Values(1, 2)},
		{name: "update column", q: qq.Update(T("users")).Set(Col("bad name"), 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := tc.q.ToSQL()
			if err == nil || !errors.Is(err, ErrInvalidIdentifier) {
				t.Fatalf("expected invalid identifier error, got %v", err)
			}
		})
	}
}

func placeholderForDialect(d Dialect, n int) string {
	if d == Postgres {
		return fmt.Sprintf("$%d", n)
	}
	return "?"
}

func TestRawPlaceholderParsing(t *testing.T) {
	dialects := []struct {
		name string
		d    Dialect
	}{
		{name: "postgres", d: Postgres},
		{name: "mysql", d: MySQL},
		{name: "sqlite", d: SQLite},
	}

	tests := []struct {
		name    string
		raw     string
		args    []any
		wantSQL map[Dialect]string
		wantErr string
	}{
		{
			name: "simple raw predicate",
			raw:  "metadata->>'role' = ?",
			args: []any{"admin"},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE metadata->>'role' = $1",
				MySQL:    "SELECT * FROM users WHERE metadata->>'role' = ?",
				SQLite:   "SELECT * FROM users WHERE metadata->>'role' = ?",
			},
		},
		{
			name: "single quoted string",
			raw:  "note = 'what?' AND id = ?",
			args: []any{7},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE note = 'what?' AND id = $1",
				MySQL:    "SELECT * FROM users WHERE note = 'what?' AND id = ?",
				SQLite:   "SELECT * FROM users WHERE note = 'what?' AND id = ?",
			},
		},
		{
			name: "double quoted identifier",
			raw:  `"what?name" = ?`,
			args: []any{7},
			wantSQL: map[Dialect]string{
				Postgres: `SELECT * FROM users WHERE "what?name" = $1`,
				MySQL:    "SELECT * FROM users WHERE \"what?name\" = ?",
				SQLite:   `SELECT * FROM users WHERE "what?name" = ?`,
			},
		},
		{
			name: "line comment",
			raw:  "-- why?\nid = ?",
			args: []any{7},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE -- why?\nid = $1",
				MySQL:    "SELECT * FROM users WHERE -- why?\nid = ?",
				SQLite:   "SELECT * FROM users WHERE -- why?\nid = ?",
			},
		},
		{
			name: "block comment",
			raw:  "/* why? */\nid = ?",
			args: []any{7},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE /* why? */\nid = $1",
				MySQL:    "SELECT * FROM users WHERE /* why? */\nid = ?",
				SQLite:   "SELECT * FROM users WHERE /* why? */\nid = ?",
			},
		},
		{
			name: "dollar quoted string",
			raw:  "body = $$what?$$ AND id = ?",
			args: []any{7},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE body = $$what?$$ AND id = $1",
				MySQL:    "SELECT * FROM users WHERE body = $$what?$$ AND id = ?",
				SQLite:   "SELECT * FROM users WHERE body = $$what?$$ AND id = ?",
			},
		},
		{
			name: "json operator",
			raw:  "metadata ?| array['role', 'team'] AND id = ?",
			args: []any{7},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE metadata ?| array['role', 'team'] AND id = $1",
				MySQL:    "SELECT * FROM users WHERE metadata ?| array['role', 'team'] AND id = ?",
				SQLite:   "SELECT * FROM users WHERE metadata ?| array['role', 'team'] AND id = ?",
			},
		},
		{
			name: "two real placeholders",
			raw:  "a = ? AND b = ?",
			args: []any{1, 2},
			wantSQL: map[Dialect]string{
				Postgres: "SELECT * FROM users WHERE a = $1 AND b = $2",
				MySQL:    "SELECT * FROM users WHERE a = ? AND b = ?",
				SQLite:   "SELECT * FROM users WHERE a = ? AND b = ?",
			},
		},
		{
			name:    "too few args",
			raw:     "a = ? AND b = ?",
			args:    []any{1},
			wantErr: "raw placeholder count does not match args count",
		},
		{
			name:    "too many args",
			raw:     "a = ?",
			args:    []any{1, 2},
			wantErr: "raw placeholder count does not match args count",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, d := range dialects {
				t.Run(d.name, func(t *testing.T) {
					qq := New(d.d)
					sql, args, err := qq.Select("*").
						From("users").
						Where(Raw(tc.raw, tc.args...)).
						ToSQL()
					if tc.wantErr != "" {
						if err == nil || !errors.Is(err, ErrPlaceholderMismatch) {
							t.Fatalf("expected error %q, got %v", tc.wantErr, err)
						}
						return
					}
					if err != nil {
						t.Fatalf("unexpected error: %v", err)
					}
					wantSQL := tc.wantSQL[d.d]
					if sql != wantSQL {
						t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sql)
					}
					if fmt.Sprint(args) != fmt.Sprint(tc.args) {
						t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", tc.args, args)
					}
				})
			}
		})
	}
}

func TestDeleteBuilder(t *testing.T) {
	qq := New(Postgres)
	sql, args, err := qq.DeleteFrom("users").
		Where(Eq("id", 10)).
		Returning("id").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sql != "DELETE FROM users WHERE id = $1 RETURNING id" {
		t.Fatalf("sql mismatch: %s", sql)
	}
	if fmt.Sprint(args) != fmt.Sprint([]any{10}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestBuilderCompleteness(t *testing.T) {
	dialects := []struct {
		name    string
		d       Dialect
		wantSQL string
	}{
		{
			name:    "postgres",
			d:       Postgres,
			wantSQL: `SELECT DISTINCT "u"."id", "u"."email" FROM "users" AS "u" LEFT JOIN profiles AS p ON p.user_id = u.id GROUP BY "u"."id", "u"."email" HAVING COUNT(*) > $1 ORDER BY "u"."email" LIMIT 10 OFFSET 20`,
		},
		{
			name:    "mysql",
			d:       MySQL,
			wantSQL: "SELECT DISTINCT `u`.`id`, `u`.`email` FROM `users` AS `u` LEFT JOIN profiles AS p ON p.user_id = u.id GROUP BY `u`.`id`, `u`.`email` HAVING COUNT(*) > ? ORDER BY `u`.`email` LIMIT 10 OFFSET 20",
		},
	}

	for _, tc := range dialects {
		t.Run(tc.name, func(t *testing.T) {
			qq := New(tc.d)
			users := T("users").As("u")
			sql, args, err := qq.Select(users.Col("id"), users.Col("email")).
				Distinct().
				From(users).
				LeftJoin(Raw("profiles AS p ON p.user_id = u.id")).
				GroupBy(users.Col("id"), users.Col("email")).
				Having(Raw("COUNT(*) > ?", 1)).
				OrderBy(users.Col("email")).
				Limit(10).
				Offset(20).
				ToSQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tc.wantSQL {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", tc.wantSQL, sql)
			}
			if fmt.Sprint(args) != fmt.Sprint([]any{1}) {
				t.Fatalf("args mismatch: %#v", args)
			}
		})
	}

	for _, tc := range []struct {
		name    string
		build   func(*Quarry) SQLer
		wantSQL string
	}{
		{
			name: "join",
			build: func(qq *Quarry) SQLer {
				return qq.Select("*").From("users").Join(Raw("profiles ON profiles.user_id = users.id"))
			},
			wantSQL: "SELECT * FROM users JOIN profiles ON profiles.user_id = users.id",
		},
		{
			name: "left_join",
			build: func(qq *Quarry) SQLer {
				return qq.Select("*").From("users").LeftJoin(Raw("profiles ON profiles.user_id = users.id"))
			},
			wantSQL: "SELECT * FROM users LEFT JOIN profiles ON profiles.user_id = users.id",
		},
		{
			name: "right_join",
			build: func(qq *Quarry) SQLer {
				return qq.Select("*").From("users").RightJoin(Raw("profiles ON profiles.user_id = users.id"))
			},
			wantSQL: "SELECT * FROM users RIGHT JOIN profiles ON profiles.user_id = users.id",
		},
		{
			name: "full_join",
			build: func(qq *Quarry) SQLer {
				return qq.Select("*").From("users").FullJoin(Raw("profiles ON profiles.user_id = users.id"))
			},
			wantSQL: "SELECT * FROM users FULL JOIN profiles ON profiles.user_id = users.id",
		},
		{
			name: "cross_join",
			build: func(qq *Quarry) SQLer {
				return qq.Select("*").From("users").CrossJoin("profiles")
			},
			wantSQL: "SELECT * FROM users CROSS JOIN profiles",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			qq := New(Postgres)
			sql, args, err := tc.build(qq).ToSQL()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tc.wantSQL {
				t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", tc.wantSQL, sql)
			}
			if len(args) != 0 {
				t.Fatalf("expected no args, got %#v", args)
			}
		})
	}

	qq := New(Postgres)
	_, _, err := qq.InsertInto(nil).Columns("email").Values("a@example.com").ToSQL()
	if err == nil || !strings.Contains(err.Error(), "insert requires a table") {
		t.Fatalf("expected insert table error, got %v", err)
	}
	_, _, err = qq.Update(nil).Set("name", "bob").ToSQL()
	if err == nil || !strings.Contains(err.Error(), "update requires a table") {
		t.Fatalf("expected update table error, got %v", err)
	}
	_, _, err = qq.DeleteFrom(nil).ToSQL()
	if err == nil || !strings.Contains(err.Error(), "delete requires a table") {
		t.Fatalf("expected delete table error, got %v", err)
	}
}

func TestPredicateDepth(t *testing.T) {
	dialects := []struct {
		name string
		d    Dialect
	}{
		{name: "postgres", d: Postgres},
		{name: "mysql", d: MySQL},
		{name: "sqlite", d: SQLite},
	}

	for _, tc := range dialects {
		t.Run(tc.name, func(t *testing.T) {
			qq := New(tc.d)

			t.Run("empty_in_and_not_in", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					In("id"),
					NotIn("status"),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				wantSQL := "SELECT * FROM users WHERE 1 = 0 AND 1 = 1"
				if sql != wantSQL {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})

			t.Run("between", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(Between("age", 18, 30)).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT * FROM users WHERE age BETWEEN $1 AND $2",
					MySQL:    "SELECT * FROM users WHERE age BETWEEN ? AND ?",
					SQLite:   "SELECT * FROM users WHERE age BETWEEN ? AND ?",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{18, 30}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("tuple_in", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("accounts").Where(
					TupleIn([]any{C("account_id"), C("user_id")}, [][]any{
						{1, 10},
						{2, 20},
					}),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: `SELECT * FROM accounts WHERE ("account_id", "user_id") IN (($1, $2), ($3, $4))`,
					MySQL:    "SELECT * FROM accounts WHERE (`account_id`, `user_id`) IN ((?, ?), (?, ?))",
					SQLite:   `SELECT * FROM accounts WHERE ("account_id", "user_id") IN ((?, ?), (?, ?))`,
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{1, 10, 2, 20}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("exists", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					Eq("tenant_id", 7),
					Exists(qq.Select("1").From("sessions").Where(Eq("user_id", 42))),
					NotExists(qq.Select("1").From("bans").Where(Eq("user_id", 99))),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "SELECT * FROM users WHERE tenant_id = $1 AND EXISTS (SELECT 1 FROM sessions WHERE user_id = $2) AND NOT EXISTS (SELECT 1 FROM bans WHERE user_id = $3)",
					MySQL:    "SELECT * FROM users WHERE tenant_id = ? AND EXISTS (SELECT 1 FROM sessions WHERE user_id = ?) AND NOT EXISTS (SELECT 1 FROM bans WHERE user_id = ?)",
					SQLite:   "SELECT * FROM users WHERE tenant_id = ? AND EXISTS (SELECT 1 FROM sessions WHERE user_id = ?) AND NOT EXISTS (SELECT 1 FROM bans WHERE user_id = ?)",
				}
				if sql != want[tc.d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[tc.d], sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{7, 42, 99}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("any", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(Any("id", []int{1, 2, 3})).ToSQL()
				if tc.d != Postgres {
					if err == nil || !strings.Contains(err.Error(), "ANY is only supported for postgres") {
						t.Fatalf("expected postgres-only error, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if sql != "SELECT * FROM users WHERE id = ANY($1)" {
					t.Fatalf("sql mismatch: %s", sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{[]int{1, 2, 3}}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})
		})
	}
}

func TestInvalidDialect(t *testing.T) {
	qq := New(Dialect("oracle"))
	_, _, err := qq.Select("id").From("users").ToSQL()
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDialectPolicy(t *testing.T) {
	if Postgres.Placeholder(2) != "$2" || MySQL.Placeholder(2) != "?" || SQLite.Placeholder(2) != "?" {
		t.Fatalf("unexpected placeholder rendering")
	}
	if q, err := Postgres.QuoteIdent("users"); err != nil || q != `"users"` {
		t.Fatalf("postgres quote mismatch: %q %v", q, err)
	}
	if q, err := MySQL.QuoteIdent("users"); err != nil || q != "`users`" {
		t.Fatalf("mysql quote mismatch: %q %v", q, err)
	}
	if _, err := Postgres.QuoteIdent("bad name"); !errors.Is(err, ErrInvalidIdentifier) {
		t.Fatalf("expected invalid identifier error, got %v", err)
	}
}

func TestUnsupportedFeatureWrapping(t *testing.T) {
	_, _, err := New(MySQL).InsertInto("users").Columns("email").Values("a@example.com").Returning("id").ToSQL()
	if err == nil || !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}

	_, _, err = New(MySQL).DeleteFrom("users").Returning("id").ToSQL()
	if err == nil || !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}

	_, _, err = New(MySQL).Select("*").From("users").Where(Any("id", []int{1})).ToSQL()
	if err == nil || !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}
}

func TestNilAndEmptyBehavior(t *testing.T) {
	dialects := []Dialect{Postgres, MySQL, SQLite}

	for _, d := range dialects {
		t.Run(d.Name(), func(t *testing.T) {
			qq := New(d)

			t.Run("zero_value_root", func(t *testing.T) {
				var zero Quarry
				_, _, err := zero.Select("id").From("users").ToSQL()
				if err == nil || !errors.Is(err, ErrInvalidBuilderState) {
					t.Fatalf("expected invalid builder state, got %v", err)
				}
			})

			t.Run("nil_eq_neq", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					Eq("deleted_at", nil),
					Neq("archived_at", nil),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := "SELECT * FROM users WHERE deleted_at IS NULL AND archived_at IS NOT NULL"
				if sql != want {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})

			t.Run("empty_groups_and_nil_predicates", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					nil,
					And(),
					Or(),
					And(nil, Or(), nil),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := "SELECT * FROM users"
				if sql != want {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})

			t.Run("exists_nil_query", func(t *testing.T) {
				var subquery *SelectBuilder
				_, _, err := qq.Select("*").From("users").Where(Exists(subquery)).ToSQL()
				if err == nil || !errors.Is(err, ErrInvalidBuilderState) {
					t.Fatalf("expected invalid builder state, got %v", err)
				}
			})

			t.Run("in_nil_and_empty", func(t *testing.T) {
				sql, args, err := qq.Select("*").From("users").Where(
					In("id", []int(nil)),
					NotIn("status", []string{}),
				).ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := "SELECT * FROM users WHERE 1 = 0 AND 1 = 1"
				if sql != want {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sql)
				}
				if len(args) != 0 {
					t.Fatalf("expected no args, got %#v", args)
				}
			})

			t.Run("insert_setmap_nil_and_empty", func(t *testing.T) {
				sql, args, err := qq.InsertInto("users").
					Columns("email").
					SetMap(nil).
					SetMap(map[string]any{}).
					Values("a@example.com").
					ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := "INSERT INTO users (email) VALUES (?)"
				if d == Postgres {
					want = "INSERT INTO users (email) VALUES ($1)"
				}
				if sql != want {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sql)
				}
				if len(args) != 1 || args[0] != "a@example.com" {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("update_setmap_nil_and_empty", func(t *testing.T) {
				sql, args, err := qq.Update("users").
					Set("name", "bob").
					SetMap(nil).
					SetMap(map[string]any{}).
					Where(Eq("id", 10)).
					ToSQL()
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				want := map[Dialect]string{
					Postgres: "UPDATE users SET name = $1 WHERE id = $2",
					MySQL:    "UPDATE users SET name = ? WHERE id = ?",
					SQLite:   "UPDATE users SET name = ? WHERE id = ?",
				}
				if sql != want[d] {
					t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want[d], sql)
				}
				if fmt.Sprint(args) != fmt.Sprint([]any{"bob", 10}) {
					t.Fatalf("args mismatch: %#v", args)
				}
			})

			t.Run("duplicate_insert_columns", func(t *testing.T) {
				_, _, err := qq.InsertInto("users").
					Columns("email", "email").
					Values("a@example.com", "b@example.com").
					ToSQL()
				if err == nil || !errors.Is(err, ErrInvalidBuilderState) {
					t.Fatalf("expected duplicate column error, got %v", err)
				}
			})

			t.Run("duplicate_update_columns", func(t *testing.T) {
				_, _, err := qq.Update("users").
					Set("name", "bob").
					SetMap(map[string]any{"name": "alice"}).
					Where(Eq("id", 10)).
					ToSQL()
				if err == nil || !errors.Is(err, ErrInvalidBuilderState) {
					t.Fatalf("expected duplicate column error, got %v", err)
				}
			})
		})
	}
}
