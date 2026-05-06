package quarry_test

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	quarry "github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/codex"
)

// GoldenCase captures one SQL rendering expectation for the golden suite.
type GoldenCase struct {
	Name    string
	Dialect quarry.Dialect
	Query   quarry.Query
	SQL     string
	Args    []any
	WantErr error
}

func TestGoldenSQL(t *testing.T) {
	for _, tc := range goldenCases() {
		t.Run(tc.Name, func(t *testing.T) {
			runGoldenCase(t, tc)
		})
	}
}

func runGoldenCase(t *testing.T, tc GoldenCase) {
	t.Helper()

	sqlText, args, err := tc.Query.ToSQL()
	if tc.WantErr != nil {
		if err == nil {
			t.Fatalf("expected error %v, got nil", tc.WantErr)
		}
		if !errors.Is(err, tc.WantErr) {
			t.Fatalf("expected error %v, got %v", tc.WantErr, err)
		}
		return
	}
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if normalizeSQL(sqlText) != normalizeSQL(tc.SQL) {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", tc.SQL, sqlText)
	}
	if !reflect.DeepEqual(args, tc.Args) {
		t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", tc.Args, args)
	}
}

func goldenCases() []GoldenCase {
	cases := make([]GoldenCase, 0, 24)

	for _, d := range []quarry.Dialect{quarry.Postgres, quarry.MySQL, quarry.SQLite} {
		qq := quarry.New(d)
		dialectName := d.Name()
		cases = append(cases,
			GoldenCase{
				Name:    "basic_select/" + dialectName,
				Dialect: d,
				Query:   qq.Select("id", "email").From("users"),
				SQL:     "SELECT id, email FROM users",
			},
			GoldenCase{
				Name:    "select_with_where/" + dialectName,
				Dialect: d,
				Query:   qq.Select("id", "email").From("users").Where(quarry.Eq("status", "active")),
				SQL:     placeholderSQL(d, "SELECT id, email FROM users WHERE status = $1", "SELECT id, email FROM users WHERE status = ?"),
				Args:    []any{"active"},
			},
			GoldenCase{
				Name:    "grouped_predicates/" + dialectName,
				Dialect: d,
				Query: qq.Select("id", "email", "created_at").
					From("users").
					Where(
						quarry.Eq("tenant_id", 42),
						quarry.Or(
							quarry.ILike("email", "%bob%"),
							quarry.ILike("name", "%bob%"),
						),
					).
					OrderBy("created_at DESC"),
				SQL: placeholderSQL(d,
					"SELECT id, email, created_at FROM users WHERE tenant_id = $1 AND (email ILIKE $2 OR name ILIKE $3) ORDER BY created_at DESC",
					"SELECT id, email, created_at FROM users WHERE tenant_id = ? AND (LOWER(email) LIKE LOWER(?) OR LOWER(name) LIKE LOWER(?)) ORDER BY created_at DESC",
				),
				Args: []any{42, "%bob%", "%bob%"},
			},
			GoldenCase{
				Name:    "dynamic_filters/" + dialectName,
				Dialect: d,
				Query: qq.Select("*").From("users").Where(
					quarry.Eq("tenant_id", 42),
					quarry.Or(
						quarry.OptionalILike("email", "%bob%"),
						quarry.OptionalILike("name", "%bob%"),
					),
					quarry.OptionalEq("status", ""),
				),
				SQL: placeholderSQL(d,
					"SELECT * FROM users WHERE tenant_id = $1 AND (email ILIKE $2 OR name ILIKE $3)",
					"SELECT * FROM users WHERE tenant_id = ? AND (LOWER(email) LIKE LOWER(?) OR LOWER(name) LIKE LOWER(?))",
				),
				Args: []any{42, "%bob%", "%bob%"},
			},
			GoldenCase{
				Name:    "insert_single_row/" + dialectName,
				Dialect: d,
				Query: qq.InsertInto("users").
					Columns("email", "status").
					Values("a@example.com", "active"),
				SQL: placeholderSQL(d,
					"INSERT INTO users (email, status) VALUES ($1, $2)",
					"INSERT INTO users (email, status) VALUES (?, ?)",
				),
				Args: []any{"a@example.com", "active"},
			},
			GoldenCase{
				Name:    "insert_multiple_rows/" + dialectName,
				Dialect: d,
				Query: qq.InsertInto("users").
					Columns("email", "status").
					Values("a@example.com", "active").
					Values("b@example.com", "inactive"),
				SQL: placeholderSQL(d,
					"INSERT INTO users (email, status) VALUES ($1, $2), ($3, $4)",
					"INSERT INTO users (email, status) VALUES (?, ?), (?, ?)",
				),
				Args: []any{"a@example.com", "active", "b@example.com", "inactive"},
			},
			GoldenCase{
				Name:    "update_with_setif/" + dialectName,
				Dialect: d,
				Query: qq.Update("users").
					SetIf(true, "name", "Bob").
					SetIf(false, "ignored", "x").
					Where(quarry.Eq("id", 10)),
				SQL: placeholderSQL(d,
					"UPDATE users SET name = $1 WHERE id = $2",
					"UPDATE users SET name = ? WHERE id = ?",
				),
				Args: []any{"Bob", 10},
			},
			GoldenCase{
				Name:    "delete_with_where/" + dialectName,
				Dialect: d,
				Query:   qq.DeleteFrom("users").Where(quarry.Eq("id", 10)),
				SQL: placeholderSQL(d,
					"DELETE FROM users WHERE id = $1",
					"DELETE FROM users WHERE id = ?",
				),
				Args: []any{10},
			},
			GoldenCase{
				Name:    "raw_fragments/" + dialectName,
				Dialect: d,
				Query: qq.Select(quarry.Raw("COUNT(*) FILTER (WHERE status = ?)", "active")).
					From("users").
					Where(quarry.Raw("created_at >= ?", "2024-01-01")),
				SQL: placeholderSQL(d,
					"SELECT COUNT(*) FILTER (WHERE status = $1) FROM users WHERE created_at >= $2",
					"SELECT COUNT(*) FILTER (WHERE status = ?) FROM users WHERE created_at >= ?",
				),
				Args: []any{"active", "2024-01-01"},
			},
			GoldenCase{
				Name:    "identifier_quoting/" + dialectName,
				Dialect: d,
				Query: func() quarry.Query {
					users := quarry.T("users").As("u")
					return qq.Select(users.Col("id"), users.Col("email"), quarry.Col("status").As("user_status")).From(users)
				}(),
				SQL: quoteSQL(d,
					`SELECT "u"."id", "u"."email", "status" AS "user_status" FROM "users" AS "u"`,
					"SELECT `u`.`id`, `u`.`email`, `status` AS `user_status` FROM `users` AS `u`",
				),
			},
			GoldenCase{
				Name:    "empty_in/" + dialectName,
				Dialect: d,
				Query:   qq.Select("*").From("users").Where(quarry.In("id")),
				SQL:     "SELECT * FROM users WHERE 1 = 0",
			},
		)

		if d == quarry.MySQL {
			cases = append(cases, GoldenCase{
				Name:    "returning_unsupported/" + dialectName,
				Dialect: d,
				Query:   qq.InsertInto("users").Columns("email").Values("a@example.com").Returning("id"),
				WantErr: quarry.ErrUnsupportedFeature,
			})
			continue
		}

		cases = append(cases, GoldenCase{
			Name:    "returning_success/" + dialectName,
			Dialect: d,
			Query:   qq.InsertInto("users").Columns("email").Values("a@example.com").Returning("id"),
			SQL: placeholderSQL(d,
				"INSERT INTO users (email) VALUES ($1) RETURNING id",
				"INSERT INTO users (email) VALUES (?) RETURNING id",
			),
			Args: []any{"a@example.com"},
		})
	}

	postgres := quarry.New(quarry.Postgres)
	cases = append(cases,
		GoldenCase{
			Name:    "named_params_codex/postgres",
			Dialect: quarry.Postgres,
			Query: func() quarry.Query {
				cx := codex.New()
				if err := cx.AddRawNamed("users.by_status", `SELECT id, email FROM users WHERE tenant_id = :tenant_id AND status = :status`); err != nil {
					panic(err)
				}
				return cx.MustRaw("users.by_status").With(postgres).BindMap(map[string]any{
					"tenant_id": 42,
					"status":    "active",
				})
			}(),
			SQL:  "SELECT id, email FROM users WHERE tenant_id = $1 AND status = $2",
			Args: []any{42, "active"},
		},
		GoldenCase{
			Name:    "invalid_identifier/postgres",
			Dialect: quarry.Postgres,
			Query:   postgres.Select(quarry.T("bad name")).From("users"),
			WantErr: quarry.ErrInvalidIdentifier,
		},
		GoldenCase{
			Name:    "tuple_in/postgres",
			Dialect: quarry.Postgres,
			Query: postgres.Select("*").From("accounts").Where(
				quarry.TupleIn([]any{quarry.C("account_id"), quarry.C("user_id")}, [][]any{
					{1, 10},
					{2, 20},
				}),
			),
			SQL:  `SELECT * FROM accounts WHERE ("account_id", "user_id") IN (($1, $2), ($3, $4))`,
			Args: []any{1, 10, 2, 20},
		},
		GoldenCase{
			Name:    "postgres_any/postgres",
			Dialect: quarry.Postgres,
			Query:   postgres.Select("*").From("users").Where(quarry.Any("id", []int{1, 2, 3})),
			SQL:     "SELECT * FROM users WHERE id = ANY($1)",
			Args:    []any{[]int{1, 2, 3}},
		},
	)

	mysql := quarry.New(quarry.MySQL)
	cases = append(cases,
		GoldenCase{
			Name:    "unsupported_any/mysql",
			Dialect: quarry.MySQL,
			Query:   mysql.Select("*").From("users").Where(quarry.Any("id", []int{1, 2, 3})),
			WantErr: quarry.ErrUnsupportedFeature,
		},
		GoldenCase{
			Name:    "unsupported_returning/mysql",
			Dialect: quarry.MySQL,
			Query:   mysql.InsertInto("users").Columns("email").Values("a@example.com").Returning("id"),
			WantErr: quarry.ErrUnsupportedFeature,
		},
	)

	return cases
}

func placeholderSQL(d quarry.Dialect, postgresSQL, otherSQL string) string {
	if d == quarry.Postgres {
		return postgresSQL
	}
	return otherSQL
}

func quoteSQL(d quarry.Dialect, pgAndSQLiteSQL, mysqlSQL string) string {
	if d == quarry.MySQL {
		return mysqlSQL
	}
	return pgAndSQLiteSQL
}

func normalizeSQL(sql string) string {
	return strings.ReplaceAll(sql, "\r\n", "\n")
}
