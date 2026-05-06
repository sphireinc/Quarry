package scan_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/scan"
)

type userRow struct {
	ID     int    `db:"id"`
	Email  string `db:"email"`
	Status string `db:"status"`
}

type fallbackUser struct {
	ID     int
	Email  string `json:"email"`
	Status string
}

type nullableUser struct {
	ID       int            `db:"id"`
	Nickname *string        `db:"nickname"`
	Status   sql.NullString `db:"status"`
}

type duplicateUser struct {
	First  int `db:"id"`
	Second int `db:"id"`
}

type unsupportedUser struct {
	ID chan int `db:"id"`
}

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()

	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_"))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)

	schema := `
CREATE TABLE users (
	id INTEGER PRIMARY KEY AUTOINCREMENT,
	email TEXT NOT NULL,
	status TEXT NOT NULL,
	nickname TEXT NULL
);`
	if _, err := db.Exec(schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seedUsers(t *testing.T, db *sql.DB) {
	t.Helper()
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	_, err := scan.Exec(ctx, db, qq.InsertInto("users").
		Columns("email", "status").
		Values("a@example.com", "active"))
	if err != nil {
		t.Fatalf("seed row 1: %v", err)
	}
	_, err = scan.Exec(ctx, db, qq.InsertInto("users").
		Columns("email", "status").
		Values("b@example.com", "inactive"))
	if err != nil {
		t.Fatalf("seed row 2: %v", err)
	}
}

func TestExecQueryAndScan(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	res, err := scan.Exec(ctx, db, qq.InsertInto("users").
		Columns("email", "status").
		Values("c@example.com", "active"))
	if err != nil {
		t.Fatalf("exec insert: %v", err)
	}
	if res == nil {
		t.Fatal("expected result")
	}

	rows, err := scan.Query(ctx, db, qq.Select("id", "email", "status").From("users").OrderBy("id ASC"))
	if err != nil {
		t.Fatalf("query select: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("expected at least one row")
	}
	var got userRow
	if err := rows.Scan(&got.ID, &got.Email, &got.Status); err != nil {
		t.Fatalf("scan row: %v", err)
	}
	if got.Email != "a@example.com" {
		t.Fatalf("unexpected row: %#v", got)
	}
}

func TestAllOneMaybeOne(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	all, err := scan.All[userRow](ctx, db, qq.Select("email", "id", "status").From("users").OrderBy("id ASC"))
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(all) != 2 {
		t.Fatalf("unexpected count: %d", len(all))
	}
	if all[0].ID != 1 || all[0].Email != "a@example.com" {
		t.Fatalf("unexpected first row: %#v", all[0])
	}

	one, err := scan.One[userRow](ctx, db, qq.Select("id", "email", "status").From("users").Where(quarry.Eq("id", 1)))
	if err != nil {
		t.Fatalf("one: %v", err)
	}
	if one.Email != "a@example.com" {
		t.Fatalf("unexpected one: %#v", one)
	}

	maybe, err := scan.MaybeOne[userRow](ctx, db, qq.Select("id", "email", "status").From("users").Where(quarry.Eq("id", 999)))
	if err != nil {
		t.Fatalf("maybe one: %v", err)
	}
	if maybe != nil {
		t.Fatalf("expected nil, got %#v", maybe)
	}

	_, err = scan.One[userRow](ctx, db, qq.Select("id", "email", "status").From("users").Where(quarry.Eq("id", 999)))
	if err == nil || !strings.Contains(err.Error(), "quarry scan: no rows") {
		t.Fatalf("expected no rows error, got %v", err)
	}
}

func TestFieldMappingUsesDBTags(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	all, err := scan.All[userRow](ctx, db, qq.Select("status", "email", "id").From("users").Where(quarry.Eq("id", 1)))
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(all) != 1 {
		t.Fatalf("unexpected count: %d", len(all))
	}
	if all[0].ID != 1 || all[0].Email != "a@example.com" || all[0].Status != "active" {
		t.Fatalf("unexpected row mapping: %#v", all[0])
	}
}

func TestFieldMappingFallbacksAndUnknownColumns(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	users, err := scan.ScanAll[fallbackUser](ctx, db, qq.Select("id", "email", "status", quarry.Raw("123 AS ignored")).From("users").Where(quarry.Eq("id", 1)))
	if err != nil {
		t.Fatalf("scan all: %v", err)
	}
	if len(users) != 1 {
		t.Fatalf("unexpected count: %d", len(users))
	}
	if users[0].ID != 1 || users[0].Email != "a@example.com" || users[0].Status != "active" {
		t.Fatalf("unexpected mapping: %#v", users[0])
	}
}

func TestNullableAndPointerScanning(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	nick := "bee"
	if _, err := db.ExecContext(ctx, `UPDATE users SET nickname = ? WHERE id = 2`, nick); err != nil {
		t.Fatalf("seed nickname: %v", err)
	}

	first, err := scan.ScanOne[nullableUser](ctx, db, qq.Select("id", "nickname", "status").From("users").Where(quarry.Eq("id", 1)))
	if err != nil {
		t.Fatalf("scan one: %v", err)
	}
	if first.Nickname != nil {
		t.Fatalf("expected nil nickname, got %#v", *first.Nickname)
	}
	if !first.Status.Valid || first.Status.String != "active" {
		t.Fatalf("unexpected sql null: %#v", first.Status)
	}

	second, err := scan.ScanOne[nullableUser](ctx, db, qq.Select("id", "nickname", "status").From("users").Where(quarry.Eq("id", 2)))
	if err != nil {
		t.Fatalf("scan one: %v", err)
	}
	if second.Nickname == nil || *second.Nickname != "bee" {
		t.Fatalf("unexpected pointer nickname: %#v", second.Nickname)
	}
}

func TestDuplicateAndUnsupportedFieldErrors(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	_, err := scan.All[userRow](ctx, db, qq.Select("id", "id").From("users").Where(quarry.Eq("id", 1)))
	if err == nil || !strings.Contains(err.Error(), "duplicate column mapping") {
		t.Fatalf("expected duplicate mapping error, got %v", err)
	}

	_, err = scan.All[unsupportedUser](ctx, db, qq.Select("id").From("users").Where(quarry.Eq("id", 1)))
	if err == nil || !strings.Contains(err.Error(), "unsupported field type") {
		t.Fatalf("expected unsupported field type error, got %v", err)
	}
}

func TestNilInputs(t *testing.T) {
	db := openTestDB(t)
	seedUsers(t, db)
	qq := quarry.New(quarry.SQLite)
	ctx := context.Background()

	if _, err := scan.Build(nil); err == nil || !strings.Contains(err.Error(), "nil query") {
		t.Fatalf("expected nil query error, got %v", err)
	}

	var nilCtx context.Context
	if _, err := scan.Exec(nilCtx, db, qq.Select("id").From("users")); err == nil || !strings.Contains(err.Error(), "nil context") {
		t.Fatalf("expected nil context error, got %v", err)
	}

	var nilDB *sql.DB
	if _, err := scan.Query(ctx, nilDB, qq.Select("id").From("users")); err == nil || !strings.Contains(err.Error(), "nil database") {
		t.Fatalf("expected nil database error, got %v", err)
	}
}

type execSpy struct {
	called bool
}

func (e *execSpy) ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error) {
	e.called = true
	return nil, errors.New("should not be called")
}

func TestSQLBuildErrorReturnedBeforeExec(t *testing.T) {
	ctx := context.Background()
	spy := &execSpy{}

	_, err := scan.Exec(ctx, spy, quarry.New(quarry.SQLite).Update("users"))
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "quarry scan: build sql: quarry: update requires at least one SET value") {
		t.Fatalf("unexpected error: %v", err)
	}
	if spy.called {
		t.Fatal("expected db exec not to be called")
	}
}

func TestTransactionCompatibility(t *testing.T) {
	db := openTestDB(t)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()

	_, err = scan.Exec(ctx, tx, qq.InsertInto("users").
		Columns("email", "status").
		Values("tx@example.com", "active"))
	if err != nil {
		t.Fatalf("insert in tx: %v", err)
	}

	users, err := scan.All[userRow](ctx, tx, qq.Select("id", "email", "status").From("users").Where(quarry.Eq("email", "tx@example.com")))
	if err != nil {
		t.Fatalf("query in tx: %v", err)
	}
	if len(users) != 1 || users[0].Email != "tx@example.com" {
		t.Fatalf("unexpected tx rows: %#v", users)
	}

	if err := tx.Commit(); err != nil {
		t.Fatalf("commit tx: %v", err)
	}
}

func TestBuildHelper(t *testing.T) {
	qq := quarry.New(quarry.SQLite)
	built, err := scan.Build(qq.Select("id").From("users").Where(quarry.Eq("status", "active")))
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if built.SQL != "SELECT id FROM users WHERE status = ?" {
		t.Fatalf("sql mismatch: %s", built.SQL)
	}
	if fmt.Sprint(built.Args) != fmt.Sprint([]any{"active"}) {
		t.Fatalf("args mismatch: %#v", built.Args)
	}
}
