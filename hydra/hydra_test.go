package hydra

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/sphireinc/quarry"
)

type hydraUser struct {
	ID     int    `db:"id"`
	Email  string `db:"email"`
	Status string `db:"status"`
}

func openHydraDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", fmt.Sprintf("file:%s?mode=memory&cache=shared", strings.ReplaceAll(t.Name(), "/", "_")))
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL, status TEXT NOT NULL)`); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO users (email, status) VALUES ('a@example.com', 'active')`); err != nil {
		t.Fatalf("seed row: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestHydraWrappers(t *testing.T) {
	db := openHydraDB(t)
	ctx := context.Background()
	qq := quarry.New(quarry.SQLite)

	rows, err := All[hydraUser](ctx, db, qq.Select("id", "email", "status").From("users").OrderBy("id ASC"))
	if err != nil {
		t.Fatalf("all: %v", err)
	}
	if len(rows) != 1 || rows[0].Email != "a@example.com" {
		t.Fatalf("unexpected rows: %#v", rows)
	}

	one, err := One[hydraUser](ctx, db, qq.Select("id", "email", "status").From("users").Where(quarry.Eq("id", 1)))
	if err != nil {
		t.Fatalf("one: %v", err)
	}
	if one.Email != "a@example.com" {
		t.Fatalf("unexpected row: %#v", one)
	}

	maybe, err := MaybeOne[hydraUser](ctx, db, qq.Select("id", "email", "status").From("users").Where(quarry.Eq("id", 999)))
	if err != nil {
		t.Fatalf("maybe one: %v", err)
	}
	if maybe != nil {
		t.Fatalf("expected nil, got %#v", maybe)
	}
}
