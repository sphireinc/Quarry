package hydra

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/sphireinc/quarry"
)

type hydraUser struct {
	ID     int    `db:"id"`
	Email  string `db:"email"`
	Status string `db:"status"`
}

const hydraDriverName = "quarry-hydra-test"

func init() {
	sql.Register(hydraDriverName, hydraDriver{})
}

func openHydraDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open(hydraDriverName, "")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	db.SetMaxOpenConns(1)
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

type hydraDriver struct{}

func (hydraDriver) Open(string) (driver.Conn, error) {
	return hydraConn{}, nil
}

type hydraConn struct{}

func (hydraConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (hydraConn) Close() error {
	return nil
}

func (hydraConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (hydraConn) QueryContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Rows, error) {
	if len(args) > 0 {
		switch v := args[0].Value.(type) {
		case int:
			if v == 999 {
				return &hydraRows{columns: []string{"id", "email", "status"}}, nil
			}
		case int64:
			if v == 999 {
				return &hydraRows{columns: []string{"id", "email", "status"}}, nil
			}
		case fmt.Stringer:
			if v.String() == "999" {
				return &hydraRows{columns: []string{"id", "email", "status"}}, nil
			}
		}
	}
	return &hydraRows{
		columns: []string{"id", "email", "status"},
		data: [][]driver.Value{
			{int64(1), "a@example.com", "active"},
		},
	}, nil
}

func (hydraConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

var _ driver.Driver = hydraDriver{}
var _ driver.QueryerContext = hydraConn{}
var _ driver.NamedValueChecker = hydraConn{}

type hydraRows struct {
	columns []string
	data    [][]driver.Value
	index   int
}

func (r *hydraRows) Columns() []string {
	return r.columns
}

func (r *hydraRows) Close() error {
	return nil
}

func (r *hydraRows) Next(dest []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.index])
	r.index++
	return nil
}
