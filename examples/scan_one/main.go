// Package main prints a single Quarry row scanned into a Go struct.
package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"strings"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/scan"
)

type User struct {
	ID     int    `db:"id"`
	Email  string `db:"email"`
	Status string `db:"status"`
}

const driverName = "quarry-example-scan-one"

func init() {
	sql.Register(driverName, exampleDriver{})
}

func main() {
	ctx := context.Background()
	db, err := sql.Open(driverName, "")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	qq := quarry.New(quarry.SQLite)
	user, err := scan.One[User](ctx, db,
		qq.Select("id", "email", "status").
			From("users").
			Where(quarry.Eq("id", 1)),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(user)
}

type exampleDriver struct{}

func (exampleDriver) Open(string) (driver.Conn, error) {
	return exampleConn{}, nil
}

type exampleConn struct{}

func (exampleConn) Prepare(string) (driver.Stmt, error) {
	return nil, driver.ErrSkip
}

func (exampleConn) Close() error {
	return nil
}

func (exampleConn) Begin() (driver.Tx, error) {
	return nil, driver.ErrSkip
}

func (exampleConn) QueryContext(_ context.Context, query string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(query)), "select") {
		return nil, fmt.Errorf("unexpected query: %s", query)
	}
	return &exampleRows{
		columns: []string{"id", "email", "status"},
		data: [][]driver.Value{
			{int64(1), "a@example.com", "active"},
		},
	}, nil
}

func (exampleConn) CheckNamedValue(*driver.NamedValue) error {
	return nil
}

var _ driver.Driver = exampleDriver{}
var _ driver.QueryerContext = exampleConn{}
var _ driver.NamedValueChecker = exampleConn{}

type exampleRows struct {
	columns []string
	data    [][]driver.Value
	index   int
}

func (r *exampleRows) Columns() []string {
	return r.columns
}

func (r *exampleRows) Close() error {
	return nil
}

func (r *exampleRows) Next(dest []driver.Value) error {
	if r.index >= len(r.data) {
		return io.EOF
	}
	copy(dest, r.data[r.index])
	r.index++
	return nil
}
