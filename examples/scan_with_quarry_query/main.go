// Package main prints a Quarry query scanned into a scalar result.
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

const driverName = "quarry-example-scan-with-quarry-query"

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
	count, err := scan.One[int64](ctx, db,
		qq.Select(quarry.Raw("COUNT(*)")).
			From("users").
			Where(quarry.Eq("status", "active")),
	)
	if err != nil {
		panic(err)
	}

	fmt.Println(count)
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
	if !strings.Contains(strings.ToLower(query), "count(*)") {
		return nil, fmt.Errorf("unexpected query shape: %s", query)
	}
	return &exampleRows{
		columns: []string{"count"},
		data:    [][]driver.Value{{int64(3)}},
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
