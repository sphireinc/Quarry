// Package scan executes Quarry queries and scans rows into Go values.
//
// The package is optional and intentionally smaller than an ORM. It covers the
// common "run this SQL and scan the result" case without adding entity
// tracking, relationship loading, or generated query code.
package scan

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"

	"github.com/sphireinc/quarry"
)

// Queryer is the minimal database handle required for Query and row scanning.
type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// QueryerRow is the row-oriented variant of Queryer.
type QueryerRow interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

// Execer is the minimal database handle required for Exec.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// BuiltQuery captures the rendered SQL text and the args that go with it.
type BuiltQuery struct {
	// SQL is the rendered statement text.
	SQL string
	// Args are the bound values in driver order.
	Args []any
}

// Build renders a Quarry query and preserves its args for execution.
func Build(q quarry.SQLer) (BuiltQuery, error) {
	if isNilValue(q) {
		return BuiltQuery{}, fmt.Errorf("quarry scan: nil query")
	}
	sqlText, args, err := q.ToSQL()
	if err != nil {
		return BuiltQuery{}, fmt.Errorf("quarry scan: build sql: %w", err)
	}
	return BuiltQuery{SQL: sqlText, Args: append([]any(nil), args...)}, nil
}

// Exec renders q and executes it through db.
func Exec(ctx context.Context, db Execer, q quarry.SQLer) (sql.Result, error) {
	if err := validateScanInputs(ctx, db, q); err != nil {
		return nil, err
	}
	built, err := Build(q)
	if err != nil {
		return nil, err
	}
	res, err := db.ExecContext(ctx, built.SQL, built.Args...)
	if err != nil {
		return nil, fmt.Errorf("quarry scan: exec: %w", err)
	}
	return res, nil
}

// Query renders q and issues a QueryContext call through db.
func Query(ctx context.Context, db Queryer, q quarry.SQLer) (*sql.Rows, error) {
	if err := validateScanInputs(ctx, db, q); err != nil {
		return nil, err
	}
	built, err := Build(q)
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, built.SQL, built.Args...)
	if err != nil {
		return nil, fmt.Errorf("quarry scan: query: %w", err)
	}
	return rows, nil
}

// validateScanInputs keeps nil contexts, databases, and queries from panicking.
func validateScanInputs(ctx context.Context, db any, q quarry.SQLer) error {
	if ctx == nil {
		return fmt.Errorf("quarry scan: nil context")
	}
	if isNilValue(db) {
		return fmt.Errorf("quarry scan: nil database")
	}
	if isNilValue(q) {
		return fmt.Errorf("quarry scan: nil query")
	}
	return nil
}

// isNilValue catches typed nils hiding behind interfaces.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}
