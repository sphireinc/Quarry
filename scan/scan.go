package scan

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/sphireinc/quarry"
)

type Queryer interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

type QueryerRow interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

type BuiltQuery struct {
	SQL  string
	Args []any
}

func Build(q quarry.SQLer) (BuiltQuery, error) {
	sqlText, args, err := q.ToSQL()
	if err != nil {
		return BuiltQuery{}, fmt.Errorf("quarry scan: build sql: %w", err)
	}
	return BuiltQuery{SQL: sqlText, Args: append([]any(nil), args...)}, nil
}

func Exec(ctx context.Context, db Execer, q quarry.SQLer) (sql.Result, error) {
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

func Query(ctx context.Context, db Queryer, q quarry.SQLer) (*sql.Rows, error) {
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
