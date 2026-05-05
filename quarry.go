package quarry

import (
	"fmt"
)

type Dialect string

const (
	Postgres Dialect = "postgres"
	MySQL    Dialect = "mysql"
	SQLite   Dialect = "sqlite"
)

// SQLer is the shared contract for any object that can build SQL and args.
type SQLer interface {
	ToSQL() (string, []any, error)
}

type Quarry struct {
	dialect Dialect
	err     error
}

func (q *Quarry) Dialect() Dialect {
	if q == nil {
		return ""
	}
	return q.dialect
}

func New(d Dialect) *Quarry {
	q := &Quarry{dialect: d}
	if !isSupportedDialect(d) {
		q.err = fmt.Errorf("quarry: unsupported dialect %q", d)
	}
	return q
}

func isSupportedDialect(d Dialect) bool {
	switch d {
	case Postgres, MySQL, SQLite:
		return true
	default:
		return false
	}
}

func (q *Quarry) errOrNil() error {
	if q == nil {
		return fmt.Errorf("quarry: nil quarry")
	}
	return q.err
}

func (q *Quarry) Select(cols ...any) *SelectBuilder {
	return &SelectBuilder{q: q, cols: append([]any(nil), cols...)}
}

func (q *Quarry) InsertInto(table any) *InsertBuilder {
	return &InsertBuilder{q: q, table: table}
}

func (q *Quarry) Update(table any) *UpdateBuilder {
	return &UpdateBuilder{q: q, table: table}
}

func (q *Quarry) DeleteFrom(table any) *DeleteBuilder {
	return &DeleteBuilder{q: q, table: table}
}
