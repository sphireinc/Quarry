package quarry

import (
	"fmt"
)

// Dialect identifies the SQL dialect Quarry should render for.
type Dialect string

const (
	// Postgres renders PostgreSQL placeholders and dialect-specific SQL.
	Postgres Dialect = "postgres"
	// MySQL renders MySQL-style placeholders and compatible SQL.
	MySQL    Dialect = "mysql"
	// SQLite renders SQLite-style placeholders and compatible SQL.
	SQLite   Dialect = "sqlite"
)

// SQLer is the shared contract for anything that can render SQL and bound args.
type SQLer interface {
	ToSQL() (string, []any, error)
}

// Quarry carries the selected dialect and manufactures builders from it.
type Quarry struct {
	dialect Dialect
	err     error
}

// Dialect returns the configured dialect for the receiver.
func (q *Quarry) Dialect() Dialect {
	if q == nil {
		return ""
	}
	return q.dialect
}

// New creates a Quarry configured for the supplied dialect.
func New(d Dialect) *Quarry {
	q := &Quarry{dialect: d}
	if !isSupportedDialect(d) {
		// Defer the unsupported-dialect failure until rendering so builder code can stay fluent.
		q.err = fmt.Errorf("quarry: unsupported dialect %q", d)
	}
	return q
}

// isSupportedDialect reports whether Quarry knows how to render the dialect.
func isSupportedDialect(d Dialect) bool {
	switch d {
	case Postgres, MySQL, SQLite:
		return true
	default:
		return false
	}
}

// errOrNil returns the constructor-time error, if any.
func (q *Quarry) errOrNil() error {
	if q == nil {
		return fmt.Errorf("quarry: nil quarry")
	}
	return q.err
}

// Select starts a SELECT builder that inherits the receiver's dialect.
func (q *Quarry) Select(cols ...any) *SelectBuilder {
	return &SelectBuilder{q: q, cols: append([]any(nil), cols...)}
}

// InsertInto starts an INSERT builder that inherits the receiver's dialect.
func (q *Quarry) InsertInto(table any) *InsertBuilder {
	return &InsertBuilder{q: q, table: table}
}

// Update starts an UPDATE builder that inherits the receiver's dialect.
func (q *Quarry) Update(table any) *UpdateBuilder {
	return &UpdateBuilder{q: q, table: table}
}

// DeleteFrom starts a DELETE builder that inherits the receiver's dialect.
func (q *Quarry) DeleteFrom(table any) *DeleteBuilder {
	return &DeleteBuilder{q: q, table: table}
}
