package quarry

import (
	"errors"
	"fmt"
)

// Dialect identifies the SQL dialect Quarry should render for.
type Dialect string

const (
	// Postgres renders PostgreSQL placeholders and dialect-specific SQL.
	Postgres Dialect = "postgres"
	// MySQL renders MySQL-style placeholders and compatible SQL.
	MySQL Dialect = "mysql"
	// SQLite renders SQLite-style placeholders and compatible SQL.
	SQLite Dialect = "sqlite"
)

// Feature identifies a dialect capability Quarry may need to gate.
type Feature string

const (
	// FeatureReturning reports whether RETURNING is supported.
	FeatureReturning Feature = "returning"
	// FeatureILike reports whether ILIKE is supported natively.
	FeatureILike Feature = "ilike"
	// FeatureAny reports whether = ANY(...) is supported.
	FeatureAny Feature = "any"
)

var (
	// ErrInvalidIdentifier reports a rejected identifier value.
	ErrInvalidIdentifier = errors.New("invalid identifier")
	// ErrUnsupportedFeature reports that the active dialect cannot render a feature.
	ErrUnsupportedFeature = errors.New("unsupported dialect feature")
	// ErrInvalidBuilderState reports a builder that cannot be rendered as configured.
	ErrInvalidBuilderState = errors.New("invalid builder state")
	// ErrPlaceholderMismatch reports a placeholder / argument count mismatch.
	ErrPlaceholderMismatch = errors.New("placeholder mismatch")
)

// SQLer is the shared contract for anything that can render SQL and bound args.
type SQLer interface {
	ToSQL() (string, []any, error)
}

// Query keeps the subquery-oriented API readable without adding a second contract.
type Query = SQLer

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

// Name returns the canonical dialect name.
func (d Dialect) Name() string {
	return string(d)
}

// Placeholder renders the dialect's positional placeholder token.
func (d Dialect) Placeholder(n int) string {
	switch d {
	case Postgres:
		return fmt.Sprintf("$%d", n)
	case MySQL, SQLite:
		return "?"
	default:
		return ""
	}
}

// QuoteIdent returns the dialect-specific quoted identifier.
func (d Dialect) QuoteIdent(ident string) (string, error) {
	if err := validateIdentifier(ident); err != nil {
		return "", err
	}
	switch d {
	case MySQL:
		return "`" + ident + "`", nil
	case Postgres, SQLite:
		return `"` + ident + `"`, nil
	default:
		return "", fmt.Errorf("quarry: unsupported dialect %q: %w", d, ErrUnsupportedFeature)
	}
}

// Supports reports whether the dialect can render the requested feature.
func (d Dialect) Supports(feature Feature) bool {
	switch d {
	case Postgres:
		switch feature {
		case FeatureReturning, FeatureILike, FeatureAny:
			return true
		}
	case SQLite:
		switch feature {
		case FeatureReturning:
			return true
		}
	case MySQL:
		return false
	}
	return false
}

// New creates a Quarry configured for the supplied dialect.
func New(d Dialect) *Quarry {
	q := &Quarry{dialect: d}
	if !isSupportedDialect(d) {
		// Defer the unsupported-dialect failure until rendering so builder code can stay fluent.
		q.err = fmt.Errorf("quarry: unsupported dialect %q: %w", d, ErrUnsupportedFeature)
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
	if q.dialect == "" && q.err == nil {
		// Treat the zero-value root as invalid so callers must opt into an explicit dialect.
		return fmt.Errorf("quarry: zero-value quarry requires quarry.New: %w", ErrInvalidBuilderState)
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
