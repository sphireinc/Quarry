package quarry

import (
	"fmt"
	"strconv"
	"strings"
)

// sqlBuilder is the internal accumulator used by every builder and predicate renderer.
type sqlBuilder struct {
	dialect Dialect
	buf     strings.Builder
	args    []any
}

// newSQLBuilder creates a builder with the active dialect baked in.
func newSQLBuilder(d Dialect) *sqlBuilder {
	return &sqlBuilder{dialect: d}
}

// write appends a string fragment verbatim.
func (b *sqlBuilder) write(s string) {
	b.buf.WriteString(s)
}

// writeByte appends a single byte to the SQL buffer.
func (b *sqlBuilder) writeByte(c byte) {
	b.buf.WriteByte(c)
}

// String returns the SQL accumulated so far.
func (b *sqlBuilder) String() string {
	return b.buf.String()
}

// arg binds a value and returns the active dialect's placeholder token.
func (b *sqlBuilder) arg(v any) string {
	b.args = append(b.args, v)
	switch b.dialect {
	case Postgres:
		return "$" + strconv.Itoa(len(b.args))
	case MySQL, SQLite:
		return "?"
	default:
		return ""
	}
}

// appendRaw rewrites `?` placeholders into dialect-specific tokens while preserving arg order.
func (b *sqlBuilder) appendRaw(sql string, args ...any) error {
	if !isSupportedDialect(b.dialect) {
		return fmt.Errorf("quarry: unsupported dialect %q", b.dialect)
	}
	argIndex := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '?' {
			b.writeByte(sql[i])
			continue
		}
		if argIndex >= len(args) {
			return fmt.Errorf("quarry: raw placeholder count does not match args count")
		}
		// The raw escape hatch stays simple: value placeholders are always `?`.
		b.write(b.arg(args[argIndex]))
		argIndex++
	}
	if argIndex != len(args) {
		return fmt.Errorf("quarry: raw placeholder count does not match args count")
	}
	return nil
}

// result returns a stable copy of the SQL string and args slice.
func (b *sqlBuilder) result() (string, []any) {
	return b.String(), append([]any(nil), b.args...)
}
