package quarry

import (
	"fmt"
	"strings"

	"github.com/sphireinc/quarry/internal/rawsql"
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
	return b.dialect.Placeholder(len(b.args))
}

// appendRaw rewrites `?` placeholders into dialect-specific tokens while preserving arg order.
func (b *sqlBuilder) appendRaw(sql string, args ...any) error {
	if !isSupportedDialect(b.dialect) {
		return fmt.Errorf("quarry: %q: %w", b.dialect, ErrUnsupportedFeature)
	}
	rewritten, out, err := rawsql.RewriteQuestionPlaceholders(sql, args, func(n int) string {
		// The raw escape hatch stays simple: value placeholders are always `?`.
		return b.dialect.Placeholder(len(b.args) + n)
	})
	if err != nil {
		if strings.Contains(err.Error(), "raw placeholder count does not match args count") {
			return fmt.Errorf("append raw sql: %w", ErrPlaceholderMismatch)
		}
		return err
	}
	b.write(rewritten)
	b.args = append(b.args, out...)
	return nil
}

// result returns a stable copy of the SQL string and args slice.
func (b *sqlBuilder) result() (string, []any) {
	return b.String(), append([]any(nil), b.args...)
}
