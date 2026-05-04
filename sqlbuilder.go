package quarry

import (
	"fmt"
	"strconv"
	"strings"
)

type sqlBuilder struct {
	dialect Dialect
	buf     strings.Builder
	args    []any
}

func newSQLBuilder(d Dialect) *sqlBuilder {
	return &sqlBuilder{dialect: d}
}

func (b *sqlBuilder) write(s string) {
	b.buf.WriteString(s)
}

func (b *sqlBuilder) writeByte(c byte) {
	b.buf.WriteByte(c)
}

func (b *sqlBuilder) String() string {
	return b.buf.String()
}

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
		b.write(b.arg(args[argIndex]))
		argIndex++
	}
	if argIndex != len(args) {
		return fmt.Errorf("quarry: raw placeholder count does not match args count")
	}
	return nil
}

func (b *sqlBuilder) result() (string, []any) {
	return b.String(), append([]any(nil), b.args...)
}
