package quarry

import (
	"fmt"
	"strings"
)

type InsertBuilder struct {
	q         *Quarry
	table     any
	columns   []string
	values    []any
	returning []any
	prefix    []rawFragment
	suffix    []rawFragment
}

func (b *InsertBuilder) Columns(cols ...string) *InsertBuilder {
	for _, col := range cols {
		if strings.TrimSpace(col) == "" {
			continue
		}
		b.columns = append(b.columns, col)
	}
	return b
}

func (b *InsertBuilder) Values(vals ...any) *InsertBuilder {
	b.values = append(b.values, vals...)
	return b
}

func (b *InsertBuilder) Returning(cols ...any) *InsertBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

func (b *InsertBuilder) Prefix(sql string, args ...any) *InsertBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *InsertBuilder) Suffix(sql string, args ...any) *InsertBuilder {
	b.suffix = append(b.suffix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *InsertBuilder) ToSQL() (string, []any, error) {
	if err := b.q.errOrNil(); err != nil {
		return "", nil, err
	}
	if len(b.columns) == 0 {
		return "", nil, fmt.Errorf("quarry: insert requires at least one column")
	}
	if len(b.values) == 0 || len(b.values) != len(b.columns) {
		return "", nil, fmt.Errorf("quarry: insert values count does not match columns count")
	}
	sb := newSQLBuilder(b.q.dialect)
	for _, frag := range b.prefix {
		if err := sb.appendRaw(frag.sql, frag.args...); err != nil {
			return "", nil, err
		}
	}
	sb.write("INSERT INTO ")
	if err := appendExpr(sb, b.table); err != nil {
		return "", nil, err
	}
	sb.write(" (")
	sb.write(strings.Join(b.columns, ", "))
	sb.write(") VALUES (")
	for i, v := range b.values {
		if i > 0 {
			sb.write(", ")
		}
		sb.write(sb.arg(v))
	}
	sb.write(")")
	if len(b.returning) > 0 {
		if b.q.dialect == MySQL {
			return "", nil, fmt.Errorf("quarry: returning is not supported for mysql")
		}
		sb.write(" RETURNING ")
		for i, col := range b.returning {
			if i > 0 {
				sb.write(", ")
			}
			if err := appendExpr(sb, col); err != nil {
				return "", nil, err
			}
		}
	}
	for _, frag := range b.suffix {
		sb.write(" ")
		if err := sb.appendRaw(frag.sql, frag.args...); err != nil {
			return "", nil, err
		}
	}
	sql, args := sb.result()
	return sql, args, nil
}
