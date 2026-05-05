package quarry

import (
	"fmt"
	"sort"
	"strings"
)

// InsertBuilder renders INSERT statements for the active Quarry dialect.
type InsertBuilder struct {
	q         *Quarry
	table     any
	columns   []any
	rows      [][]any
	returning []any
	prefix    []rawFragment
	suffix    []rawFragment
}

// Columns appends INSERT column expressions.
func (b *InsertBuilder) Columns(cols ...any) *InsertBuilder {
	for _, col := range cols {
		if s, ok := col.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		b.columns = append(b.columns, col)
	}
	return b
}

// Values appends a single INSERT row.
func (b *InsertBuilder) Values(vals ...any) *InsertBuilder {
	b.rows = append(b.rows, copyAnySlice(vals))
	return b
}

// Rows appends one or more explicit INSERT rows.
func (b *InsertBuilder) Rows(rows ...[]any) *InsertBuilder {
	for _, row := range rows {
		b.rows = append(b.rows, copyAnySlice(row))
	}
	return b
}

// SetMap converts a map into a deterministic INSERT column/value row.
func (b *InsertBuilder) SetMap(values map[string]any) *InsertBuilder {
	if len(values) == 0 {
		return b
	}
	if len(b.columns) == 0 {
		keys := sortedKeys(values)
		for _, key := range keys {
			b.columns = append(b.columns, key)
		}
		row := make([]any, 0, len(keys))
		for _, key := range keys {
			row = append(row, values[key])
		}
		b.rows = append(b.rows, row)
		return b
	}
	row := make([]any, len(b.columns))
	for i, col := range b.columns {
		key, ok := columnMapKey(col)
		if !ok {
			return b
		}
		value, ok := values[key]
		if !ok {
			return b
		}
		row[i] = value
	}
	b.rows = append(b.rows, row)
	return b
}

// Returning appends RETURNING expressions.
func (b *InsertBuilder) Returning(cols ...any) *InsertBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

// Prefix appends a raw fragment before the INSERT statement.
func (b *InsertBuilder) Prefix(sql string, args ...any) *InsertBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

// Suffix appends a raw fragment after the INSERT statement.
func (b *InsertBuilder) Suffix(sql string, args ...any) *InsertBuilder {
	b.suffix = append(b.suffix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *InsertBuilder) ToSQL() (string, []any, error) {
	if err := b.q.errOrNil(); err != nil {
		return "", nil, err
	}
	if err := requireTableValue(b.table, "insert"); err != nil {
		return "", nil, err
	}
	if len(b.columns) == 0 {
		return "", nil, fmt.Errorf("quarry: insert requires at least one column")
	}
	if len(b.rows) == 0 {
		return "", nil, fmt.Errorf("quarry: insert requires at least one row")
	}
	if err := ensureUniqueColumnKeys("insert", b.columns); err != nil {
		return "", nil, err
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
	for i, col := range b.columns {
		if i > 0 {
			sb.write(", ")
		}
		if err := appendExpr(sb, col); err != nil {
			return "", nil, err
		}
	}
	sb.write(") VALUES ")
	for rowIdx, row := range b.rows {
		if len(row) != len(b.columns) {
			return "", nil, fmt.Errorf("quarry: insert values count does not match columns count")
		}
		if rowIdx > 0 {
			sb.write(", ")
		}
		sb.write("(")
		for i, v := range row {
			if i > 0 {
				sb.write(", ")
			}
			sb.write(sb.arg(v))
		}
		sb.write(")")
	}
	if len(b.returning) > 0 {
		if !b.q.dialect.Supports(FeatureReturning) {
			return "", nil, fmt.Errorf("render returning clause: %w: %s", ErrUnsupportedFeature, b.q.dialect.Name())
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

func copyAnySlice(in []any) []any {
	if len(in) == 0 {
		return nil
	}
	out := make([]any, len(in))
	copy(out, in)
	return out
}

func sortedKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
