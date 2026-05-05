package quarry

import (
	"fmt"
	"strings"
)

type rawFragment struct {
	sql  string
	args []any
}

type SelectBuilder struct {
	q       *Quarry
	cols    []any
	from    any
	preds   []Predicate
	orderBy []string
	limit   *int
	offset  *int
	prefix  []rawFragment
	suffix  []rawFragment
}

func (b *SelectBuilder) From(table any) *SelectBuilder {
	b.from = table
	return b
}

func (b *SelectBuilder) Where(preds ...Predicate) *SelectBuilder {
	b.preds = append(b.preds, preds...)
	return b
}

func (b *SelectBuilder) OrderBy(parts ...string) *SelectBuilder {
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		b.orderBy = append(b.orderBy, part)
	}
	return b
}

func (b *SelectBuilder) Limit(n int) *SelectBuilder {
	b.limit = &n
	return b
}

func (b *SelectBuilder) Offset(n int) *SelectBuilder {
	b.offset = &n
	return b
}

func (b *SelectBuilder) Prefix(sql string, args ...any) *SelectBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *SelectBuilder) Suffix(sql string, args ...any) *SelectBuilder {
	b.suffix = append(b.suffix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *SelectBuilder) ToSQL() (string, []any, error) {
	if err := b.q.errOrNil(); err != nil {
		return "", nil, err
	}
	sb := newSQLBuilder(b.q.dialect)
	for _, frag := range b.prefix {
		if err := sb.appendRaw(frag.sql, frag.args...); err != nil {
			return "", nil, err
		}
	}
	sb.write("SELECT ")
	if len(b.cols) == 0 {
		sb.write("*")
	} else {
		for i, col := range b.cols {
			if i > 0 {
				sb.write(", ")
			}
			if err := appendExpr(sb, col); err != nil {
				return "", nil, err
			}
		}
	}
	if b.from != nil {
		sb.write(" FROM ")
		if err := appendExpr(sb, b.from); err != nil {
			return "", nil, err
		}
	}
	if pred := And(b.preds...); pred != nil && !pred.empty() {
		sb.write(" WHERE ")
		if err := renderPredicate(sb, pred, false); err != nil {
			return "", nil, err
		}
	}
	if len(b.orderBy) > 0 {
		sb.write(" ORDER BY ")
		sb.write(strings.Join(b.orderBy, ", "))
	}
	if b.limit != nil {
		sb.write(fmt.Sprintf(" LIMIT %d", *b.limit))
	}
	if b.offset != nil {
		sb.write(fmt.Sprintf(" OFFSET %d", *b.offset))
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
