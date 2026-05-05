package quarry

import (
	"fmt"
	"strings"
)

type rawFragment struct {
	sql  string
	args []any
}

type joinClause struct {
	kind   string
	target any
}

// SelectBuilder renders SELECT statements for the active Quarry dialect.
type SelectBuilder struct {
	q        *Quarry
	distinct bool
	cols     []any
	from     any
	joins    []joinClause
	preds    []Predicate
	groupBy  []any
	having   []Predicate
	orderBy  []any
	limit    *uint64
	offset   *uint64
	prefix   []rawFragment
	suffix   []rawFragment
}

// Distinct toggles SELECT DISTINCT rendering.
func (b *SelectBuilder) Distinct() *SelectBuilder {
	b.distinct = true
	return b
}

// From sets the FROM clause.
func (b *SelectBuilder) From(table any) *SelectBuilder {
	b.from = table
	return b
}

// Join appends a plain JOIN clause.
func (b *SelectBuilder) Join(expr any) *SelectBuilder {
	b.joins = append(b.joins, joinClause{kind: "JOIN", target: expr})
	return b
}

// LeftJoin appends a LEFT JOIN clause.
func (b *SelectBuilder) LeftJoin(expr any) *SelectBuilder {
	b.joins = append(b.joins, joinClause{kind: "LEFT JOIN", target: expr})
	return b
}

// RightJoin appends a RIGHT JOIN clause.
func (b *SelectBuilder) RightJoin(expr any) *SelectBuilder {
	b.joins = append(b.joins, joinClause{kind: "RIGHT JOIN", target: expr})
	return b
}

// FullJoin appends a FULL JOIN clause.
func (b *SelectBuilder) FullJoin(expr any) *SelectBuilder {
	b.joins = append(b.joins, joinClause{kind: "FULL JOIN", target: expr})
	return b
}

// CrossJoin appends a CROSS JOIN clause.
func (b *SelectBuilder) CrossJoin(expr any) *SelectBuilder {
	b.joins = append(b.joins, joinClause{kind: "CROSS JOIN", target: expr})
	return b
}

// Where appends WHERE predicates.
func (b *SelectBuilder) Where(preds ...Predicate) *SelectBuilder {
	b.preds = append(b.preds, preds...)
	return b
}

// GroupBy appends GROUP BY expressions.
func (b *SelectBuilder) GroupBy(parts ...any) *SelectBuilder {
	b.groupBy = append(b.groupBy, parts...)
	return b
}

// Having appends HAVING predicates.
func (b *SelectBuilder) Having(preds ...Predicate) *SelectBuilder {
	b.having = append(b.having, preds...)
	return b
}

// OrderBy appends ORDER BY expressions or trusted fragments.
func (b *SelectBuilder) OrderBy(parts ...any) *SelectBuilder {
	for _, part := range parts {
		if s, ok := part.(string); ok && strings.TrimSpace(s) == "" {
			continue
		}
		b.orderBy = append(b.orderBy, part)
	}
	return b
}

// Limit sets an explicit LIMIT value.
func (b *SelectBuilder) Limit(n uint64) *SelectBuilder {
	b.limit = &n
	return b
}

// Offset sets an explicit OFFSET value.
func (b *SelectBuilder) Offset(n uint64) *SelectBuilder {
	b.offset = &n
	return b
}

// Prefix appends a raw fragment before the SELECT statement.
func (b *SelectBuilder) Prefix(sql string, args ...any) *SelectBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

// Suffix appends a raw fragment after the SELECT statement.
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
	if b.distinct {
		sb.write("DISTINCT ")
	}
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
	for _, join := range b.joins {
		sb.write(" ")
		sb.write(join.kind)
		sb.write(" ")
		if err := appendExpr(sb, join.target); err != nil {
			return "", nil, err
		}
	}
	if pred := And(b.preds...); pred != nil && !pred.empty() {
		sb.write(" WHERE ")
		if err := renderPredicate(sb, pred, false); err != nil {
			return "", nil, err
		}
	}
	if len(b.groupBy) > 0 {
		sb.write(" GROUP BY ")
		for i, part := range b.groupBy {
			if i > 0 {
				sb.write(", ")
			}
			if err := appendExpr(sb, part); err != nil {
				return "", nil, err
			}
		}
	}
	if pred := And(b.having...); pred != nil && !pred.empty() {
		sb.write(" HAVING ")
		if err := renderPredicate(sb, pred, false); err != nil {
			return "", nil, err
		}
	}
	if len(b.orderBy) > 0 {
		sb.write(" ORDER BY ")
		for i, part := range b.orderBy {
			if i > 0 {
				sb.write(", ")
			}
			if err := appendExpr(sb, part); err != nil {
				return "", nil, err
			}
		}
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
