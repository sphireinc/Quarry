package quarry

import (
	"fmt"
	"strings"
)

type setClause struct {
	col string
	val any
}

type UpdateBuilder struct {
	q         *Quarry
	table     any
	sets      []setClause
	preds     []Predicate
	returning []any
	prefix    []rawFragment
	suffix    []rawFragment
}

func (b *UpdateBuilder) Set(col string, val any) *UpdateBuilder {
	if strings.TrimSpace(col) == "" {
		return b
	}
	b.sets = append(b.sets, setClause{col: col, val: val})
	return b
}

func (b *UpdateBuilder) Where(preds ...Predicate) *UpdateBuilder {
	b.preds = append(b.preds, preds...)
	return b
}

func (b *UpdateBuilder) Returning(cols ...any) *UpdateBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

func (b *UpdateBuilder) Prefix(sql string, args ...any) *UpdateBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *UpdateBuilder) Suffix(sql string, args ...any) *UpdateBuilder {
	b.suffix = append(b.suffix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *UpdateBuilder) ToSQL() (string, []any, error) {
	if err := b.q.errOrNil(); err != nil {
		return "", nil, err
	}
	if len(b.sets) == 0 {
		return "", nil, fmt.Errorf("quarry: update requires at least one SET value")
	}
	sb := newSQLBuilder(b.q.dialect)
	for _, frag := range b.prefix {
		if err := sb.appendRaw(frag.sql, frag.args...); err != nil {
			return "", nil, err
		}
	}
	sb.write("UPDATE ")
	if err := appendExpr(sb, b.table); err != nil {
		return "", nil, err
	}
	sb.write(" SET ")
	for i, set := range b.sets {
		if i > 0 {
			sb.write(", ")
		}
		sb.write(set.col)
		sb.write(" = ")
		sb.write(sb.arg(set.val))
	}
	if pred := And(b.preds...); pred != nil && !pred.empty() {
		sb.write(" WHERE ")
		if err := renderPredicate(sb, pred, false); err != nil {
			return "", nil, err
		}
	}
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
