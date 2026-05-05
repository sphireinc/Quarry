package quarry

import (
	"fmt"
)

// DeleteBuilder renders DELETE statements for the active Quarry dialect.
type DeleteBuilder struct {
	q         *Quarry
	table     any
	preds     []Predicate
	returning []any
	prefix    []rawFragment
	suffix    []rawFragment
}

// Where appends DELETE predicates.
func (b *DeleteBuilder) Where(preds ...Predicate) *DeleteBuilder {
	b.preds = append(b.preds, preds...)
	return b
}

// Returning appends RETURNING expressions.
func (b *DeleteBuilder) Returning(cols ...any) *DeleteBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

// Prefix appends a raw fragment before the DELETE statement.
func (b *DeleteBuilder) Prefix(sql string, args ...any) *DeleteBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

// Suffix appends a raw fragment after the DELETE statement.
func (b *DeleteBuilder) Suffix(sql string, args ...any) *DeleteBuilder {
	b.suffix = append(b.suffix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *DeleteBuilder) ToSQL() (string, []any, error) {
	if err := b.q.errOrNil(); err != nil {
		return "", nil, err
	}
	if err := requireTableValue(b.table, "delete"); err != nil {
		return "", nil, err
	}
	sb := newSQLBuilder(b.q.dialect)
	for _, frag := range b.prefix {
		if err := sb.appendRaw(frag.sql, frag.args...); err != nil {
			return "", nil, err
		}
	}
	sb.write("DELETE FROM ")
	if err := appendExpr(sb, b.table); err != nil {
		return "", nil, err
	}
	if pred := And(b.preds...); pred != nil && !pred.empty() {
		sb.write(" WHERE ")
		if err := renderPredicate(sb, pred, false); err != nil {
			return "", nil, err
		}
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
