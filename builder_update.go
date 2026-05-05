package quarry

import (
	"fmt"
	"sort"
	"strings"
)

type setClause struct {
	col any
	val any
}

// UpdateBuilder renders UPDATE statements for the active Quarry dialect.
type UpdateBuilder struct {
	q         *Quarry
	table     any
	sets      []setClause
	preds     []Predicate
	returning []any
	prefix    []rawFragment
	suffix    []rawFragment
}

// Set appends an explicit SET clause.
func (b *UpdateBuilder) Set(col any, val any) *UpdateBuilder {
	if s, ok := col.(string); ok && strings.TrimSpace(s) == "" {
		return b
	}
	b.sets = append(b.sets, setClause{col: col, val: val})
	return b
}

// SetMap appends deterministic SET clauses from a map.
func (b *UpdateBuilder) SetMap(values map[string]any) *UpdateBuilder {
	if len(values) == 0 {
		return b
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		b.sets = append(b.sets, setClause{col: key, val: values[key]})
	}
	return b
}

// Where appends UPDATE predicates.
func (b *UpdateBuilder) Where(preds ...Predicate) *UpdateBuilder {
	b.preds = append(b.preds, preds...)
	return b
}

// Returning appends RETURNING expressions.
func (b *UpdateBuilder) Returning(cols ...any) *UpdateBuilder {
	b.returning = append(b.returning, cols...)
	return b
}

// Prefix appends a raw fragment before the UPDATE statement.
func (b *UpdateBuilder) Prefix(sql string, args ...any) *UpdateBuilder {
	b.prefix = append(b.prefix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

// Suffix appends a raw fragment after the UPDATE statement.
func (b *UpdateBuilder) Suffix(sql string, args ...any) *UpdateBuilder {
	b.suffix = append(b.suffix, rawFragment{sql: sql, args: append([]any(nil), args...)})
	return b
}

func (b *UpdateBuilder) ToSQL() (string, []any, error) {
	if err := b.q.errOrNil(); err != nil {
		return "", nil, err
	}
	if err := requireTableValue(b.table, "update"); err != nil {
		return "", nil, err
	}
	if len(b.sets) == 0 {
		return "", nil, fmt.Errorf("quarry: update requires at least one SET value")
	}
	setCols := make([]any, 0, len(b.sets))
	for _, set := range b.sets {
		setCols = append(setCols, set.col)
	}
	if err := ensureUniqueColumnKeys("update", setCols); err != nil {
		return "", nil, err
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
		if err := appendExpr(sb, set.col); err != nil {
			return "", nil, err
		}
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
