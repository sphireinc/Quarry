package quarry

import "reflect"

// Filters is a convenience alias for a batch of optional predicates.
type Filters []Predicate

// SortMap maps caller-facing sort keys to trusted ORDER BY fragments.
type SortMap map[string]string

// optionalPredicate wraps a predicate that may disappear entirely.
type optionalPredicate struct {
	pred Predicate
}

// appendSQL emits the wrapped predicate only when it is not empty.
func (o optionalPredicate) appendSQL(b *sqlBuilder) error {
	if o.pred == nil || o.pred.empty() {
		return nil
	}
	return o.pred.appendSQL(b)
}

// empty reports whether the wrapped predicate should be omitted.
func (o optionalPredicate) empty() bool {
	return o.pred == nil || o.pred.empty()
}

// OptionalEq returns Eq when val is present and a no-op predicate otherwise.
func OptionalEq(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "=", value: v}
	}
	return optionalPredicate{}
}

// OptionalNeq returns Neq when val is present and a no-op predicate otherwise.
func OptionalNeq(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "<>", value: v}
	}
	return optionalPredicate{}
}

// OptionalGt returns Gt when val is present and a no-op predicate otherwise.
func OptionalGt(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: ">", value: v}
	}
	return optionalPredicate{}
}

// OptionalGte returns Gte when val is present and a no-op predicate otherwise.
func OptionalGte(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: ">=", value: v}
	}
	return optionalPredicate{}
}

// OptionalLt returns Lt when val is present and a no-op predicate otherwise.
func OptionalLt(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "<", value: v}
	}
	return optionalPredicate{}
}

// OptionalLte returns Lte when val is present and a no-op predicate otherwise.
func OptionalLte(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "<=", value: v}
	}
	return optionalPredicate{}
}

// OptionalLike returns Like when val is present and a no-op predicate otherwise.
func OptionalLike(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return likePredicate{left: col, value: v, caseInsensitive: false}
	}
	return optionalPredicate{}
}

// OptionalILike returns ILike when val is present and a no-op predicate otherwise.
func OptionalILike(col any, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return likePredicate{left: col, value: v, caseInsensitive: true}
	}
	return optionalPredicate{}
}

// OptionalIn returns In when vals is present and a no-op predicate otherwise.
func OptionalIn(col any, values ...any) Predicate {
	normalized, empty, _ := normalizeINValues(values)
	if empty {
		return optionalPredicate{}
	}
	return inPredicate{left: col, values: normalized, not: false}
}

// WhereIf appends pred only when cond is true and the predicate is non-empty.
func (b *SelectBuilder) WhereIf(cond bool, pred Predicate) *SelectBuilder {
	if cond && pred != nil && !pred.empty() {
		b.preds = append(b.preds, pred)
	}
	return b
}

// OrderBySafe appends a trusted ORDER BY fragment selected from allowed.
func (b *SelectBuilder) OrderBySafe(input string, allowed SortMap) *SelectBuilder {
	if clause, ok := allowed[input]; ok && clause != "" {
		b.orderBy = append(b.orderBy, clause)
	}
	return b
}

// OrderBySafeDefault appends the selected sort key or falls back to a trusted default.
func (b *SelectBuilder) OrderBySafeDefault(input string, allowed SortMap, fallback string) *SelectBuilder {
	if clause, ok := allowed[input]; ok && clause != "" {
		b.orderBy = append(b.orderBy, clause)
		return b
	}
	if clause, ok := allowed[fallback]; ok && clause != "" {
		b.orderBy = append(b.orderBy, clause)
	}
	return b
}

// Page applies one-based page/per-page pagination and derives LIMIT/OFFSET.
func (b *SelectBuilder) Page(page, perPage int) *SelectBuilder {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 50
	}
	limit := uint64(perPage)
	b.limit = &limit
	offset := uint64((page - 1) * perPage)
	b.offset = &offset
	return b
}

// LimitDefault applies n when it is positive, otherwise a positive fallback.
func (b *SelectBuilder) LimitDefault(n, fallback int) *SelectBuilder {
	switch {
	case n > 0:
		limit := uint64(n)
		b.limit = &limit
	case fallback > 0:
		limit := uint64(fallback)
		b.limit = &limit
	}
	return b
}

// OffsetDefault applies n when it is non-negative, otherwise a non-negative fallback.
func (b *SelectBuilder) OffsetDefault(n, fallback int) *SelectBuilder {
	switch {
	case n >= 0:
		offset := uint64(n)
		b.offset = &offset
	case fallback >= 0:
		offset := uint64(fallback)
		b.offset = &offset
	}
	return b
}

// WhereIf appends pred only when cond is true and the predicate is non-empty.
func (b *UpdateBuilder) WhereIf(cond bool, pred Predicate) *UpdateBuilder {
	if cond && pred != nil && !pred.empty() {
		b.preds = append(b.preds, pred)
	}
	return b
}

// SetIf appends a SET clause only when cond is true.
func (b *UpdateBuilder) SetIf(cond bool, col any, val any) *UpdateBuilder {
	if cond && col != nil {
		b.sets = append(b.sets, setClause{col: col, val: val})
	}
	return b
}

// SetOptional appends a SET clause only when val is a present, non-empty value.
func (b *UpdateBuilder) SetOptional(col any, val any) *UpdateBuilder {
	if v, ok := optionalBindValue(val); ok && col != nil {
		b.sets = append(b.sets, setClause{col: col, val: v})
	}
	return b
}

// WhereIf appends pred only when cond is true and the predicate is non-empty.
func (b *DeleteBuilder) WhereIf(cond bool, pred Predicate) *DeleteBuilder {
	if cond && pred != nil && !pred.empty() {
		b.preds = append(b.preds, pred)
	}
	return b
}

// optionalBindValue normalizes optional inputs and filters empty values.
func optionalBindValue(v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	rv := reflect.ValueOf(v)
	// Follow pointer chains so pointer-to-pointer inputs still behave predictably.
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, false
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil, false
	}
	switch rv.Kind() {
	case reflect.String:
		if rv.Len() == 0 {
			return nil, false
		}
		return rv.String(), true
	case reflect.Slice:
		if rv.IsNil() || rv.Len() == 0 {
			return nil, false
		}
		return rv.Interface(), true
	case reflect.Array:
		if rv.Len() == 0 {
			return nil, false
		}
		return rv.Interface(), true
	default:
		return rv.Interface(), true
	}
}
