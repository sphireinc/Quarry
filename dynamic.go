package quarry

import "reflect"

type Filters []Predicate

type SortMap map[string]string

type optionalPredicate struct {
	pred Predicate
}

func (o optionalPredicate) appendSQL(b *sqlBuilder) error {
	if o.pred == nil || o.pred.empty() {
		return nil
	}
	return o.pred.appendSQL(b)
}

func (o optionalPredicate) empty() bool {
	return o.pred == nil || o.pred.empty()
}

func OptionalEq(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "=", value: v}
	}
	return optionalPredicate{}
}

func OptionalNeq(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "<>", value: v}
	}
	return optionalPredicate{}
}

func OptionalGt(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: ">", value: v}
	}
	return optionalPredicate{}
}

func OptionalGte(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: ">=", value: v}
	}
	return optionalPredicate{}
}

func OptionalLt(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "<", value: v}
	}
	return optionalPredicate{}
}

func OptionalLte(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return comparisonPredicate{left: col, op: "<=", value: v}
	}
	return optionalPredicate{}
}

func OptionalLike(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return likePredicate{left: col, value: v, caseInsensitive: false}
	}
	return optionalPredicate{}
}

func OptionalILike(col string, val any) Predicate {
	if v, ok := optionalBindValue(val); ok {
		return likePredicate{left: col, value: v, caseInsensitive: true}
	}
	return optionalPredicate{}
}

func OptionalIn(col string, vals any) Predicate {
	if v, ok := optionalBindSlice(vals); ok {
		return inPredicate{left: col, values: v, not: false}
	}
	return optionalPredicate{}
}

func (b *SelectBuilder) WhereIf(cond bool, pred Predicate) *SelectBuilder {
	if cond && pred != nil && !pred.empty() {
		b.preds = append(b.preds, pred)
	}
	return b
}

func (b *SelectBuilder) OrderBySafe(input string, allowed SortMap) *SelectBuilder {
	if clause, ok := allowed[input]; ok && clause != "" {
		b.orderBy = append(b.orderBy, clause)
	}
	return b
}

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

func (b *SelectBuilder) Page(page, perPage int) *SelectBuilder {
	if page < 1 {
		page = 1
	}
	if perPage < 1 {
		perPage = 50
	}
	b.limit = intPtr(perPage)
	offset := (page - 1) * perPage
	b.offset = intPtr(offset)
	return b
}

func (b *SelectBuilder) LimitDefault(n, fallback int) *SelectBuilder {
	switch {
	case n > 0:
		b.limit = intPtr(n)
	case fallback > 0:
		b.limit = intPtr(fallback)
	}
	return b
}

func (b *SelectBuilder) OffsetDefault(n, fallback int) *SelectBuilder {
	switch {
	case n >= 0:
		b.offset = intPtr(n)
	case fallback >= 0:
		b.offset = intPtr(fallback)
	}
	return b
}

func (b *UpdateBuilder) WhereIf(cond bool, pred Predicate) *UpdateBuilder {
	if cond && pred != nil && !pred.empty() {
		b.preds = append(b.preds, pred)
	}
	return b
}

func (b *UpdateBuilder) SetIf(cond bool, col string, val any) *UpdateBuilder {
	if cond && col != "" {
		b.sets = append(b.sets, setClause{col: col, val: val})
	}
	return b
}

func (b *UpdateBuilder) SetOptional(col string, val any) *UpdateBuilder {
	if v, ok := optionalBindValue(val); ok && col != "" {
		b.sets = append(b.sets, setClause{col: col, val: v})
	}
	return b
}

func (b *DeleteBuilder) WhereIf(cond bool, pred Predicate) *DeleteBuilder {
	if cond && pred != nil && !pred.empty() {
		b.preds = append(b.preds, pred)
	}
	return b
}

func optionalBindValue(v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	rv := reflect.ValueOf(v)
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

func optionalBindSlice(v any) (any, bool) {
	if v == nil {
		return nil, false
	}
	rv := reflect.ValueOf(v)
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
		return nil, false
	}
}

func intPtr(v int) *int {
	return &v
}
