package quarry

import (
	"fmt"
	"reflect"

	"github.com/sphireinc/quarry/internal/rawsql"
)

// Expr is the minimal contract for SQL fragments Quarry can render.
//
// It stays unexported in practice because only Quarry's own expression types
// can satisfy the interface.
type Expr interface {
	appendSQL(*sqlBuilder) error
}

// Predicate is a boolean SQL expression that may also be empty and therefore omitted.
type Predicate interface {
	Expr
	empty() bool
}

// Table identifies a SQL table name and can produce qualified columns from it.
type Table struct {
	name  string
	alias string
}

// Column identifies a SQL column, optionally qualified by a table.
type Column struct {
	table *Table
	name  string
	alias string
}

// T constructs a safe table helper for SQL rendering and column qualification.
func T(name string) Table {
	return Table{name: name}
}

// TableName is a named alias for T.
func TableName(name string) Table {
	return T(name)
}

// C constructs a safe column helper without table qualification.
func C(name string) Column {
	return Column{name: name}
}

// Col is a named alias for C.
func Col(name string) Column {
	return C(name)
}

// As returns a copy of the table with the supplied alias.
func (t Table) As(alias string) Table {
	t.alias = alias
	return t
}

// C returns a column helper qualified by the receiver table.
func (t Table) C(name string) Column {
	tcopy := t
	return Column{table: &tcopy, name: name}
}

// Col returns a column helper qualified by the receiver table (wraper).
func (t Table) Col(name string) Column {
	return t.C(name)
}

// As returns a copy of the column with the supplied alias.
func (c Column) As(alias string) Column {
	c.alias = alias
	return c
}

// appendSQL renders the table as a quoted identifier with an optional quoted alias.
func (t Table) appendSQL(b *sqlBuilder) error {
	if err := appendQuotedIdentifier(b, t.name); err != nil {
		return err
	}
	if t.alias != "" {
		b.write(" AS ")
		if err := appendQuotedIdentifier(b, t.alias); err != nil {
			return err
		}
	}
	return nil
}

// appendSQL renders the column as a quoted identifier with an optional quoted alias.
func (c Column) appendSQL(b *sqlBuilder) error {
	if c.table != nil && c.table.name != "" {
		if c.table.alias != "" {
			if err := appendQuotedIdentifier(b, c.table.alias); err != nil {
				return err
			}
		} else {
			if err := appendQuotedIdentifier(b, c.table.name); err != nil {
				return err
			}
		}
		b.write(".")
	}
	if err := appendQuotedIdentifier(b, c.name); err != nil {
		return err
	}
	if c.alias != "" {
		b.write(" AS ")
		if err := appendQuotedIdentifier(b, c.alias); err != nil {
			return err
		}
	}
	return nil
}

// Eq returns a column comparison predicate using =.
func (c Column) Eq(val any) Predicate { return comparisonPredicate{left: c, op: "=", value: val} }

// Neq returns a column comparison predicate using <>.
func (c Column) Neq(val any) Predicate { return comparisonPredicate{left: c, op: "<>", value: val} }

// Gt returns a column comparison predicate using >.
func (c Column) Gt(val any) Predicate { return comparisonPredicate{left: c, op: ">", value: val} }

// Gte returns a column comparison predicate using >=.
func (c Column) Gte(val any) Predicate { return comparisonPredicate{left: c, op: ">=", value: val} }

// Lt returns a column comparison predicate using <.
func (c Column) Lt(val any) Predicate { return comparisonPredicate{left: c, op: "<", value: val} }

// Lte returns a column comparison predicate using <=.
func (c Column) Lte(val any) Predicate { return comparisonPredicate{left: c, op: "<=", value: val} }

// Like returns a LIKE predicate for the column.
func (c Column) Like(val any) Predicate {
	return likePredicate{left: c, value: val, caseInsensitive: false}
}

// ILike returns a case-insensitive LIKE predicate for the column.
func (c Column) ILike(val any) Predicate {
	return likePredicate{left: c, value: val, caseInsensitive: true}
}

// IsNull returns an IS NULL predicate for the column.
func (c Column) IsNull() Predicate { return nullPredicate{left: c, not: false} }

// IsNotNull returns an IS NOT NULL predicate for the column.
func (c Column) IsNotNull() Predicate { return nullPredicate{left: c, not: true} }

// In returns an IN predicate for the column.
func (c Column) In(values ...any) Predicate { return In(c, values...) }

// NotIn returns a NOT IN predicate for the column.
func (c Column) NotIn(values ...any) Predicate { return NotIn(c, values...) }

// Between returns a BETWEEN predicate for the column.
func (c Column) Between(low any, high any) Predicate { return Between(c, low, high) }

// Any returns a Postgres ANY predicate for the column.
func (c Column) Any(values any) Predicate { return Any(c, values) }

// rawPredicate wraps literal SQL with bound arguments.
type rawPredicate struct {
	sql  string
	args []any
}

// Raw injects a raw SQL fragment while still binding values safely.
func Raw(sql string, args ...any) Predicate {
	return rawPredicate{sql: sql, args: append([]any(nil), args...)}
}

// appendSQL rewrites the raw fragment against the active dialect.
func (r rawPredicate) appendSQL(b *sqlBuilder) error {
	return b.appendRaw(r.sql, r.args...)
}

// empty reports that raw predicates are always rendered.
func (r rawPredicate) empty() bool { return false }

// comparisonPredicate renders a binary comparison between a left expression and a bound value.
type comparisonPredicate struct {
	left  any
	op    string
	value any
}

// Eq returns a comparison predicate using =.
func Eq(col string, val any) Predicate { return comparisonPredicate{left: col, op: "=", value: val} }

// Neq returns a comparison predicate using <>.
func Neq(col string, val any) Predicate { return comparisonPredicate{left: col, op: "<>", value: val} }

// Gt returns a comparison predicate using >.
func Gt(col string, val any) Predicate { return comparisonPredicate{left: col, op: ">", value: val} }

// Gte returns a comparison predicate using >=.
func Gte(col string, val any) Predicate { return comparisonPredicate{left: col, op: ">=", value: val} }

// Lt returns a comparison predicate using <.
func Lt(col string, val any) Predicate { return comparisonPredicate{left: col, op: "<", value: val} }

// Lte returns a comparison predicate using <=.
func Lte(col string, val any) Predicate { return comparisonPredicate{left: col, op: "<=", value: val} }

// Like returns a LIKE predicate using a bound value.
func Like(col string, val any) Predicate {
	return likePredicate{left: col, value: val, caseInsensitive: false}
}

// ILike returns a case-insensitive LIKE predicate using a bound value.
func ILike(col string, val any) Predicate {
	return likePredicate{left: col, value: val, caseInsensitive: true}
}

// IsNull returns an IS NULL predicate.
func IsNull(col string) Predicate { return nullPredicate{left: col, not: false} }

// IsNotNull returns an IS NOT NULL predicate.
func IsNotNull(col string) Predicate { return nullPredicate{left: col, not: true} }

// In returns an IN predicate using bound values.
func In(col any, values ...any) Predicate {
	return inPredicate{left: col, values: append([]any(nil), values...), not: false}
}

// NotIn returns a NOT IN predicate using bound values.
func NotIn(col any, values ...any) Predicate {
	return inPredicate{left: col, values: append([]any(nil), values...), not: true}
}

// Between returns a BETWEEN predicate using bound low/high values.
func Between(col any, low any, high any) Predicate {
	return betweenPredicate{left: col, low: low, high: high}
}

// Any returns a Postgres-specific ANY predicate.
func Any(col any, values any) Predicate {
	return anyPredicate{left: col, values: values}
}

// Exists returns an EXISTS predicate for a subquery.
func Exists(query Query) Predicate {
	return existsPredicate{query: query}
}

// NotExists returns a NOT EXISTS predicate for a subquery.
func NotExists(query Query) Predicate {
	return existsPredicate{query: query, not: true}
}

// appendSQL renders the comparison as `left op arg`.
func (p comparisonPredicate) appendSQL(b *sqlBuilder) error {
	if isNilValue(p.value) {
		if err := appendExpr(b, p.left); err != nil {
			return err
		}
		switch p.op {
		case "=":
			b.write(" IS NULL")
			return nil
		case "<>":
			b.write(" IS NOT NULL")
			return nil
		default:
			return fmt.Errorf("quarry: nil value not allowed for comparison %q: %w", p.op, ErrInvalidBuilderState)
		}
	}
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	b.write(" ")
	b.write(p.op)
	b.write(" ")
	b.write(b.arg(p.value))
	return nil
}

// empty reports that comparison predicates always render.
func (p comparisonPredicate) empty() bool { return false }

// likePredicate renders LIKE or ILIKE, with a portable fallback for non-Postgres dialects.
type likePredicate struct {
	left            any
	value           any
	caseInsensitive bool
}

// appendSQL renders the LIKE predicate using the active dialect's syntax.
func (p likePredicate) appendSQL(b *sqlBuilder) error {
	if p.caseInsensitive && !b.dialect.Supports(FeatureILike) {
		// MySQL and SQLite do not support ILIKE, so fall back to LOWER(lhs) LIKE LOWER(rhs).
		b.write("LOWER(")
		if err := appendExpr(b, p.left); err != nil {
			return err
		}
		b.write(") LIKE LOWER(")
		b.write(b.arg(p.value))
		b.write(")")
		return nil
	}
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	if p.caseInsensitive {
		b.write(" ILIKE ")
	} else {
		b.write(" LIKE ")
	}
	b.write(b.arg(p.value))
	return nil
}

// empty reports that LIKE predicates always render.
func (p likePredicate) empty() bool { return false }

// nullPredicate renders IS NULL or IS NOT NULL checks.
type nullPredicate struct {
	left any
	not  bool
}

// appendSQL renders the NULL check against the active expression.
func (p nullPredicate) appendSQL(b *sqlBuilder) error {
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	if p.not {
		b.write(" IS NOT NULL")
		return nil
	}
	b.write(" IS NULL")
	return nil
}

// empty reports that NULL predicates always render.
func (p nullPredicate) empty() bool { return false }

// inPredicate renders IN and NOT IN checks over a slice or array.
type inPredicate struct {
	left   any
	values []any
	not    bool
}

// appendSQL renders the IN predicate and binds each element independently.
func (p inPredicate) appendSQL(b *sqlBuilder) error {
	values, empty, err := normalizeINValues(p.values)
	if err != nil {
		return err
	}
	if empty {
		// Empty IN lists intentionally collapse to a constant predicate so callers
		// do not have to special-case "no filter" inputs.
		b.write("1")
		if p.not {
			b.write(" = 1")
		} else {
			b.write(" = 0")
		}
		return nil
	}
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	// Bind each value separately so placeholder numbering stays deterministic.
	if p.not {
		b.write(" NOT IN (")
	} else {
		b.write(" IN (")
	}
	for i, val := range values {
		if i > 0 {
			b.write(", ")
		}
		b.write(b.arg(val))
	}
	b.write(")")
	return nil
}

// empty reports that IN predicates always render.
func (p inPredicate) empty() bool { return false }

// betweenPredicate renders a BETWEEN comparison over two bound values.
type betweenPredicate struct {
	left any
	low  any
	high any
}

// appendSQL renders the BETWEEN predicate.
func (p betweenPredicate) appendSQL(b *sqlBuilder) error {
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	b.write(" BETWEEN ")
	b.write(b.arg(p.low))
	b.write(" AND ")
	b.write(b.arg(p.high))
	return nil
}

// empty reports that BETWEEN always renders.
func (p betweenPredicate) empty() bool { return false }

// anyPredicate renders a Postgres ANY comparison.
type anyPredicate struct {
	left   any
	values any
}

// appendSQL renders the ANY predicate.
func (p anyPredicate) appendSQL(b *sqlBuilder) error {
	if !b.dialect.Supports(FeatureAny) {
		return fmt.Errorf("quarry: ANY is only supported for postgres: %w", ErrUnsupportedFeature)
	}
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	b.write(" = ANY(")
	b.write(b.arg(p.values))
	b.write(")")
	return nil
}

// empty reports that ANY always renders.
func (p anyPredicate) empty() bool { return false }

// existsPredicate renders EXISTS or NOT EXISTS around a subquery.
type existsPredicate struct {
	query Query
	not   bool
}

// appendSQL renders the subquery and offsets Postgres placeholders when needed.
func (p existsPredicate) appendSQL(b *sqlBuilder) error {
	if isNilQuery(p.query) {
		return fmt.Errorf("quarry: EXISTS requires a query: %w", ErrInvalidBuilderState)
	}
	if p.not {
		b.write("NOT ")
	}
	b.write("EXISTS (")
	if err := appendSubquery(b, p.query); err != nil {
		return err
	}
	b.write(")")
	return nil
}

// empty reports that EXISTS always renders.
func (p existsPredicate) empty() bool { return false }

// tupleInPredicate renders composite IN checks over multiple columns.
type tupleInPredicate struct {
	columns []any
	tuples  [][]any
}

// TupleIn returns a composite IN predicate over multiple columns.
func TupleIn(columns []any, tuples [][]any) Predicate {
	copiedCols := append([]any(nil), columns...)
	copiedRows := make([][]any, 0, len(tuples))
	for _, tuple := range tuples {
		copiedRows = append(copiedRows, append([]any(nil), tuple...))
	}
	return tupleInPredicate{columns: copiedCols, tuples: copiedRows}
}

// appendSQL renders the tuple IN predicate.
func (p tupleInPredicate) appendSQL(b *sqlBuilder) error {
	if len(p.columns) == 0 {
		return fmt.Errorf("quarry: tuple IN requires at least one column")
	}
	if len(p.tuples) == 0 {
		b.write("1 = 0")
		return nil
	}
	for i, tuple := range p.tuples {
		if len(tuple) != len(p.columns) {
			return fmt.Errorf("quarry: tuple IN row %d has %d values, want %d", i, len(tuple), len(p.columns))
		}
	}
	b.write("(")
	for i, col := range p.columns {
		if i > 0 {
			b.write(", ")
		}
		if err := appendExpr(b, col); err != nil {
			return err
		}
	}
	b.write(") IN (")
	for i, tuple := range p.tuples {
		if i > 0 {
			b.write(", ")
		}
		b.write("(")
		for j, val := range tuple {
			if j > 0 {
				b.write(", ")
			}
			b.write(b.arg(val))
		}
		b.write(")")
	}
	b.write(")")
	return nil
}

// empty reports that tuple IN always renders.
func (p tupleInPredicate) empty() bool { return false }

// groupPredicate represents a flattened AND/OR group of child predicates.
type groupPredicate struct {
	op    string
	preds []Predicate
}

// And joins predicates with AND, dropping empty children.
func And(preds ...Predicate) Predicate { return newGroupPredicate("AND", preds...) }

// Or joins predicates with OR, dropping empty children.
func Or(preds ...Predicate) Predicate { return newGroupPredicate("OR", preds...) }

// Not negates a predicate while preserving empty/no-op behavior.
func Not(pred Predicate) Predicate {
	if pred == nil || pred.empty() {
		return nil
	}
	return notPredicate{pred: pred}
}

// newGroupPredicate flattens same-op groups and removes empty children up front.
func newGroupPredicate(op string, preds ...Predicate) Predicate {
	flat := make([]Predicate, 0, len(preds))
	for _, pred := range preds {
		if pred == nil || pred.empty() {
			continue
		}
		if gp, ok := pred.(groupPredicate); ok && gp.op == op {
			flat = append(flat, gp.preds...)
			continue
		}
		if gp, ok := pred.(*groupPredicate); ok && gp.op == op {
			flat = append(flat, gp.preds...)
			continue
		}
		flat = append(flat, pred)
	}
	return groupPredicate{op: op, preds: flat}
}

// appendSQL renders the group while preserving grouping when nested.
func (g groupPredicate) appendSQL(b *sqlBuilder) error {
	return renderPredicate(b, g, false)
}

// empty reports whether every child predicate is empty.
func (g groupPredicate) empty() bool {
	for _, pred := range g.preds {
		if pred != nil && !pred.empty() {
			return false
		}
	}
	return true
}

// notPredicate negates a child predicate.
type notPredicate struct {
	pred Predicate
}

// appendSQL renders NOT (...) around the child predicate.
func (n notPredicate) appendSQL(b *sqlBuilder) error {
	return renderPredicate(b, n, false)
}

// empty reports whether the negated predicate is empty.
func (n notPredicate) empty() bool {
	return n.pred == nil || n.pred.empty()
}

// appendExpr renders any supported SQL expression into the builder.
func appendExpr(b *sqlBuilder, v any) error {
	switch x := v.(type) {
	case nil:
		return fmt.Errorf("quarry: nil expression")
	case string:
		b.write(x)
		return nil
	case Table:
		// Tables render as bare identifiers.
		return x.appendSQL(b)
	case *Table:
		if x == nil {
			return fmt.Errorf("quarry: nil table")
		}
		// Pointer table helpers are accepted for ergonomics.
		return x.appendSQL(b)
	case Column:
		// Columns render as bare or qualified identifiers.
		return x.appendSQL(b)
	case *Column:
		if x == nil {
			return fmt.Errorf("quarry: nil column")
		}
		// Pointer column helpers are accepted for ergonomics.
		return x.appendSQL(b)
	case Expr:
		if isNilValue(x) {
			return fmt.Errorf("quarry: nil expression")
		}
		// Nested expressions recurse through the same rendering path.
		return x.appendSQL(b)
	case fmt.Stringer:
		if isNilValue(x) {
			return fmt.Errorf("quarry: nil expression")
		}
		// Stringers are treated as trusted SQL fragments.
		b.write(x.String())
		return nil
	default:
		return fmt.Errorf("quarry: unsupported expression type %T", v)
	}
}

// appendSubquery renders a subquery and rewrites placeholders when nesting Postgres queries.
func appendSubquery(b *sqlBuilder, q Query) error {
	if isNilQuery(q) {
		return fmt.Errorf("quarry: nil query: %w", ErrInvalidBuilderState)
	}
	sqlText, args, err := q.ToSQL()
	if err != nil {
		return err
	}
	switch b.dialect {
	case Postgres:
		// Nested Postgres queries need placeholder renumbering so outer args stay stable.
		rewritten, err := rawsql.OffsetDollarPlaceholders(sqlText, len(b.args))
		if err != nil {
			return err
		}
		b.write(rewritten)
	default:
		b.write(sqlText)
	}
	b.args = append(b.args, args...)
	return nil
}

// isNilQuery catches typed-nil builders so EXISTS can fail cleanly instead of panicking.
func isNilQuery(q Query) bool {
	return isNilValue(q)
}

// isNilValue catches typed nils hiding behind interfaces.
func isNilValue(v any) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Interface, reflect.Pointer, reflect.Slice, reflect.Map, reflect.Func, reflect.Chan:
		return rv.IsNil()
	default:
		return false
	}
}

// normalizeINValues expands the variadic IN form into a stable value list.
//
// A single slice or array argument is treated as the value set so callers can
// use either `In("id", []int{1, 2})` or `In("id", 1, 2)`.
func normalizeINValues(values []any) ([]any, bool, error) {
	switch len(values) {
	case 0:
		return nil, true, nil
	case 1:
		expanded, ok, err := normalizeSingleINValue(values[0])
		if err != nil {
			return nil, false, err
		}
		if ok {
			return expanded, len(expanded) == 0, nil
		}
	}
	return append([]any(nil), values...), false, nil
}

// normalizeSingleINValue expands a slice-like single IN argument.
func normalizeSingleINValue(value any) ([]any, bool, error) {
	if value == nil {
		return nil, true, nil
	}
	rv := reflect.ValueOf(value)
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, true, nil
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() {
		return nil, true, nil
	}
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		out := make([]any, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			out[i] = rv.Index(i).Interface()
		}
		return out, true, nil
	default:
		return nil, false, nil
	}
}

// renderPredicate renders predicates while handling groups and negation recursively.
func renderPredicate(b *sqlBuilder, p Predicate, nested bool) error {
	if p == nil || p.empty() {
		return nil
	}
	switch x := p.(type) {
	case groupPredicate:
		return renderGroupPredicate(b, x, nested)
	case *groupPredicate:
		return renderGroupPredicate(b, *x, nested)
	case notPredicate:
		return renderNotPredicate(b, x)
	case *notPredicate:
		return renderNotPredicate(b, *x)
	default:
		return x.appendSQL(b)
	}
}

// renderGroupPredicate renders an AND/OR group and elides empty children.
func renderGroupPredicate(b *sqlBuilder, g groupPredicate, nested bool) error {
	children := make([]Predicate, 0, len(g.preds))
	for _, pred := range g.preds {
		if pred == nil || pred.empty() {
			continue
		}
		children = append(children, pred)
	}
	switch len(children) {
	case 0:
		return nil
	case 1:
		// Collapse single-child groups to avoid unnecessary parentheses.
		return renderPredicate(b, children[0], nested)
	}

	if nested {
		// Parenthesize nested multi-child groups for correctness.
		b.write("(")
	}
	for i, pred := range children {
		if i > 0 {
			b.write(" ")
			b.write(g.op)
			b.write(" ")
		}
		if err := renderPredicate(b, pred, true); err != nil {
			return err
		}
	}
	if nested {
		b.write(")")
	}
	return nil
}

// renderNotPredicate renders NOT (...) around a child predicate.
func renderNotPredicate(b *sqlBuilder, n notPredicate) error {
	if n.pred == nil || n.pred.empty() {
		return nil
	}
	b.write("NOT (")
	if err := renderPredicate(b, n.pred, true); err != nil {
		return err
	}
	b.write(")")
	return nil
}
