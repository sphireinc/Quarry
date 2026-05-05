package quarry

import (
	"fmt"
	"reflect"
)

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
	name string
}

// Column identifies a SQL column, optionally qualified by a table.
type Column struct {
	table *Table
	name  string
}

// T constructs a table helper for SQL rendering and column qualification.
func T(name string) Table {
	return Table{name: name}
}

// C returns a column helper qualified by the receiver table.
func (t Table) C(name string) Column {
	tcopy := t
	return Column{table: &tcopy, name: name}
}

// appendSQL renders the table name directly into the SQL buffer.
func (t Table) appendSQL(b *sqlBuilder) error {
	b.write(t.name)
	return nil
}

// appendSQL renders the column, qualifying it with the table when present.
func (c Column) appendSQL(b *sqlBuilder) error {
	if c.table != nil && c.table.name != "" {
		b.write(c.table.name)
		b.write(".")
	}
	b.write(c.name)
	return nil
}

// Eq returns a column comparison predicate using =.
func (c Column) Eq(val any) Predicate  { return comparisonPredicate{left: c, op: "=", value: val} }
// Neq returns a column comparison predicate using <>.
func (c Column) Neq(val any) Predicate { return comparisonPredicate{left: c, op: "<>", value: val} }
// Gt returns a column comparison predicate using >.
func (c Column) Gt(val any) Predicate  { return comparisonPredicate{left: c, op: ">", value: val} }
// Gte returns a column comparison predicate using >=.
func (c Column) Gte(val any) Predicate { return comparisonPredicate{left: c, op: ">=", value: val} }
// Lt returns a column comparison predicate using <.
func (c Column) Lt(val any) Predicate  { return comparisonPredicate{left: c, op: "<", value: val} }
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
func (c Column) IsNull() Predicate        { return nullPredicate{left: c, not: false} }
// IsNotNull returns an IS NOT NULL predicate for the column.
func (c Column) IsNotNull() Predicate     { return nullPredicate{left: c, not: true} }
// In returns an IN predicate for the column.
func (c Column) In(vals any) Predicate    { return inPredicate{left: c, values: vals, not: false} }
// NotIn returns a NOT IN predicate for the column.
func (c Column) NotIn(vals any) Predicate { return inPredicate{left: c, values: vals, not: true} }

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
func Eq(col string, val any) Predicate  { return comparisonPredicate{left: col, op: "=", value: val} }
// Neq returns a comparison predicate using <>.
func Neq(col string, val any) Predicate { return comparisonPredicate{left: col, op: "<>", value: val} }
// Gt returns a comparison predicate using >.
func Gt(col string, val any) Predicate  { return comparisonPredicate{left: col, op: ">", value: val} }
// Gte returns a comparison predicate using >=.
func Gte(col string, val any) Predicate { return comparisonPredicate{left: col, op: ">=", value: val} }
// Lt returns a comparison predicate using <.
func Lt(col string, val any) Predicate  { return comparisonPredicate{left: col, op: "<", value: val} }
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
func IsNull(col string) Predicate    { return nullPredicate{left: col, not: false} }
// IsNotNull returns an IS NOT NULL predicate.
func IsNotNull(col string) Predicate { return nullPredicate{left: col, not: true} }
// In returns an IN predicate using bound values.
func In(col string, vals any) Predicate {
	return inPredicate{left: col, values: vals, not: false}
}
// NotIn returns a NOT IN predicate using bound values.
func NotIn(col string, vals any) Predicate {
	return inPredicate{left: col, values: vals, not: true}
}

// appendSQL renders the comparison as `left op arg`.
func (p comparisonPredicate) appendSQL(b *sqlBuilder) error {
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
	if p.caseInsensitive && b.dialect != Postgres {
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
	values any
	not    bool
}

// appendSQL renders the IN predicate and binds each element independently.
func (p inPredicate) appendSQL(b *sqlBuilder) error {
	if err := appendExpr(b, p.left); err != nil {
		return err
	}
	v := reflect.ValueOf(p.values)
	if !v.IsValid() {
		return fmt.Errorf("quarry: IN requires a slice or array")
	}
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
	default:
		return fmt.Errorf("quarry: IN requires a slice or array")
	}
	if v.Len() == 0 {
		return fmt.Errorf("quarry: IN requires at least one value")
	}
	// Bind each value separately so placeholder numbering stays deterministic.
	if p.not {
		b.write(" NOT IN (")
	} else {
		b.write(" IN (")
	}
	for i := 0; i < v.Len(); i++ {
		if i > 0 {
			b.write(", ")
		}
		b.write(b.arg(v.Index(i).Interface()))
	}
	b.write(")")
	return nil
}

// empty reports that IN predicates always render when they have values.
func (p inPredicate) empty() bool { return false }

// groupPredicate represents a flattened AND/OR group of child predicates.
type groupPredicate struct {
	op    string
	preds []Predicate
}

// And joins predicates with AND, dropping empty children.
func And(preds ...Predicate) Predicate { return newGroupPredicate("AND", preds...) }
// Or joins predicates with OR, dropping empty children.
func Or(preds ...Predicate) Predicate  { return newGroupPredicate("OR", preds...) }

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
		// Nested expressions recurse through the same rendering path.
		return x.appendSQL(b)
	case fmt.Stringer:
		// Stringers are treated as trusted SQL fragments.
		b.write(x.String())
		return nil
	default:
		return fmt.Errorf("quarry: unsupported expression type %T", v)
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
