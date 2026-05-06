// Package codex provides Quarry's optional named-query and recipe registry.
//
// Codex keeps raw SQL visible while giving reusable names to templates and
// builder recipes. It does not execute SQL or replace Quarry's core builders.
package codex

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/internal/rawsql"
)

// StoredQuery is the shared registry contract for named raw queries and recipes.
// It stays intentionally small so callers can store either raw SQL templates or recipe wrappers.
type StoredQuery interface {
	Name() string
}

// StoredRecipe is a named recipe that can build Quarry SQL from a parameter value.
// The registry stores recipes alongside raw templates without turning Codex into a general framework.
type StoredRecipe interface {
	StoredQuery
	Build(*quarry.Quarry, any) (quarry.SQLer, error)
}

// Codex stores named raw queries and recipes in one registry.
// It is an optional layer for reuse; it does not try to own query execution or relation loading.
type Codex struct {
	queries map[string]StoredQuery
}

// New creates an empty codex registry.
func New() *Codex {
	return &Codex{queries: make(map[string]StoredQuery)}
}

// Add stores a named raw query or recipe after validating the name and duplicate state.
func (c *Codex) Add(name string, q StoredQuery) error {
	if c == nil {
		return fmt.Errorf("quarry codex: nil codex")
	}
	if c.queries == nil {
		c.queries = make(map[string]StoredQuery)
	}
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("quarry codex: query name is required")
	}
	if q == nil {
		return fmt.Errorf("quarry codex: query %q is nil", name)
	}
	if existing, ok := c.queries[name]; ok && existing != nil {
		return fmt.Errorf("quarry codex: query %q already exists", name)
	}
	if q.Name() != name {
		return fmt.Errorf("quarry codex: query name mismatch for %q", name)
	}
	c.queries[name] = q
	return nil
}

// Get fetches a stored query by name.
func (c *Codex) Get(name string) (StoredQuery, bool) {
	if c == nil {
		return nil, false
	}
	q, ok := c.queries[name]
	return q, ok
}

// Raw is a compatibility alias for AddRaw.
func (c *Codex) Raw(name, sql string) error {
	return c.AddRaw(name, sql)
}

// RawNamed is a compatibility alias for AddRawNamed.
func (c *Codex) RawNamed(name, sql string) error {
	return c.AddRawNamed(name, sql)
}

// AddRaw stores a positional raw template that uses `?` placeholders.
// The template remains raw SQL, with only placeholder rewriting handled by Quarry.
func (c *Codex) AddRaw(name, sql string) error {
	return c.Add(name, RawQuery{name: name, sql: sql})
}

// AddRawNamed stores a named raw template that uses `:name` placeholders.
// Named templates are useful when the SQL is clearer than a builder but args still need binding.
func (c *Codex) AddRawNamed(name, sql string) error {
	return c.Add(name, RawQuery{name: name, sql: sql, named: true})
}

// Recipe is a compatibility alias for AddRecipe.
func (c *Codex) Recipe(name string, recipe any) error {
	return c.AddRecipe(name, recipe)
}

// AddRecipe stores either a typed recipe wrapper or a function value.
// Recipes are intentionally just SQL-producing helpers; callers still decide when and how to execute them.
func (c *Codex) AddRecipe(name string, recipe any) error {
	if c == nil {
		return fmt.Errorf("quarry codex: nil codex")
	}
	if recipe == nil {
		return fmt.Errorf("quarry codex: recipe %q is nil", name)
	}
	switch r := recipe.(type) {
	case interface{ WithName(string) StoredRecipe }:
		return c.Add(name, r.WithName(name))
	case StoredRecipe:
		if r.Name() != name {
			return fmt.Errorf("quarry codex: query name mismatch for %q", name)
		}
		return c.Add(name, r)
	}
	rv := reflect.ValueOf(recipe)
	if rv.Kind() == reflect.Func {
		return c.Add(name, reflectedRecipe{name: name, fn: rv})
	}
	return fmt.Errorf("quarry codex: unsupported recipe type %T", recipe)
}

// MustRaw returns a raw query and panics if the registry entry is missing or not raw.
// This is the explicit panic-only path; normal code should prefer Add/Get and handle errors.
func (c *Codex) MustRaw(name string) RawQuery {
	q, ok := c.Get(name)
	if !ok {
		panic(fmt.Sprintf("quarry codex: raw query %q not found", name))
	}
	raw, ok := q.(RawQuery)
	if !ok {
		panic(fmt.Sprintf("quarry codex: query %q is not a raw query", name))
	}
	return raw
}

// Must is a shorthand alias for MustRaw.
func (c *Codex) Must(name string) RawQuery {
	return c.MustRaw(name)
}

// MustRecipe returns a recipe and panics if the registry entry is missing or not a recipe.
// It mirrors MustRaw for users who prefer the shorter panic-based lookup.
func (c *Codex) MustRecipe(name string) StoredRecipe {
	q, ok := c.Get(name)
	if !ok {
		panic(fmt.Sprintf("quarry codex: recipe %q not found", name))
	}
	recipe, ok := q.(StoredRecipe)
	if !ok {
		panic(fmt.Sprintf("quarry codex: query %q is not a recipe", name))
	}
	return recipe
}

type rawMode int

const (
	rawPositional rawMode = iota
	rawNamed
)

// RawQuery stores a raw SQL template that can be bound later.
// It keeps the SQL visible and leaves argument binding to the BoundRaw wrapper.
type RawQuery struct {
	name   string
	sql    string
	named  bool
	strict bool
}

// Name returns the registry name for the raw template.
func (r RawQuery) Name() string {
	return r.name
}

// With binds the template to a Quarry dialect before arguments are supplied.
// The dialect controls placeholder rendering while the raw SQL text stays intact.
func (r RawQuery) With(qq *quarry.Quarry) *BoundRaw {
	b := &BoundRaw{template: r}
	if qq == nil {
		b.err = fmt.Errorf("quarry codex: nil quarry")
		return b
	}
	b.dialect = qq.Dialect()
	return b
}

type bindMode int

const (
	bindNone bindMode = iota
	bindPositional
	bindNamed
)

// BoundRaw is a raw template bound to a specific dialect and argument set.
// It is the point where raw text becomes a fully bound Quarry query.
type BoundRaw struct {
	template RawQuery
	dialect  quarry.Dialect
	mode     bindMode
	args     []any
	named    map[string]any
	err      error
}

// Name returns the registry name of the underlying raw template.
func (b *BoundRaw) Name() string {
	if b == nil {
		return ""
	}
	return b.template.Name()
}

// Bind supplies positional arguments for a `?`-based raw query.
// Positional binding keeps the call site simple when the SQL already has the right shape.
func (b *BoundRaw) Bind(args ...any) *BoundRaw {
	if b == nil {
		return nil
	}
	b.mode = bindPositional
	b.args = append([]any(nil), args...)
	b.named = nil
	b.err = nil
	return b
}

// BindMap supplies named arguments for a `:name`-based raw query.
// Named binding is explicit and keeps user-controlled values separate from the SQL text.
func (b *BoundRaw) BindMap(values map[string]any) *BoundRaw {
	if b == nil {
		return nil
	}
	b.mode = bindNamed
	b.args = nil
	b.named = copyStringMap(values)
	b.err = nil
	return b
}

// BindStruct extracts named arguments from a struct before binding them.
// It follows the same db/json/snake_case rules as the scan package.
func (b *BoundRaw) BindStruct(v any) *BoundRaw {
	if b == nil {
		return nil
	}
	values, err := structToMap(v)
	if err != nil {
		b.err = err
		return b
	}
	return b.BindMap(values)
}

// ToSQL rewrites the raw query using the bound dialect and arguments.
// The method returns explicit errors for unsupported dialects, bad bindings, and placeholder mismatches.
func (b *BoundRaw) ToSQL() (string, []any, error) {
	if b == nil {
		return "", nil, fmt.Errorf("quarry codex: nil bound raw query")
	}
	if b.err != nil {
		return "", nil, b.err
	}
	if !supportedDialect(b.dialect) {
		return "", nil, fmt.Errorf("quarry: unsupported dialect %q", b.dialect)
	}
	if b.template.named {
		if b.mode != bindNamed {
			return "", nil, fmt.Errorf("quarry codex: raw query %q requires named bindings", b.template.name)
		}
		sqlText, args, err := rawsql.RewriteNamedPlaceholders(b.template.sql, b.named, func(n int) string {
			return placeholderFor(b.dialect, n)
		}, b.template.strict)
		if err != nil {
			return "", nil, err
		}
		return sqlText, args, nil
	}
	if b.mode != bindPositional {
		return "", nil, fmt.Errorf("quarry codex: raw query %q requires positional bindings", b.template.name)
	}
	sqlText, args, err := rawsql.RewriteQuestionPlaceholders(b.template.sql, b.args, func(n int) string {
		return placeholderFor(b.dialect, n)
	})
	if err != nil {
		if strings.Contains(err.Error(), "raw placeholder count does not match args count") {
			return "", nil, fmt.Errorf("quarry codex: %w", quarry.ErrPlaceholderMismatch)
		}
		return "", nil, err
	}
	return sqlText, args, nil
}

// RecipeFunc is the typed builder signature accepted by recipe wrappers.
// Recipes receive a Quarry and a typed parameter value and must return a SQLer, not execute anything.
type RecipeFunc[P any] func(*quarry.Quarry, P) quarry.SQLer

// Recipe stores a typed recipe function without forcing registry registration.
// It is the small, explicit recipe form Quarry uses for reusable SQL composition.
type Recipe[P any] struct {
	fn RecipeFunc[P]
}

// NewRecipe wraps a typed builder function.
func NewRecipe[P any](fn RecipeFunc[P]) Recipe[P] {
	return Recipe[P]{fn: fn}
}

// Build invokes the typed recipe directly.
// Normal use returns an error instead of panicking so callers can keep recipe failures explicit.
func (r Recipe[P]) Build(qq *quarry.Quarry, p P) (quarry.SQLer, error) {
	if r.fn == nil {
		return nil, fmt.Errorf("quarry codex: recipe is nil")
	}
	if qq == nil {
		return nil, fmt.Errorf("quarry codex: recipe received nil quarry")
	}
	sqler := r.fn(qq, p)
	if sqler == nil {
		return nil, fmt.Errorf("quarry codex: recipe returned nil query")
	}
	return sqler, nil
}

// WithName turns an anonymous typed recipe into a named registry entry.
func (r Recipe[P]) WithName(name string) StoredRecipe {
	return namedRecipe[P]{name: name, fn: r.fn}
}

// namedRecipe is the generic, type-safe stored recipe form.
type namedRecipe[P any] struct {
	name string
	fn   RecipeFunc[P]
}

// Name returns the registry name for the typed recipe.
func (r namedRecipe[P]) Name() string {
	return r.name
}

// Build invokes the typed recipe and returns helpful errors for invalid input.
// The method is strict on the parameter type so the registry remains easy to reason about.
func (r namedRecipe[P]) Build(qq *quarry.Quarry, params any) (quarry.SQLer, error) {
	if r.fn == nil {
		return nil, fmt.Errorf("quarry codex: recipe %q is nil", r.name)
	}
	typed, ok := params.(P)
	if !ok {
		var zero P
		return nil, fmt.Errorf("quarry codex: recipe %q received %T, expected %T", r.name, params, zero)
	}
	sqler := r.fn(qq, typed)
	if sqler == nil {
		return nil, fmt.Errorf("quarry codex: recipe %q returned nil query", r.name)
	}
	return sqler, nil
}

// reflectedRecipe adapts a runtime function value into the recipe contract.
type reflectedRecipe struct {
	name string
	fn   reflect.Value
}

// Name returns the registry name for the reflected recipe.
func (r reflectedRecipe) Name() string {
	return r.name
}

// Build validates the function shape at runtime and then invokes it.
// This keeps the registry flexible for small helper functions without hiding the shape from callers.
func (r reflectedRecipe) Build(qq *quarry.Quarry, params any) (quarry.SQLer, error) {
	if !r.fn.IsValid() {
		return nil, fmt.Errorf("quarry codex: recipe %q is nil", r.name)
	}
	if qq == nil {
		return nil, fmt.Errorf("quarry codex: recipe %q received nil quarry", r.name)
	}
	fnType := r.fn.Type()
	if fnType.Kind() != reflect.Func || fnType.NumIn() != 2 || fnType.NumOut() != 1 {
		return nil, fmt.Errorf("quarry codex: recipe %q has unsupported function signature", r.name)
	}

	// Keep the first parameter strict so recipes stay visually obvious in code review.
	quarryType := reflect.TypeOf((*quarry.Quarry)(nil))
	if !fnType.In(0).AssignableTo(quarryType) {
		return nil, fmt.Errorf("quarry codex: recipe %q must accept *quarry.Quarry as first argument", r.name)
	}

	paramType := fnType.In(1)
	var paramValue reflect.Value
	if params == nil {
		if isNilable(paramType) {
			paramValue = reflect.Zero(paramType)
		} else {
			return nil, fmt.Errorf("quarry codex: recipe %q received nil params", r.name)
		}
	} else {
		paramValue = reflect.ValueOf(params)
		if !paramValue.Type().AssignableTo(paramType) {
			if paramValue.Type().ConvertibleTo(paramType) {
				paramValue = paramValue.Convert(paramType)
			} else {
				return nil, fmt.Errorf("quarry codex: recipe %q received %T, expected %s", r.name, params, paramType)
			}
		}
	}

	out := r.fn.Call([]reflect.Value{reflect.ValueOf(qq), paramValue})
	if len(out) != 1 {
		return nil, fmt.Errorf("quarry codex: recipe %q returned unexpected values", r.name)
	}
	sqler, ok := out[0].Interface().(quarry.SQLer)
	if !ok {
		return nil, fmt.Errorf("quarry codex: recipe %q returned unsupported type %T", r.name, out[0].Interface())
	}
	if sqler == nil {
		return nil, fmt.Errorf("quarry codex: recipe %q returned nil query", r.name)
	}
	return sqler, nil
}

func placeholderFor(d quarry.Dialect, n int) string {
	return d.Placeholder(n)
}

// structToMap extracts bind values from a struct using the documented tag precedence.
// The precedence is db, then json, then snake_case, which matches the scan package.
func structToMap(v any) (map[string]any, error) {
	if v == nil {
		return nil, fmt.Errorf("quarry codex: struct binding requires a value")
	}
	rv := reflect.ValueOf(v)
	// Follow pointer chains so callers can pass either a struct or a pointer to one.
	for rv.IsValid() && rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return nil, fmt.Errorf("quarry codex: struct binding requires a non-nil value")
		}
		rv = rv.Elem()
	}
	if !rv.IsValid() || rv.Kind() != reflect.Struct {
		return nil, fmt.Errorf("quarry codex: struct binding requires a struct")
	}
	out := make(map[string]any)
	collectStructValues(rv.Type(), rv, out)
	return out, nil
}

// collectStructValues walks exported fields and nested anonymous structs.
func collectStructValues(t reflect.Type, v reflect.Value, out map[string]any) {
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.PkgPath != "" && !field.Anonymous {
			continue
		}
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			collectStructValues(field.Type, v.Field(i), out)
			continue
		}
		name, ok := bindingName(field)
		if !ok {
			continue
		}
		out[name] = v.Field(i).Interface()
	}
}

// bindingName resolves a field's bind name using db, json, then snake_case.
func bindingName(field reflect.StructField) (string, bool) {
	if tag := field.Tag.Get("db"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return strings.ToLower(tag), true
	}
	if tag := field.Tag.Get("json"); tag != "" {
		if tag == "-" {
			return "", false
		}
		return strings.ToLower(strings.Split(tag, ",")[0]), true
	}
	return strings.ToLower(toSnakeCase(field.Name)), true
}

// copyStringMap detaches caller-owned maps from the bound query state.
func copyStringMap(in map[string]any) map[string]any {
	if len(in) == 0 {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func isNilable(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return true
	default:
		return false
	}
}

func supportedDialect(d quarry.Dialect) bool {
	switch d {
	case quarry.Postgres, quarry.MySQL, quarry.SQLite:
		return true
	default:
		return false
	}
}

func toSnakeCase(s string) string {
	var out strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			out.WriteByte('_')
		}
		out.WriteRune(r)
	}
	return strings.ToLower(out.String())
}
