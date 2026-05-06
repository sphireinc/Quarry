package quarry

import (
	"errors"
	"reflect"
	"strings"
	"testing"
)

type testStringer string

func (s testStringer) String() string { return string(s) }

func TestCoreSurfaceHelpers(t *testing.T) {
	if got := Dialect("postgres").Name(); got != "postgres" {
		t.Fatalf("unexpected dialect name: %s", got)
	}
	if got := Dialect("postgres").Supports(FeatureReturning); !got {
		t.Fatal("expected postgres to support RETURNING")
	}
	if got := Dialect("mysql").Supports(FeatureReturning); got {
		t.Fatal("expected mysql to reject RETURNING")
	}
	if got := Dialect("sqlite").Supports(FeatureILike); got {
		t.Fatal("expected sqlite to reject ILIKE")
	}
	if got := Dialect("oracle").Placeholder(3); got != "" {
		t.Fatalf("expected empty placeholder, got %q", got)
	}
	if _, err := Dialect("oracle").QuoteIdent("users"); err == nil || !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported dialect error, got %v", err)
	}

	if got, want := TableName("users"), T("users"); !reflect.DeepEqual(got, want) {
		t.Fatalf("table alias mismatch: %#v vs %#v", got, want)
	}
	if got, want := Col("id"), C("id"); !reflect.DeepEqual(got, want) {
		t.Fatalf("column alias mismatch: %#v vs %#v", got, want)
	}

	if err := validateIdentifier(""); !errors.Is(err, ErrInvalidIdentifier) {
		t.Fatalf("expected invalid identifier error, got %v", err)
	}
	if err := validateIdentifier("1bad"); !errors.Is(err, ErrInvalidIdentifier) {
		t.Fatalf("expected invalid identifier error, got %v", err)
	}
	if err := validateIdentifier("bad name"); !errors.Is(err, ErrInvalidIdentifier) {
		t.Fatalf("expected invalid identifier error, got %v", err)
	}

	if key, ok := columnMapKey((*Column)(nil)); ok || key != "" {
		t.Fatalf("expected nil column to be ignored, got %q %v", key, ok)
	}
	if key, ok := columnMapKey(""); ok || key != "" {
		t.Fatalf("unexpected blank string key: %q %v", key, ok)
	}
	if key, ok := columnMapKey(Column{name: "email"}); !ok || key != "email" {
		t.Fatalf("unexpected column key: %q %v", key, ok)
	}
	if key, ok := columnMapKey(Table{name: "users"}); !ok || key != "users" {
		t.Fatalf("unexpected table key: %q %v", key, ok)
	}
	if key, ok := columnMapKey(&Column{name: "email"}); !ok || key != "email" {
		t.Fatalf("unexpected column key: %q %v", key, ok)
	}
	if key, ok := columnMapKey(&Table{name: "users"}); !ok || key != "users" {
		t.Fatalf("unexpected table key: %q %v", key, ok)
	}
	if key, ok := columnMapKey((*Table)(nil)); ok || key != "" {
		t.Fatalf("expected nil table to be ignored, got %q %v", key, ok)
	}

	if err := requireTableValue(nil, "insert"); err == nil {
		t.Fatal("expected nil table error")
	}
	if err := requireTableValue("   ", "insert"); err == nil {
		t.Fatal("expected blank table error")
	}

	var nilQuarry *Quarry
	if got := nilQuarry.Dialect(); got != "" {
		t.Fatalf("expected zero dialect from nil quarry, got %q", got)
	}
	if err := nilQuarry.errOrNil(); err == nil || !strings.Contains(err.Error(), "nil quarry") {
		t.Fatalf("expected nil quarry error, got %v", err)
	}
	if err := (&Quarry{}).errOrNil(); err == nil || !errors.Is(err, ErrInvalidBuilderState) {
		t.Fatalf("expected invalid builder state, got %v", err)
	}
}

func TestOptionalPredicatesAndOrderingHelpers(t *testing.T) {
	qq := New(Postgres)
	alias := "bob"
	aliasPtr := &alias
	aliasPtrPtr := &aliasPtr

	sqlText, args, err := qq.Select("*").
		From("users").
		WhereIf(false, Eq("ignored", 1)).
		WhereIf(true, Eq("tenant_id", 99)).
		WhereIf(true, nil).
		WhereIf(true, OptionalEq("ignored", "")).
		Where(
			OptionalNeq("status", "inactive"),
			OptionalGt("age", 18),
			OptionalGte("score", 90),
			OptionalLt("rank", 10),
			OptionalLte("level", uint(7)),
			OptionalLike("name", "bob%"),
			OptionalILike("email", "bob%"),
			OptionalEq("nickname", aliasPtrPtr),
		).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL := "SELECT * FROM users WHERE tenant_id = $1 AND status <> $2 AND age > $3 AND score >= $4 AND rank < $5 AND level <= $6 AND name LIKE $7 AND email ILIKE $8 AND nickname = $9"
	if sqlText != wantSQL {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sqlText)
	}
	if wantArgs := []any{99, "inactive", 18, 90, 10, uint(7), "bob%", "bob%", alias}; !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", wantArgs, args)
	}

	tags := []string{"news", "sports"}
	sqlText, args, err = qq.Select("*").
		From("users").
		Where(
			OptionalIn("id", 1, 2, 3),
			OptionalIn("skip", []int{}),
			OptionalIn("tags", &tags),
			OptionalIn("ignored", (*[]int)(nil)),
		).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL = "SELECT * FROM users WHERE id IN ($1, $2, $3) AND tags IN ($4, $5)"
	if sqlText != wantSQL {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sqlText)
	}
	if wantArgs := []any{1, 2, 3, "news", "sports"}; !reflect.DeepEqual(args, wantArgs) {
		t.Fatalf("args mismatch\nwant: %#v\ngot:  %#v", wantArgs, args)
	}

	sqlText, args, err = qq.Select("*").
		From("users").
		OrderBySafe("missing", SortMap{"newest": "created_at DESC"}).
		OrderBySafeDefault("missing", SortMap{"newest": "created_at DESC", "email": "email ASC"}, "newest").
		LimitDefault(-1, 25).
		OffsetDefault(-1, 10).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL = "SELECT * FROM users ORDER BY created_at DESC LIMIT 25 OFFSET 10"
	if sqlText != wantSQL {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sqlText)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %#v", args)
	}

	sqlText, args, err = qq.Select("*").
		From("users").
		OrderBySafe("email", SortMap{"email": "email ASC"}).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wantSQL = "SELECT * FROM users ORDER BY email ASC"; sqlText != wantSQL {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sqlText)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %#v", args)
	}

	sqlText, args, err = qq.Select("*").
		From("users").
		OrderBySafeDefault("email", SortMap{"newest": "created_at DESC", "email": "email ASC"}, "newest").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if wantSQL = "SELECT * FROM users ORDER BY email ASC"; sqlText != wantSQL {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sqlText)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %#v", args)
	}

	sqlText, args, err = qq.Select("*").
		From("users").
		OrderBySafe("email", SortMap{"email": "email ASC"}).
		LimitDefault(7, 25).
		OffsetDefault(3, 10).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wantSQL = "SELECT * FROM users ORDER BY email ASC LIMIT 7 OFFSET 3"
	if sqlText != wantSQL {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", wantSQL, sqlText)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %#v", args)
	}

	sqlText, args, err = qq.InsertInto("users").
		Columns("email", "status").
		SetMap(map[string]any{"status": "active", "email": "a@example.com"}).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "INSERT INTO users (email, status) VALUES ($1, $2)"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if !reflect.DeepEqual(args, []any{"a@example.com", "active"}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestWhereIfOnUpdateAndDelete(t *testing.T) {
	qq := New(Postgres)

	sqlText, args, err := qq.Update("users").
		WhereIf(false, Eq("ignored", 1)).
		WhereIf(true, Eq("id", 7)).
		Set("name", "bob").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "UPDATE users SET name = $1 WHERE id = $2"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if !reflect.DeepEqual(args, []any{"bob", 7}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sqlText, args, err = qq.DeleteFrom("users").
		WhereIf(false, Eq("ignored", 1)).
		WhereIf(true, Eq("id", 7)).
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "DELETE FROM users WHERE id = $1"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if !reflect.DeepEqual(args, []any{7}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestBuilderPrefixesSuffixesAndBlankInputs(t *testing.T) {
	qq := New(Postgres)

	sqlText, args, err := qq.Select("*").
		Prefix("/* pre */ ").
		From("users").
		OrderBy("created_at DESC", "   ").
		Suffix("/* post */").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "/* pre */ SELECT * FROM users ORDER BY created_at DESC /* post */"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if len(args) != 0 {
		t.Fatalf("expected no args, got %#v", args)
	}

	sqlText, args, err = qq.InsertInto("users").
		Prefix("/* pre */ ").
		Columns("email", "   ").
		Values("a@example.com").
		Suffix("/* post */").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "/* pre */ INSERT INTO users (email) VALUES ($1) /* post */"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if !reflect.DeepEqual(args, []any{"a@example.com"}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sqlText, args, err = qq.Update("users").
		Prefix("/* pre */ ").
		Set("name", "bob").
		Set("   ", "ignored").
		Where(Eq("id", 1)).
		Suffix("/* post */").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "/* pre */ UPDATE users SET name = $1 WHERE id = $2 /* post */"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if !reflect.DeepEqual(args, []any{"bob", 1}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	sqlText, args, err = qq.DeleteFrom("users").
		Prefix("/* pre */ ").
		Where(Eq("id", 1)).
		Suffix("/* post */").
		ToSQL()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if want := "/* pre */ DELETE FROM users WHERE id = $1 /* post */"; sqlText != want {
		t.Fatalf("sql mismatch\nwant: %s\ngot:  %s", want, sqlText)
	}
	if !reflect.DeepEqual(args, []any{1}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestCoreInternals(t *testing.T) {
	sb := newSQLBuilder(Postgres)
	sb.writeByte('x')
	if sb.String() != "x" {
		t.Fatalf("unexpected buffer: %q", sb.String())
	}

	sb = newSQLBuilder(Postgres)
	if err := appendExpr(sb, "RAW"); err != nil {
		t.Fatalf("append string: %v", err)
	}
	if sb.String() != "RAW" {
		t.Fatalf("unexpected string output: %q", sb.String())
	}

	sb = newSQLBuilder(Postgres)
	if err := appendExpr(sb, testStringer("raw")); err != nil {
		t.Fatalf("append stringer: %v", err)
	}
	if sb.String() != "raw" {
		t.Fatalf("unexpected stringer output: %q", sb.String())
	}

	sb = newSQLBuilder(Postgres)
	if err := appendExpr(sb, T("users")); err != nil {
		t.Fatalf("append table: %v", err)
	}
	if err := appendExpr(sb, &Table{name: "profiles", alias: "p"}); err != nil {
		t.Fatalf("append table pointer: %v", err)
	}
	if err := appendExpr(sb, C("id")); err != nil {
		t.Fatalf("append column: %v", err)
	}
	if err := appendExpr(sb, &Column{name: "email"}); err != nil {
		t.Fatalf("append column pointer: %v", err)
	}
	if err := appendExpr(sb, nil); err == nil {
		t.Fatal("expected nil expression error")
	}
	if err := appendExpr(sb, struct{}{}); err == nil {
		t.Fatal("expected unsupported expression error")
	}
	var nilStringer *testStringer
	if err := appendExpr(sb, nilStringer); err == nil {
		t.Fatal("expected nil stringer error")
	}

	sb = newSQLBuilder(Postgres)
	if err := renderPredicate(sb, Not(And(Eq("id", 1), Or(Eq("status", "active"), nil))), false); err != nil {
		t.Fatalf("render predicate: %v", err)
	}
	if got := sb.String(); got != "NOT ((id = $1 AND status = $2))" {
		t.Fatalf("unexpected not rendering: %q", got)
	}

	if err := newSQLBuilder(Dialect("oracle")).appendRaw("a = ?", 1); err == nil || !errors.Is(err, ErrUnsupportedFeature) {
		t.Fatalf("expected unsupported feature error, got %v", err)
	}

	if out, empty, err := normalizeINValues([]any{1, 2}); err != nil || empty || !reflect.DeepEqual(out, []any{1, 2}) {
		t.Fatalf("unexpected normalize output: %#v %v %v", out, empty, err)
	}
	if out, empty, err := normalizeINValues([]any{1}); err != nil || empty || !reflect.DeepEqual(out, []any{1}) {
		t.Fatalf("unexpected scalar normalize output: %#v %v %v", out, empty, err)
	}
	if out, empty, err := normalizeINValues([]any{&[]int{1, 2}}); err != nil || empty || !reflect.DeepEqual(out, []any{1, 2}) {
		t.Fatalf("unexpected pointer-slice normalize output: %#v %v %v", out, empty, err)
	}
	if out, empty, err := normalizeINValues(nil); err != nil || !empty || out != nil {
		t.Fatalf("unexpected empty normalize output: %#v %v %v", out, empty, err)
	}
	if out, ok, err := normalizeSingleINValue(1); err != nil || ok || out != nil {
		t.Fatalf("unexpected scalar single normalize output: %#v %v %v", out, ok, err)
	}
	if out, ok, err := normalizeSingleINValue(&[]int{3, 4}); err != nil || !ok || !reflect.DeepEqual(out, []any{3, 4}) {
		t.Fatalf("unexpected single normalize output: %#v %v %v", out, ok, err)
	}

	var nilQuery Query
	if !isNilQuery(nilQuery) {
		t.Fatal("expected nil query to be detected")
	}
	if !isNilValue((*testStringer)(nil)) {
		t.Fatal("expected typed nil value to be detected")
	}

	sub := New(Postgres).Select("1").From("users").Where(Eq("id", 1))
	sb = newSQLBuilder(Postgres)
	sb.write("EXISTS (")
	if err := appendSubquery(sb, sub); err != nil {
		t.Fatalf("append subquery: %v", err)
	}
	sb.write(")")
	if got := sb.String(); got != "EXISTS (SELECT 1 FROM users WHERE id = $1)" {
		t.Fatalf("unexpected subquery output: %q", got)
	}
}

func TestOptionalHelperInternals(t *testing.T) {
	for _, tc := range []struct {
		name string
		pred Predicate
	}{
		{name: "neq_empty", pred: OptionalNeq("status", "")},
		{name: "gt_empty", pred: OptionalGt("age", nil)},
		{name: "gte_empty", pred: OptionalGte("score", (*int)(nil))},
		{name: "lt_empty", pred: OptionalLt("rank", "")},
		{name: "lte_empty", pred: OptionalLte("level", []int{})},
		{name: "like_empty", pred: OptionalLike("name", "")},
		{name: "ilike_empty", pred: OptionalILike("email", (*string)(nil))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.pred != nil && !tc.pred.empty() {
				t.Fatalf("expected empty predicate, got %#v", tc.pred)
			}
		})
	}

	if _, ok := optionalBindValue(""); ok {
		t.Fatal("expected empty string to be omitted")
	}
	if _, ok := optionalBindValue((*string)(nil)); ok {
		t.Fatal("expected nil pointer to be omitted")
	}
	if _, ok := optionalBindValue([]int{}); ok {
		t.Fatal("expected empty slice to be omitted")
	}
	if _, ok := optionalBindValue([0]int{}); ok {
		t.Fatal("expected empty array to be omitted")
	}
	str := "hello"
	if got, ok := optionalBindValue(&[]int{1, 2}); !ok || !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatalf("unexpected slice value: %#v %v", got, ok)
	}
	if got, ok := optionalBindValue(&str); !ok || got != "hello" {
		t.Fatalf("unexpected string pointer value: %#v %v", got, ok)
	}

	sb := newSQLBuilder(Postgres)
	if err := (optionalPredicate{}).appendSQL(sb); err != nil {
		t.Fatalf("append empty optional predicate: %v", err)
	}
	if sb.String() != "" {
		t.Fatalf("expected empty optional predicate to emit nothing, got %q", sb.String())
	}

	sb = newSQLBuilder(Postgres)
	if err := (optionalPredicate{pred: Raw("id = ?", 1)}).appendSQL(sb); err != nil {
		t.Fatalf("append optional predicate: %v", err)
	}
	if got := sb.String(); got != "id = $1" {
		t.Fatalf("unexpected optional predicate output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := renderPredicate(sb, nil, false); err != nil {
		t.Fatalf("render nil predicate: %v", err)
	}
	if sb.String() != "" {
		t.Fatalf("expected nil predicate to emit nothing, got %q", sb.String())
	}

	sb = newSQLBuilder(Postgres)
	if err := renderPredicate(sb, rawPredicate{sql: "flag = ?", args: []any{true}}, false); err != nil {
		t.Fatalf("render raw predicate: %v", err)
	}
	if got := sb.String(); got != "flag = $1" {
		t.Fatalf("unexpected raw predicate output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := renderPredicate(sb, &groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1), Eq("status", "active")}}, false); err != nil {
		t.Fatalf("render pointer group: %v", err)
	}
	if got := sb.String(); got != "id = $1 AND status = $2" {
		t.Fatalf("unexpected pointer group output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := renderPredicate(sb, &notPredicate{pred: Eq("id", 1)}, false); err != nil {
		t.Fatalf("render pointer not: %v", err)
	}
	if got := sb.String(); got != "NOT (id = $1)" {
		t.Fatalf("unexpected pointer not output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := renderGroupPredicate(sb, groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1)}}, true); err != nil {
		t.Fatalf("render nested single group: %v", err)
	}
	if got := sb.String(); got != "id = $1" {
		t.Fatalf("unexpected nested single output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := renderGroupPredicate(sb, groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1), Or(Eq("status", "active"), Eq("status", "pending"))}}, true); err != nil {
		t.Fatalf("render nested group: %v", err)
	}
	if got := sb.String(); got != "(id = $1 AND (status = $2 OR status = $3))" {
		t.Fatalf("unexpected nested group output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := (groupPredicate{op: "AND"}).appendSQL(sb); err != nil {
		t.Fatalf("append empty group: %v", err)
	}
	if sb.String() != "" {
		t.Fatalf("expected empty group to emit nothing, got %q", sb.String())
	}

	sb = newSQLBuilder(Postgres)
	if err := (groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1)}}).appendSQL(sb); err != nil {
		t.Fatalf("append single-child group: %v", err)
	}
	if got := sb.String(); got != "id = $1" {
		t.Fatalf("unexpected single-child group output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := (groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1), Eq("status", "active")}}).appendSQL(sb); err != nil {
		t.Fatalf("append group: %v", err)
	}
	if got := sb.String(); got != "id = $1 AND status = $2" {
		t.Fatalf("unexpected group output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := (notPredicate{pred: Eq("id", 1)}).appendSQL(sb); err != nil {
		t.Fatalf("append not: %v", err)
	}
	if got := sb.String(); got != "NOT (id = $1)" {
		t.Fatalf("unexpected not output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := (comparisonPredicate{left: "age", op: ">", value: nil}).appendSQL(sb); err == nil || !strings.Contains(err.Error(), "nil value not allowed") {
		t.Fatalf("expected nil comparison error, got %v", err)
	}

	sb = newSQLBuilder(Postgres)
	if err := (groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1), Eq("status", "active")}}).appendSQL(sb); err != nil {
		t.Fatalf("append group: %v", err)
	}
	if got := sb.String(); got != "id = $1 AND status = $2" {
		t.Fatalf("unexpected group output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := renderPredicate(sb, groupPredicate{op: "AND", preds: []Predicate{Eq("id", 1)}}, true); err != nil {
		t.Fatalf("render single-child group: %v", err)
	}
	if got := sb.String(); got != "id = $1" {
		t.Fatalf("unexpected single-child output: %q", got)
	}

	sb = newSQLBuilder(Postgres)
	if err := (notPredicate{pred: Eq("id", 1)}).appendSQL(sb); err != nil {
		t.Fatalf("append not: %v", err)
	}
	if got := sb.String(); got != "NOT (id = $1)" {
		t.Fatalf("unexpected not output: %q", got)
	}
}
