package codex

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sphireinc/quarry"
)

type recipeParams struct {
	ID    int    `db:"id"`
	Name  string `json:"name"`
	Extra string `db:"-"`
}

type embeddedBinding struct {
	TeamID int `db:"team_id"`
}

type bindingParams struct {
	embeddedBinding
	Slug  string `json:"slug,omitempty"`
	Plain string
	Extra string `db:"-"`
}

func TestCodexAliasesAndNilSafety(t *testing.T) {
	var nilCodex *Codex
	if err := nilCodex.Add("users.by_id", RawQuery{name: "users.by_id"}); err == nil || !strings.Contains(err.Error(), "nil codex") {
		t.Fatalf("expected nil codex error, got %v", err)
	}
	if _, ok := nilCodex.Get("users.by_id"); ok {
		t.Fatal("expected nil codex lookup to fail")
	}

	cx := New()
	if err := cx.Raw("users.by_id", "SELECT id FROM users WHERE id = ?"); err != nil {
		t.Fatalf("raw alias: %v", err)
	}
	if err := cx.RawNamed("users.by_name", "SELECT id FROM users WHERE name = :name"); err != nil {
		t.Fatalf("raw named alias: %v", err)
	}
	if err := cx.Recipe("users.search", NewRecipe(func(qq *quarry.Quarry, p recipeParams) quarry.SQLer {
		return qq.Select("id").From("users").Where(quarry.Eq("id", p.ID))
	})); err != nil {
		t.Fatalf("recipe alias: %v", err)
	}
	if err := cx.AddRecipe("users.nil", nil); err == nil || !strings.Contains(err.Error(), "is nil") {
		t.Fatalf("expected nil recipe error, got %v", err)
	}
	if err := cx.AddRecipe("users.unsupported", 123); err == nil || !strings.Contains(err.Error(), "unsupported recipe type") {
		t.Fatalf("expected unsupported recipe type error, got %v", err)
	}
	if err := cx.Add("users.mismatch", RawQuery{name: "users.other", sql: "SELECT 1"}); err == nil || !strings.Contains(err.Error(), "name mismatch") {
		t.Fatalf("expected name mismatch error, got %v", err)
	}
	if err := cx.Add("users.nil_query", nil); err == nil || !strings.Contains(err.Error(), "is nil") {
		t.Fatalf("expected nil query error, got %v", err)
	}

	if raw := cx.Must("users.by_id"); raw.Name() != "users.by_id" {
		t.Fatalf("unexpected raw name: %s", raw.Name())
	}
	if raw := cx.MustRaw("users.by_id"); raw.Name() != "users.by_id" {
		t.Fatalf("unexpected raw name: %s", raw.Name())
	}
	if recipe := cx.MustRecipe("users.search"); recipe.Name() != "users.search" {
		t.Fatalf("unexpected recipe name: %s", recipe.Name())
	}
}

func TestStoreValidationAndCopies(t *testing.T) {
	var nilStore *Store
	if nilStore.SetStrict(true) != nil {
		t.Fatal("expected nil store SetStrict to return nil")
	}
	if err := nilStore.Add("users.by_id", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "nil store") {
		t.Fatalf("expected nil store error, got %v", err)
	}
	if _, ok := nilStore.Get("users.by_id"); ok {
		t.Fatal("expected nil store lookup to fail")
	}
	if names := nilStore.Names(); names != nil {
		t.Fatalf("expected nil store names to be nil, got %#v", names)
	}

	var store Store
	if err := store.Add("users.by_id", "SELECT id FROM users WHERE id = :id"); err != nil {
		t.Fatalf("zero-value add: %v", err)
	}
	if err := store.Add("users.by_id", "SELECT id FROM users WHERE id = :id"); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("expected duplicate error, got %v", err)
	}
	if err := store.Add("bad name!", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "invalid query name") {
		t.Fatalf("expected invalid name error, got %v", err)
	}
	if err := store.Add("", "SELECT 1"); err == nil || !strings.Contains(err.Error(), "query name is required") {
		t.Fatalf("expected missing name error, got %v", err)
	}
	if err := store.Add("users.empty", "   "); err == nil || !strings.Contains(err.Error(), "is empty") {
		t.Fatalf("expected empty sql error, got %v", err)
	}
	if names := store.Names(); !reflect.DeepEqual(names, []string{"users.by_id"}) {
		t.Fatalf("unexpected names: %#v", names)
	}

	src := map[string]any{"id": 10}
	raw := RawQuery{name: "users.by_id", sql: "SELECT id FROM users WHERE id = :id", named: true}
	bound := raw.With(quarry.New(quarry.Postgres)).BindMap(src)
	src["id"] = 99
	sqlText, args, err := bound.ToSQL()
	if err != nil {
		t.Fatalf("to sql: %v", err)
	}
	if sqlText != "SELECT id FROM users WHERE id = $1" {
		t.Fatalf("sql mismatch: %s", sqlText)
	}
	if !reflect.DeepEqual(args, []any{10}) {
		t.Fatalf("args mismatch: %#v", args)
	}
}

func TestMustHelpersPanic(t *testing.T) {
	t.Run("must_add", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		NewStore().MustAdd("", "SELECT 1")
	})

	t.Run("must_raw_missing", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		New().MustRaw("missing")
	})

	t.Run("must_raw_wrong_type", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		cx := New()
		if err := cx.AddRecipe("users.search", NewRecipe(func(qq *quarry.Quarry, p recipeParams) quarry.SQLer {
			return qq.Select("id").From("users")
		})); err != nil {
			t.Fatalf("add recipe: %v", err)
		}
		cx.MustRaw("users.search")
	})

	t.Run("must_recipe_missing", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		New().MustRecipe("missing")
	})

	t.Run("must_recipe_wrong_type", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected panic")
			}
		}()
		cx := New()
		if err := cx.Raw("users.by_id", "SELECT 1"); err != nil {
			t.Fatalf("raw alias: %v", err)
		}
		cx.MustRecipe("users.by_id")
	})
}

func TestBoundRawErrorsAndHelpers(t *testing.T) {
	var nilBound *BoundRaw
	if got := nilBound.Name(); got != "" {
		t.Fatalf("unexpected nil bound name: %q", got)
	}
	if nilBound.Bind(1) != nil {
		t.Fatal("expected nil bound bind to return nil")
	}
	if nilBound.BindMap(map[string]any{"id": 1}) != nil {
		t.Fatal("expected nil bound bindmap to return nil")
	}
	if nilBound.BindStruct(struct{ ID int }{ID: 1}) != nil {
		t.Fatal("expected nil bound bindstruct to return nil")
	}

	raw := RawQuery{name: "users.by_id", sql: "SELECT id FROM users WHERE id = ?"}
	if _, _, err := raw.With(nil).ToSQL(); err == nil || !strings.Contains(err.Error(), "nil quarry") {
		t.Fatalf("expected nil quarry error, got %v", err)
	}
	if _, _, err := raw.With(quarry.New(quarry.Dialect("oracle"))).Bind(1).ToSQL(); err == nil || !strings.Contains(err.Error(), "unsupported dialect") {
		t.Fatalf("expected unsupported dialect error, got %v", err)
	}
	if _, _, err := raw.With(quarry.New(quarry.Postgres)).ToSQL(); err == nil || !strings.Contains(err.Error(), "requires positional bindings") {
		t.Fatalf("expected positional binding error, got %v", err)
	}

	named := RawQuery{name: "users.by_name", sql: "SELECT id FROM users WHERE name = :name", named: true}
	if _, _, err := named.With(quarry.New(quarry.Postgres)).ToSQL(); err == nil || !strings.Contains(err.Error(), "requires named bindings") {
		t.Fatalf("expected named binding error, got %v", err)
	}
}

func TestRecipeHelpers(t *testing.T) {
	recipe := NewRecipe(func(qq *quarry.Quarry, p recipeParams) quarry.SQLer {
		return qq.Select("id").From("users").Where(quarry.Eq("id", p.ID))
	})

	if _, err := (Recipe[recipeParams]{}).Build(quarry.New(quarry.Postgres), recipeParams{ID: 1}); err == nil || !strings.Contains(err.Error(), "recipe is nil") {
		t.Fatalf("expected nil recipe error, got %v", err)
	}
	if _, err := recipe.Build(nil, recipeParams{ID: 1}); err == nil || !strings.Contains(err.Error(), "nil quarry") {
		t.Fatalf("expected nil quarry error, got %v", err)
	}

	nilPtrRecipe := NewRecipe(func(qq *quarry.Quarry, p *int) quarry.SQLer {
		return qq.Select("id").From("users")
	})
	if sqler, err := nilPtrRecipe.Build(quarry.New(quarry.Postgres), nil); err != nil || sqler == nil {
		t.Fatalf("expected nil-param recipe to succeed, got %v %#v", err, sqler)
	}

	sqler, err := recipe.Build(quarry.New(quarry.Postgres), recipeParams{ID: 10})
	if err != nil {
		t.Fatalf("recipe build: %v", err)
	}
	sqlText, args, err := sqler.ToSQL()
	if err != nil {
		t.Fatalf("recipe to sql: %v", err)
	}
	if sqlText != "SELECT id FROM users WHERE id = $1" {
		t.Fatalf("sql mismatch: %s", sqlText)
	}
	if !reflect.DeepEqual(args, []any{10}) {
		t.Fatalf("args mismatch: %#v", args)
	}

	named := recipe.WithName("users.search")
	if named.Name() != "users.search" {
		t.Fatalf("unexpected named recipe: %s", named.Name())
	}
	if _, err := named.Build(quarry.New(quarry.Postgres), "wrong"); err == nil || !strings.Contains(err.Error(), "received string") {
		t.Fatalf("expected type mismatch error, got %v", err)
	}

	cx := New()
	if err := cx.AddRecipe("users.search", named); err != nil {
		t.Fatalf("add recipe: %v", err)
	}
	if _, err := cx.MustRecipe("users.search").Build(quarry.New(quarry.Postgres), recipeParams{ID: 1}); err != nil {
		t.Fatalf("named recipe build: %v", err)
	}

	var nilFunc Recipe[recipeParams]
	if _, err := nilFunc.Build(quarry.New(quarry.Postgres), recipeParams{}); err == nil || !strings.Contains(err.Error(), "recipe is nil") {
		t.Fatalf("expected nil recipe error, got %v", err)
	}
}

func TestReflectedRecipeAndStructBindingHelpers(t *testing.T) {
	cx := New()
	if err := cx.AddRecipe("users.runtime", func(qq *quarry.Quarry, p recipeParams) quarry.SQLer {
		return qq.Select("id").From("users").Where(quarry.Eq("id", p.ID))
	}); err != nil {
		t.Fatalf("add recipe: %v", err)
	}
	if err := cx.AddRecipe("users.bad_sig", func() {}); err != nil {
		t.Fatalf("add bad signature: %v", err)
	}

	if _, err := cx.MustRecipe("users.bad_sig").Build(quarry.New(quarry.Postgres), recipeParams{}); err == nil || !strings.Contains(err.Error(), "unsupported function signature") {
		t.Fatalf("expected unsupported signature error, got %v", err)
	}
	if _, err := (reflectedRecipe{name: "users.runtime", fn: reflect.ValueOf(func(qq *quarry.Quarry, p recipeParams) quarry.SQLer {
		return nil
	})}).Build(quarry.New(quarry.Postgres), recipeParams{}); err == nil || !strings.Contains(err.Error(), "unsupported type <nil>") {
		t.Fatalf("expected nil query error, got %v", err)
	}
	if _, err := (reflectedRecipe{name: "users.runtime", fn: reflect.ValueOf(func(qq *quarry.Quarry, p recipeParams) quarry.SQLer {
		return qq.Select("id").From("users")
	})}).Build(nil, recipeParams{}); err == nil || !strings.Contains(err.Error(), "nil quarry") {
		t.Fatalf("expected nil quarry error, got %v", err)
	}
	if sqler, err := (reflectedRecipe{name: "users.nilptr", fn: reflect.ValueOf(func(qq *quarry.Quarry, p *int) quarry.SQLer {
		return qq.Select("id").From("users")
	})}).Build(quarry.New(quarry.Postgres), nil); err != nil || sqler == nil {
		t.Fatalf("expected nil-param recipe to succeed, got %v %#v", err, sqler)
	}

	var nilRecipe reflectedRecipe
	if _, err := nilRecipe.Build(quarry.New(quarry.Postgres), recipeParams{}); err == nil || !strings.Contains(err.Error(), "is nil") {
		t.Fatalf("expected nil reflected recipe error, got %v", err)
	}

	bp := &bindingParams{
		embeddedBinding: embeddedBinding{TeamID: 44},
		Slug:            "hello",
		Plain:           "plain",
		Extra:           "ignored",
	}
	values, err := structToMap(&bp)
	if err != nil {
		t.Fatalf("struct to map: %v", err)
	}
	if !reflect.DeepEqual(values, map[string]any{
		"team_id": 44,
		"slug":    "hello",
		"plain":   "plain",
	}) {
		t.Fatalf("unexpected struct values: %#v", values)
	}

	if _, err := structToMap(nil); err == nil || !strings.Contains(err.Error(), "requires a value") {
		t.Fatalf("expected nil struct error, got %v", err)
	}
	if _, err := structToMap((*bindingParams)(nil)); err == nil || !strings.Contains(err.Error(), "non-nil value") {
		t.Fatalf("expected nil pointer error, got %v", err)
	}
	if _, err := structToMap(10); err == nil || !strings.Contains(err.Error(), "requires a struct") {
		t.Fatalf("expected struct error, got %v", err)
	}

	out := make(map[string]any)
	collectStructValues(reflect.TypeOf(bindingParams{}), reflect.ValueOf(bindingParams{
		embeddedBinding: embeddedBinding{TeamID: 7},
		Slug:            "slug",
		Plain:           "plain",
	}), out)
	if !reflect.DeepEqual(out, map[string]any{
		"team_id": 7,
		"slug":    "slug",
		"plain":   "plain",
	}) {
		t.Fatalf("unexpected collected values: %#v", out)
	}

	field, _ := reflect.TypeOf(bindingParams{}).FieldByName("Plain")
	if name, ok := bindingName(field); !ok || name != "plain" {
		t.Fatalf("unexpected binding name: %q %v", name, ok)
	}
	field, _ = reflect.TypeOf(bindingParams{}).FieldByName("Slug")
	if name, ok := bindingName(field); !ok || name != "slug" {
		t.Fatalf("unexpected binding name: %q %v", name, ok)
	}
	field, _ = reflect.TypeOf(bindingParams{}).FieldByName("Extra")
	if name, ok := bindingName(field); ok || name != "" {
		t.Fatalf("expected ignored field, got %q %v", name, ok)
	}

	if !isNilable(reflect.TypeOf((*string)(nil))) {
		t.Fatal("expected pointer to be nilable")
	}
	if isNilable(reflect.TypeOf(10)) {
		t.Fatal("expected int to be non-nilable")
	}
	if toSnakeCase("HTTPRequestID") != "h_t_t_p_request_i_d" {
		t.Fatalf("unexpected snake case conversion: %s", toSnakeCase("HTTPRequestID"))
	}
	if toSnakeCase("UserID") != "user_i_d" {
		t.Fatalf("unexpected snake case conversion: %s", toSnakeCase("UserID"))
	}

	if out := copyStringMap(nil); len(out) != 0 {
		t.Fatalf("expected empty copy, got %#v", out)
	}
}
