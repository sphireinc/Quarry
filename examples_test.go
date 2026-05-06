package quarry_test

import (
	"fmt"

	quarry "github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/codex"
)

func ExampleQuarry_Select() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.Select("id", "email").
		From("users").
		Where(quarry.Eq("status", "active")).
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// SELECT id, email FROM users WHERE status = $1
	// [active]
}

func ExampleSelectBuilder() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.Select("id", "email", "created_at").
		From("users").
		Where(
			quarry.Eq("tenant_id", 42),
			quarry.OptionalILike("email", "%bob%"),
			quarry.OptionalEq("status", (*string)(nil)),
		).
		OrderBySafeDefault("newest", quarry.SortMap{
			"newest": "created_at DESC",
			"email":  "email ASC",
		}, "newest").
		Page(1, 25).
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// SELECT id, email, created_at FROM users WHERE tenant_id = $1 AND email ILIKE $2 ORDER BY created_at DESC LIMIT 25 OFFSET 0
	// [42 %bob%]
}

func ExampleSelectBuilder_OrderBySafeDefault() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.Select("id", "email").
		From("users").
		OrderBySafeDefault("newest", quarry.SortMap{
			"newest": "created_at DESC",
			"email":  "email ASC",
		}, "newest").
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// SELECT id, email FROM users ORDER BY created_at DESC
	// []
}

func ExampleUpdateBuilder_SetOptional() {
	qq := quarry.New(quarry.Postgres)
	enabled := true

	sqlText, args, err := qq.Update("users").
		SetOptional("name", "Quarry User").
		SetOptional("email", "user@example.com").
		SetIf(enabled, "enabled", enabled).
		Where(quarry.Eq("id", 7)).
		Returning("id").
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// UPDATE users SET name = $1, email = $2, enabled = $3 WHERE id = $4 RETURNING id
	// [Quarry User user@example.com true 7]
}

func ExampleInsertBuilder_Returning() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.InsertInto("users").
		Columns("email", "status").
		Values("a@example.com", "active").
		Returning("id").
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// INSERT INTO users (email, status) VALUES ($1, $2) RETURNING id
	// [a@example.com active]
}

func ExampleDeleteBuilder_Returning() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.DeleteFrom("users").
		Where(quarry.Eq("id", 7)).
		Returning("id").
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// DELETE FROM users WHERE id = $1 RETURNING id
	// [7]
}

func ExampleRaw() {
	qq := quarry.New(quarry.Postgres)

	sqlText, args, err := qq.Select(quarry.Raw("COUNT(*) FILTER (WHERE status = ?)", "active")).
		From("users").
		Where(quarry.Raw("created_at >= ?", "2024-01-01")).
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
	// Output:
	// SELECT COUNT(*) FILTER (WHERE status = $1) FROM users WHERE created_at >= $2
	// [active 2024-01-01]
}

func ExampleCodex() {
	type searchParams struct {
		Search string
	}

	cx := codex.New()
	if err := cx.AddRawNamed("users.by_id", `SELECT id, email FROM users WHERE id = :id`); err != nil {
		panic(err)
	}
	if err := cx.AddRecipe("users.search", codex.NewRecipe(func(qq *quarry.Quarry, p searchParams) quarry.SQLer {
		return qq.Select("id", "email").
			From("users").
			Where(quarry.OptionalILike("email", p.Search))
	})); err != nil {
		panic(err)
	}

	qq := quarry.New(quarry.Postgres)
	rawSQL, rawArgs, err := cx.MustRaw("users.by_id").With(qq).BindMap(map[string]any{"id": 10}).ToSQL()
	if err != nil {
		panic(err)
	}
	recipeSQL, err := cx.MustRecipe("users.search").Build(qq, searchParams{Search: "%bob%"})
	if err != nil {
		panic(err)
	}
	builtSQL, builtArgs, err := recipeSQL.ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(rawSQL)
	fmt.Println(rawArgs)
	fmt.Println(builtSQL)
	fmt.Println(builtArgs)
	// Output:
	// SELECT id, email FROM users WHERE id = $1
	// [10]
	// SELECT id, email FROM users WHERE email ILIKE $1
	// [%bob%]
}
