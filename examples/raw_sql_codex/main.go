// Package main prints Quarry raw SQL and Codex binding examples.
package main

import (
	"fmt"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/codex"
)

func main() {
	cx := codex.New()
	if err := cx.AddRawNamed("users.by_id", `SELECT id, email FROM users WHERE id = :id`); err != nil {
		panic(err)
	}

	qq := quarry.New(quarry.Postgres)
	sqlText, args, err := cx.MustRaw("users.by_id").With(qq).BindMap(map[string]any{
		"id": 10,
	}).ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
}
