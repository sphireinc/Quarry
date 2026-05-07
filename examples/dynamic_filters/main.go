// Package main prints a Quarry query with optional filters and safe paging.
package main

import (
	"fmt"

	"github.com/sphireinc/quarry"
)

func main() {
	params := struct {
		TenantID int
		Search   string
		Status   *string
		Page     int
		PerPage  int
	}{
		TenantID: 42,
		Search:   "%bob%",
		Page:     1,
		PerPage:  25,
	}

	qq := quarry.New(quarry.Postgres)
	sqlText, args, err := qq.Select("id", "email", "created_at").
		From("users").
		Where(
			quarry.Eq("tenant_id", params.TenantID),
			quarry.Or(
				quarry.OptionalILike("email", params.Search),
				quarry.OptionalILike("name", params.Search),
			),
			quarry.OptionalEq("status", params.Status),
		).
		OrderBySafeDefault("newest", quarry.SortMap{
			"newest": "created_at DESC",
			"email":  "email ASC",
		}, "newest").
		Page(params.Page, params.PerPage).
		ToSQL()
	if err != nil {
		panic(err)
	}

	fmt.Println(sqlText)
	fmt.Println(args)
}
