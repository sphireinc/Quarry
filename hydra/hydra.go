package hydra

import (
	"context"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/scan"
)

func All[T any](ctx context.Context, db scan.Queryer, q quarry.SQLer) ([]T, error) {
	return scan.All[T](ctx, db, q)
}

func One[T any](ctx context.Context, db scan.Queryer, q quarry.SQLer) (T, error) {
	return scan.One[T](ctx, db, q)
}

func MaybeOne[T any](ctx context.Context, db scan.Queryer, q quarry.SQLer) (*T, error) {
	return scan.MaybeOne[T](ctx, db, q)
}
