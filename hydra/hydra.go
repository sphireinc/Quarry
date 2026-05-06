// Package hydra provides a thin compatibility wrapper around Quarry's scan package.
//
// It exists so older imports can keep the same row-scanning helpers without
// pulling in a separate abstraction layer.
package hydra

import (
	"context"

	"github.com/sphireinc/quarry"
	"github.com/sphireinc/quarry/scan"
)

// All delegates to scan.All so hydra remains a thin compatibility wrapper.
func All[T any](ctx context.Context, db scan.Queryer, q quarry.SQLer) ([]T, error) {
	return scan.All[T](ctx, db, q)
}

// One delegates to scan.One so hydra keeps the same row-count semantics.
func One[T any](ctx context.Context, db scan.Queryer, q quarry.SQLer) (T, error) {
	return scan.One[T](ctx, db, q)
}

// MaybeOne delegates to scan.MaybeOne and preserves its nil-on-empty behavior.
func MaybeOne[T any](ctx context.Context, db scan.Queryer, q quarry.SQLer) (*T, error) {
	return scan.MaybeOne[T](ctx, db, q)
}
