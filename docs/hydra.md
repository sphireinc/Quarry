# Hydra Integration

Hydra is Sphire's standalone struct hydration library.

Quarry and Hydra can be used together:

1. Quarry composes SQL and args.
2. `database/sql`, `sqlx`, or `pgx` executes the query.
3. Hydra hydrates structs from the result.

Use Quarry/scan when you want lightweight scanning.
Use Hydra when you want richer hydration behavior.

Quarry does not depend on Hydra.

For Quarry's lightweight result-scanning package, see [docs/scan.md](scan.md).
