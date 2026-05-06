# Quarry Comparison

Quarry sits in the narrow space between raw string SQL and a heavier database abstraction.
It helps when the hard part is dynamic query assembly, placeholder handling, and keeping
user-controlled inputs separate from trusted SQL fragments.

## Quarry vs Raw String SQL

Raw SQL is still the mental model in Quarry. The difference is that Quarry gives you
small helpers for values, identifiers, optional predicates, and dialect-aware rendering.

Use Quarry when:

- you want SQL to stay visible
- you want args bound in order instead of interpolated
- you want to compose optional filters without building strings by hand

Stay with raw SQL when:

- the query is simple enough that helpers would add more ceremony than value
- you do not need dialect-aware placeholder handling

## Quarry vs Squirrel

Squirrel proved that explicit SQL composition is practical. Quarry keeps that spirit, but
leans harder into identifier safety, dialect policy, and an intentionally small public surface.

Quarry is a better fit when:

- you want the builder surface to stay compact
- you want dialect behavior documented up front
- you want raw SQL to remain a first-class escape hatch

Squirrel may still be the better fit when:

- your project already depends on it
- you want its broader ecosystem and long-standing familiarity

## Quarry vs sqlc

sqlc generates Go code from SQL files. Quarry does not.

Use Quarry instead of sqlc when:

- your hardest problem is composing SQL dynamically
- you want to keep the SQL hand-written and explicit
- you do not want code generation in the workflow

Use sqlc instead of Quarry when:

- you want compile-time generated query code from static SQL
- your query set is mostly fixed and generation is a better fit

Quarry is not a sqlc replacement. It can coexist with sqlc in the same repository if
different parts of the application benefit from different approaches.

## Quarry vs GORM

GORM is an ORM. Quarry is not.

Use Quarry when:

- you want to decide exactly what SQL gets sent
- you want values and identifiers to stay explicit
- you do not want entity tracking or relationship loading

Use GORM when:

- you want model-centric persistence and ORM features
- you are comfortable with the extra abstraction

Quarry deliberately stops before ORM behavior starts.

## Quarry vs sqlx

sqlx is great when you want `database/sql` with better scanning and convenience helpers.
Quarry fits alongside that style, especially when SQL assembly is the main pain point.

Use Quarry with sqlx when:

- you want to build the query with Quarry and execute it with sqlx
- you want a thin layer around `database/sql` without losing explicit SQL

Use sqlx alone when:

- your query text is already stable and you mainly want nicer scanning

## Practical Rule

If the problem is "how do I model my database?", Quarry is probably the wrong tool.
If the problem is "how do I build this SQL without string-concat traps?", Quarry is
usually a good fit.
