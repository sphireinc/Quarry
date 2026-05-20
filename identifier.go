package quarry

import (
	"fmt"
	"strings"
)

// appendQuotedIdentifier validates ident and writes it quoted for the active dialect.
func appendQuotedIdentifier(b *sqlBuilder, ident string) error {
	quoted, err := b.dialect.QuoteIdent(ident)
	if err != nil {
		return err
	}
	b.write(quoted)
	return nil
}

// appendIdentifierLike renders a trusted identifier-like value.
//
// Plain strings are validated locally and quoted segment-by-segment. Raw SQL
// fragments must be passed explicitly through Quarry's expression types.
func appendIdentifierLike(b *sqlBuilder, v any) error {
	switch x := v.(type) {
	case string:
		return appendIdentifierString(b, x)
	case Table:
		return x.appendSQL(b)
	case *Table:
		if x == nil {
			return fmt.Errorf("quarry: nil table")
		}
		return x.appendSQL(b)
	case Column:
		return x.appendSQL(b)
	case *Column:
		if x == nil {
			return fmt.Errorf("quarry: nil column")
		}
		return x.appendSQL(b)
	case Expr:
		if isNilValue(x) {
			return fmt.Errorf("quarry: nil expression")
		}
		return x.appendSQL(b)
	default:
		return fmt.Errorf("quarry: unsupported identifier type %T", v)
	}
}

// appendIdentifierString validates and quotes a possibly-qualified identifier.
func appendIdentifierString(b *sqlBuilder, ident string) error {
	if ident == "*" {
		b.write("*")
		return nil
	}
	if strings.TrimSpace(ident) == "" {
		return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
	}
	parts := strings.Split(ident, ".")
	for i, part := range parts {
		if part == "" {
			return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
		}
		if part == "*" {
			if i != len(parts)-1 {
				return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
			}
			if i > 0 {
				b.write(".")
			}
			b.write("*")
			return nil
		}
		if err := validateIdentifier(part); err != nil {
			return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
		}
		if i > 0 {
			b.write(".")
		}
		if err := appendQuotedIdentifier(b, part); err != nil {
			return err
		}
	}
	return nil
}

// validateIdentifier enforces the simple identifier rule used by safe identifier helpers.
func validateIdentifier(ident string) error {
	if ident == "" {
		return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
	}
	for i, r := range ident {
		if i == 0 {
			if !isIdentifierStart(r) {
				return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
			}
			continue
		}
		if !isIdentifierPart(r) {
			return fmt.Errorf("quarry: %w %q", ErrInvalidIdentifier, ident)
		}
	}
	return nil
}

// isIdentifierStart reports whether r can start a safe identifier.
func isIdentifierStart(r rune) bool {
	return r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')
}

// isIdentifierPart reports whether r can continue a safe identifier.
func isIdentifierPart(r rune) bool {
	return isIdentifierStart(r) || (r >= '0' && r <= '9')
}

// columnMapKey extracts the underlying name from safe identifier helpers or plain strings.
func columnMapKey(v any) (string, bool) {
	switch x := v.(type) {
	case string:
		if x == "" {
			return "", false
		}
		return x, true
	case quotedIdentifier:
		if string(x) == "" {
			return "", false
		}
		return string(x), true
	case Column:
		if x.name == "" {
			return "", false
		}
		return x.name, true
	case *Column:
		if x == nil || x.name == "" {
			return "", false
		}
		return x.name, true
	case Table:
		if x.name == "" {
			return "", false
		}
		return x.name, true
	case *Table:
		if x == nil || x.name == "" {
			return "", false
		}
		return x.name, true
	default:
		return "", false
	}
}

// quotedIdentifier renders a validated identifier using the active dialect.
type quotedIdentifier string

// appendSQL renders the identifier with dialect quoting.
func (i quotedIdentifier) appendSQL(b *sqlBuilder) error {
	return appendQuotedIdentifier(b, string(i))
}

// invalidIdentifierExpr defers an identifier validation failure until render time.
type invalidIdentifierExpr struct {
	kind string
	name string
}

// appendSQL reports the invalid identifier with the original context preserved.
func (i invalidIdentifierExpr) appendSQL(_ *sqlBuilder) error {
	return fmt.Errorf("quarry: invalid %s %q: %w", i.kind, i.name, ErrInvalidIdentifier)
}

// mapIdentifierExpr converts a map key into a renderable identifier expression.
func mapIdentifierExpr(kind, name string) Expr {
	if err := validateIdentifier(name); err != nil {
		return invalidIdentifierExpr{kind: kind, name: name}
	}
	return quotedIdentifier(name)
}

// ensureUniqueColumnKeys rejects duplicate identifier-like columns before rendering invalid SQL.
func ensureUniqueColumnKeys(kind string, values []any) error {
	seen := make(map[string]struct{}, len(values))
	for _, v := range values {
		key, ok := columnMapKey(v)
		if !ok {
			continue
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("quarry: duplicate %s column %q: %w", kind, key, ErrInvalidBuilderState)
		}
		seen[key] = struct{}{}
	}
	return nil
}

// requireTableValue rejects missing or blank string tables before rendering.
func requireTableValue(v any, op string) error {
	if v == nil {
		return fmt.Errorf("quarry: %s requires a table", op)
	}
	if s, ok := v.(string); ok && strings.TrimSpace(s) == "" {
		return fmt.Errorf("quarry: %s requires a table", op)
	}
	return nil
}
