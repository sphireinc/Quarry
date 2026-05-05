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
