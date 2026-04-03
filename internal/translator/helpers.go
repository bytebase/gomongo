package translator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bytebase/omni/mongo/ast"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// convertNode converts an omni AST node to a Go value for BSON.
func convertNode(node ast.Node) (any, error) {
	switch n := node.(type) {
	case *ast.Document:
		return convertDocument(n)
	case *ast.Array:
		return convertArray(n)
	case *ast.StringLiteral:
		return n.Value, nil
	case *ast.NumberLiteral:
		return parseNumber(n.Value)
	case *ast.BoolLiteral:
		return n.Value, nil
	case *ast.NullLiteral:
		return nil, nil
	case *ast.RegexLiteral:
		return bson.Regex{Pattern: n.Pattern, Options: n.Flags}, nil
	case *ast.HelperCall:
		return convertHelper(n)
	case *ast.Identifier:
		return nil, fmt.Errorf("unsupported value: identifier %q", n.Name)
	default:
		return nil, fmt.Errorf("unsupported AST node type: %T", node)
	}
}

// convertDocument converts an ast.Document to bson.D.
func convertDocument(doc *ast.Document) (bson.D, error) {
	result := bson.D{}
	for _, kv := range doc.Pairs {
		val, err := convertNode(kv.Value)
		if err != nil {
			return nil, fmt.Errorf("error converting value for key %q: %w", kv.Key, err)
		}
		result = append(result, bson.E{Key: kv.Key, Value: val})
	}
	return result, nil
}

// convertArray converts an ast.Array to bson.A.
func convertArray(arr *ast.Array) (bson.A, error) {
	result := bson.A{}
	for _, elem := range arr.Elements {
		val, err := convertNode(elem)
		if err != nil {
			return nil, err
		}
		result = append(result, val)
	}
	return result, nil
}

// parseNumber parses a number string to int32, int64, or float64.
func parseNumber(s string) (any, error) {
	if strings.Contains(s, ".") || strings.Contains(s, "e") || strings.Contains(s, "E") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number: %s", s)
		}
		return f, nil
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number: %s", s)
	}

	if i >= -2147483648 && i <= 2147483647 {
		return int32(i), nil
	}
	return i, nil
}

// ToInt64 converts various numeric types to int64.
func ToInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	}
	return 0, false
}

// ToInt32 converts various numeric types to int32.
func ToInt32(v any) (int32, bool) {
	switch n := v.(type) {
	case int:
		return int32(n), true
	case int32:
		return n, true
	case int64:
		return int32(n), true
	case float64:
		return int32(n), true
	}
	return 0, false
}

// requireDocument extracts and converts a document node from args at the given index.
func requireDocument(args []ast.Node, idx int, context string) (bson.D, error) {
	if idx >= len(args) {
		return nil, fmt.Errorf("%s must be a document", context)
	}
	doc, ok := args[idx].(*ast.Document)
	if !ok {
		return nil, fmt.Errorf("%s must be a document", context)
	}
	return convertDocument(doc)
}

// requireString extracts a string value from args at the given index.
func requireString(args []ast.Node, idx int, context string) (string, error) {
	if idx >= len(args) {
		return "", fmt.Errorf("%s must be a string", context)
	}
	str, ok := args[idx].(*ast.StringLiteral)
	if !ok {
		return "", fmt.Errorf("%s must be a string", context)
	}
	return str.Value, nil
}
