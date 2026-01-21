package translator

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bytebase/parser/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// unquoteString removes quotes from a string literal.
func unquoteString(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// convertValue converts a parsed value context to a Go value for BSON.
func convertValue(ctx mongodb.IValueContext) (any, error) {
	switch v := ctx.(type) {
	case *mongodb.DocumentValueContext:
		return convertDocument(v.Document())
	case *mongodb.ArrayValueContext:
		return convertArray(v.Array())
	case *mongodb.LiteralValueContext:
		return convertLiteral(v.Literal())
	case *mongodb.HelperValueContext:
		return convertHelperFunction(v.HelperFunction())
	case *mongodb.RegexLiteralValueContext:
		return convertRegexLiteral(v.REGEX_LITERAL().GetText())
	case *mongodb.RegexpConstructorValueContext:
		return convertRegExpConstructor(v.RegExpConstructor())
	default:
		return nil, fmt.Errorf("unsupported value type: %T", ctx)
	}
}

// convertDocument converts a document context to bson.D.
func convertDocument(ctx mongodb.IDocumentContext) (bson.D, error) {
	doc, ok := ctx.(*mongodb.DocumentContext)
	if !ok {
		return nil, fmt.Errorf("invalid document context")
	}

	result := bson.D{}
	for _, pair := range doc.AllPair() {
		key, value, err := convertPair(pair)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.E{Key: key, Value: value})
	}
	return result, nil
}

// convertPair converts a pair context to key-value.
func convertPair(ctx mongodb.IPairContext) (string, any, error) {
	pair, ok := ctx.(*mongodb.PairContext)
	if !ok {
		return "", nil, fmt.Errorf("invalid pair context")
	}

	key := extractKey(pair.Key())
	value, err := convertValue(pair.Value())
	if err != nil {
		return "", nil, fmt.Errorf("error converting value for key %q: %w", key, err)
	}
	return key, value, nil
}

// extractKey extracts the key string from a key context.
func extractKey(ctx mongodb.IKeyContext) string {
	switch k := ctx.(type) {
	case *mongodb.UnquotedKeyContext:
		return k.Identifier().GetText()
	case *mongodb.QuotedKeyContext:
		return unquoteString(k.StringLiteral().GetText())
	default:
		return ""
	}
}

// convertArray converts an array context to bson.A.
func convertArray(ctx mongodb.IArrayContext) (bson.A, error) {
	arr, ok := ctx.(*mongodb.ArrayContext)
	if !ok {
		return nil, fmt.Errorf("invalid array context")
	}

	result := bson.A{}
	for _, val := range arr.AllValue() {
		v, err := convertValue(val)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

// convertLiteral converts a literal context to a Go value.
func convertLiteral(ctx mongodb.ILiteralContext) (any, error) {
	switch l := ctx.(type) {
	case *mongodb.NumberLiteralContext:
		return parseNumber(l.NUMBER().GetText())
	case *mongodb.StringLiteralValueContext:
		return unquoteString(l.StringLiteral().GetText()), nil
	case *mongodb.TrueLiteralContext:
		return true, nil
	case *mongodb.FalseLiteralContext:
		return false, nil
	case *mongodb.NullLiteralContext:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", ctx)
	}
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

// convertRegexLiteral converts a regex literal like /pattern/flags to bson.Regex.
func convertRegexLiteral(text string) (bson.Regex, error) {
	if len(text) < 2 || text[0] != '/' {
		return bson.Regex{}, fmt.Errorf("invalid regex literal: %s", text)
	}

	lastSlash := strings.LastIndex(text, "/")
	if lastSlash <= 0 {
		return bson.Regex{}, fmt.Errorf("invalid regex literal: %s", text)
	}

	pattern := text[1:lastSlash]
	options := ""
	if lastSlash < len(text)-1 {
		options = text[lastSlash+1:]
	}

	return bson.Regex{Pattern: pattern, Options: options}, nil
}

// convertRegExpConstructor converts RegExp("pattern", "flags") to bson.Regex.
func convertRegExpConstructor(ctx mongodb.IRegExpConstructorContext) (bson.Regex, error) {
	constructor, ok := ctx.(*mongodb.RegExpConstructorContext)
	if !ok {
		return bson.Regex{}, fmt.Errorf("invalid RegExp constructor context")
	}

	strings := constructor.AllStringLiteral()
	if len(strings) == 0 {
		return bson.Regex{}, fmt.Errorf("RegExp requires at least a pattern argument")
	}

	pattern := unquoteString(strings[0].GetText())
	options := ""
	if len(strings) > 1 {
		options = unquoteString(strings[1].GetText())
	}

	return bson.Regex{Pattern: pattern, Options: options}, nil
}
