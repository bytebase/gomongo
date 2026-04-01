package translator

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/bytebase/omni/mongo/ast"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// convertHelper converts an ast.HelperCall to a BSON value.
func convertHelper(h *ast.HelperCall) (any, error) {
	switch h.Name {
	case "ObjectId":
		return convertObjectId(h.Args)
	case "ISODate":
		return convertISODate(h.Args)
	case "Date":
		return convertDate(h.Args)
	case "UUID":
		return convertUUID(h.Args)
	case "NumberLong", "Long":
		return convertLong(h.Args)
	case "NumberInt", "Int32":
		return convertInt32(h.Args)
	case "Double":
		return convertDouble(h.Args)
	case "NumberDecimal", "Decimal128":
		return convertDecimal128(h.Args)
	case "Timestamp":
		return convertTimestamp(h.Args)
	default:
		return nil, fmt.Errorf("unsupported helper: %s()", h.Name)
	}
}

func convertObjectId(args []ast.Node) (bson.ObjectID, error) {
	if len(args) == 0 {
		return bson.NewObjectID(), nil
	}
	str, ok := args[0].(*ast.StringLiteral)
	if !ok {
		return bson.ObjectID{}, fmt.Errorf("ObjectId() argument must be a string")
	}
	hexStr := str.Value
	if len(hexStr) != 24 {
		return bson.ObjectID{}, fmt.Errorf("invalid ObjectId: %q is not a valid 24-character hex string", hexStr)
	}
	bytes, err := hex.DecodeString(hexStr)
	if err != nil {
		return bson.ObjectID{}, fmt.Errorf("invalid ObjectId: %q is not valid hex", hexStr)
	}
	var oid bson.ObjectID
	copy(oid[:], bytes)
	return oid, nil
}

func convertISODate(args []ast.Node) (bson.DateTime, error) {
	if len(args) == 0 {
		return bson.DateTime(time.Now().UnixMilli()), nil
	}
	str, ok := args[0].(*ast.StringLiteral)
	if !ok {
		return 0, fmt.Errorf("ISODate() argument must be a string")
	}
	return parseDateTime(str.Value)
}

func convertDate(args []ast.Node) (any, error) {
	if len(args) == 0 {
		return bson.DateTime(time.Now().UnixMilli()), nil
	}
	switch a := args[0].(type) {
	case *ast.StringLiteral:
		return parseDateTime(a.Value)
	case *ast.NumberLiteral:
		ts, err := strconv.ParseInt(a.Value, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid timestamp: %w", err)
		}
		return bson.DateTime(ts), nil
	default:
		return nil, fmt.Errorf("Date() argument must be a string or number")
	}
}

// parseDateTime parses various date formats to bson.DateTime.
func parseDateTime(s string) (bson.DateTime, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05.000Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return bson.DateTime(t.UnixMilli()), nil
		}
	}
	return 0, fmt.Errorf("invalid date format: %s", s)
}

func convertUUID(args []ast.Node) (bson.Binary, error) {
	if len(args) == 0 {
		return bson.Binary{}, fmt.Errorf("UUID requires a string argument")
	}
	str, ok := args[0].(*ast.StringLiteral)
	if !ok {
		return bson.Binary{}, fmt.Errorf("UUID() argument must be a string")
	}
	parsed, err := uuid.Parse(str.Value)
	if err != nil {
		return bson.Binary{}, fmt.Errorf("invalid UUID: %w", err)
	}
	return bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data:    parsed[:],
	}, nil
}

func convertLong(args []ast.Node) (int64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	switch a := args[0].(type) {
	case *ast.NumberLiteral:
		return strconv.ParseInt(a.Value, 10, 64)
	case *ast.StringLiteral:
		return strconv.ParseInt(a.Value, 10, 64)
	default:
		return 0, fmt.Errorf("Long() argument must be a number or string")
	}
}

func convertInt32(args []ast.Node) (int32, error) {
	if len(args) == 0 {
		return 0, nil
	}
	var numStr string
	switch a := args[0].(type) {
	case *ast.NumberLiteral:
		numStr = a.Value
	case *ast.StringLiteral:
		numStr = a.Value
	default:
		return 0, fmt.Errorf("Int32() argument must be a number or string")
	}
	i, err := strconv.ParseInt(numStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

func convertDouble(args []ast.Node) (float64, error) {
	if len(args) == 0 {
		return 0, nil
	}
	num, ok := args[0].(*ast.NumberLiteral)
	if !ok {
		return 0, fmt.Errorf("Double() argument must be a number")
	}
	return strconv.ParseFloat(num.Value, 64)
}

func convertDecimal128(args []ast.Node) (bson.Decimal128, error) {
	if len(args) == 0 {
		return bson.Decimal128{}, fmt.Errorf("Decimal128 requires a string argument")
	}
	str, ok := args[0].(*ast.StringLiteral)
	if !ok {
		return bson.Decimal128{}, fmt.Errorf("Decimal128() argument must be a string")
	}
	d, err := bson.ParseDecimal128(str.Value)
	if err != nil {
		return bson.Decimal128{}, fmt.Errorf("invalid Decimal128: %w", err)
	}
	return d, nil
}

func convertTimestamp(args []ast.Node) (bson.Timestamp, error) {
	if len(args) == 0 {
		return bson.Timestamp{}, fmt.Errorf("timestamp requires arguments")
	}
	// Timestamp({t: 123, i: 1}) form
	if doc, ok := args[0].(*ast.Document); ok {
		return convertTimestampDoc(doc)
	}
	// Timestamp(t, i) form
	if len(args) < 2 {
		return bson.Timestamp{}, fmt.Errorf("timestamp requires t and i arguments")
	}
	tNum, ok := args[0].(*ast.NumberLiteral)
	if !ok {
		return bson.Timestamp{}, fmt.Errorf("timestamp t must be a number")
	}
	iNum, ok := args[1].(*ast.NumberLiteral)
	if !ok {
		return bson.Timestamp{}, fmt.Errorf("timestamp i must be a number")
	}
	t, err := strconv.ParseUint(tNum.Value, 10, 32)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid Timestamp t value: %w", err)
	}
	i, err := strconv.ParseUint(iNum.Value, 10, 32)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid Timestamp i value: %w", err)
	}
	return bson.Timestamp{T: uint32(t), I: uint32(i)}, nil
}

func convertTimestampDoc(doc *ast.Document) (bson.Timestamp, error) {
	d, err := convertDocument(doc)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid Timestamp document: %w", err)
	}
	var t, i uint32
	for _, elem := range d {
		switch elem.Key {
		case "t":
			if v, ok := elem.Value.(int32); ok {
				t = uint32(v)
			} else if v, ok := elem.Value.(int64); ok {
				t = uint32(v)
			}
		case "i":
			if v, ok := elem.Value.(int32); ok {
				i = uint32(v)
			} else if v, ok := elem.Value.(int64); ok {
				i = uint32(v)
			}
		}
	}
	return bson.Timestamp{T: t, I: i}, nil
}
