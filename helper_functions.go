package gomongo

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"time"

	"github.com/bytebase/parser/mongodb"
	"github.com/google/uuid"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// convertObjectIdHelper converts ObjectId("hex") to primitive.ObjectID.
func convertObjectIdHelper(ctx mongodb.IObjectIdHelperContext) (bson.ObjectID, error) {
	helper, ok := ctx.(*mongodb.ObjectIdHelperContext)
	if !ok {
		return bson.ObjectID{}, fmt.Errorf("invalid ObjectId helper context")
	}

	if helper.StringLiteral() == nil {
		return bson.NewObjectID(), nil
	}

	hexStr := unquoteString(helper.StringLiteral().GetText())
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

// convertIsoDateHelper converts ISODate("iso-string") to primitive.DateTime.
func convertIsoDateHelper(ctx mongodb.IIsoDateHelperContext) (bson.DateTime, error) {
	helper, ok := ctx.(*mongodb.IsoDateHelperContext)
	if !ok {
		return 0, fmt.Errorf("invalid ISODate helper context")
	}

	if helper.StringLiteral() == nil {
		return bson.DateTime(time.Now().UnixMilli()), nil
	}

	dateStr := unquoteString(helper.StringLiteral().GetText())
	return parseDateTime(dateStr)
}

// convertDateHelper converts new Date() or Date() to primitive.DateTime or string.
func convertDateHelper(ctx mongodb.IDateHelperContext) (any, error) {
	helper, ok := ctx.(*mongodb.DateHelperContext)
	if !ok {
		return nil, fmt.Errorf("invalid Date helper context")
	}

	hasNew := helper.NEW() != nil

	if helper.StringLiteral() == nil {
		if hasNew {
			return bson.DateTime(time.Now().UnixMilli()), nil
		}
		return time.Now().Format(time.RFC3339), nil
	}

	dateStr := unquoteString(helper.StringLiteral().GetText())
	if hasNew {
		return parseDateTime(dateStr)
	}
	return dateStr, nil
}

// parseDateTime parses various date formats to primitive.DateTime.
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

// convertUuidHelper converts UUID("uuid-string") to primitive.Binary (subtype 4).
func convertUuidHelper(ctx mongodb.IUuidHelperContext) (bson.Binary, error) {
	helper, ok := ctx.(*mongodb.UuidHelperContext)
	if !ok {
		return bson.Binary{}, fmt.Errorf("invalid UUID helper context")
	}

	if helper.StringLiteral() == nil {
		return bson.Binary{}, fmt.Errorf("UUID requires a string argument")
	}

	uuidStr := unquoteString(helper.StringLiteral().GetText())
	parsed, err := uuid.Parse(uuidStr)
	if err != nil {
		return bson.Binary{}, fmt.Errorf("invalid UUID: %w", err)
	}

	return bson.Binary{
		Subtype: bson.TypeBinaryUUID,
		Data:    parsed[:],
	}, nil
}

// convertLongHelper converts Long(123) or NumberLong("123") to int64.
func convertLongHelper(ctx mongodb.ILongHelperContext) (int64, error) {
	helper, ok := ctx.(*mongodb.LongHelperContext)
	if !ok {
		return 0, fmt.Errorf("invalid Long helper context")
	}

	var numStr string
	if helper.NUMBER() != nil {
		numStr = helper.NUMBER().GetText()
	} else if helper.StringLiteral() != nil {
		numStr = unquoteString(helper.StringLiteral().GetText())
	} else {
		return 0, nil
	}

	return strconv.ParseInt(numStr, 10, 64)
}

// convertInt32Helper converts Int32(123) or NumberInt(123) to int32.
func convertInt32Helper(ctx mongodb.IInt32HelperContext) (int32, error) {
	helper, ok := ctx.(*mongodb.Int32HelperContext)
	if !ok {
		return 0, fmt.Errorf("invalid Int32 helper context")
	}

	if helper.NUMBER() == nil {
		return 0, nil
	}

	numStr := helper.NUMBER().GetText()
	i, err := strconv.ParseInt(numStr, 10, 32)
	if err != nil {
		return 0, err
	}
	return int32(i), nil
}

// convertDoubleHelper converts Double(1.5) to float64.
func convertDoubleHelper(ctx mongodb.IDoubleHelperContext) (float64, error) {
	helper, ok := ctx.(*mongodb.DoubleHelperContext)
	if !ok {
		return 0, fmt.Errorf("invalid Double helper context")
	}

	if helper.NUMBER() == nil {
		return 0, nil
	}

	return strconv.ParseFloat(helper.NUMBER().GetText(), 64)
}

// convertDecimal128Helper converts Decimal128("123.45") to primitive.Decimal128.
func convertDecimal128Helper(ctx mongodb.IDecimal128HelperContext) (bson.Decimal128, error) {
	helper, ok := ctx.(*mongodb.Decimal128HelperContext)
	if !ok {
		return bson.Decimal128{}, fmt.Errorf("invalid Decimal128 helper context")
	}

	if helper.StringLiteral() == nil {
		return bson.Decimal128{}, fmt.Errorf("Decimal128 requires a string argument")
	}

	decStr := unquoteString(helper.StringLiteral().GetText())
	d, err := bson.ParseDecimal128(decStr)
	if err != nil {
		return bson.Decimal128{}, fmt.Errorf("invalid Decimal128: %w", err)
	}
	return d, nil
}

// convertTimestampHelper converts Timestamp(t, i) to primitive.Timestamp.
func convertTimestampHelper(ctx mongodb.ITimestampHelperContext) (bson.Timestamp, error) {
	switch h := ctx.(type) {
	case *mongodb.TimestampArgsHelperContext:
		return convertTimestampArgs(h)
	case *mongodb.TimestampDocHelperContext:
		return convertTimestampDoc(h)
	default:
		return bson.Timestamp{}, fmt.Errorf("unsupported Timestamp helper type: %T", ctx)
	}
}

// convertTimestampArgs converts Timestamp(t, i) format.
func convertTimestampArgs(ctx *mongodb.TimestampArgsHelperContext) (bson.Timestamp, error) {
	numbers := ctx.AllNUMBER()
	if len(numbers) < 2 {
		return bson.Timestamp{}, fmt.Errorf("timestamp requires t and i arguments")
	}

	t, err := strconv.ParseUint(numbers[0].GetText(), 10, 32)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid Timestamp t value: %w", err)
	}

	i, err := strconv.ParseUint(numbers[1].GetText(), 10, 32)
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid Timestamp i value: %w", err)
	}

	return bson.Timestamp{T: uint32(t), I: uint32(i)}, nil
}

// convertTimestampDoc converts Timestamp({t: 123, i: 1}) format.
func convertTimestampDoc(ctx *mongodb.TimestampDocHelperContext) (bson.Timestamp, error) {
	doc, err := convertDocument(ctx.Document())
	if err != nil {
		return bson.Timestamp{}, fmt.Errorf("invalid Timestamp document: %w", err)
	}

	var t, i uint32
	for _, elem := range doc {
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
