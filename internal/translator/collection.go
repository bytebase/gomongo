package translator

import (
	"fmt"
	"strconv"

	"github.com/bytebase/omni/mongo/ast"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func extractFindArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return nil
	}

	// First argument: filter
	filter, err := requireDocument(args, 0, "find() filter")
	if err != nil {
		return err
	}
	op.Filter = filter

	// Second argument: projection (optional)
	if len(args) >= 2 {
		projection, err := requireDocument(args, 1, "find() projection")
		if err != nil {
			return err
		}
		op.Projection = projection
	}

	// Third argument: options (optional)
	if len(args) >= 3 {
		options, err := requireDocument(args, 2, "find() options")
		if err != nil {
			return err
		}
		if err := extractFindOptions(op, "find", options); err != nil {
			return err
		}
	}

	if len(args) > 3 {
		return fmt.Errorf("find() takes at most 3 arguments")
	}
	return nil
}

func extractFindOneArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return nil
	}

	// First argument: filter
	filter, err := requireDocument(args, 0, "findOne() filter")
	if err != nil {
		return err
	}
	op.Filter = filter

	// Second argument: projection (optional)
	if len(args) >= 2 {
		projection, err := requireDocument(args, 1, "findOne() projection")
		if err != nil {
			return err
		}
		op.Projection = projection
	}

	// Third argument: options (optional)
	if len(args) >= 3 {
		options, err := requireDocument(args, 2, "findOne() options")
		if err != nil {
			return err
		}
		if err := extractFindOptions(op, "findOne", options); err != nil {
			return err
		}
	}

	if len(args) > 3 {
		return fmt.Errorf("findOne() takes at most 3 arguments")
	}
	return nil
}

// extractFindOptions extracts supported options for find/findOne.
func extractFindOptions(op *Operation, methodName string, options bson.D) error {
	for _, opt := range options {
		switch opt.Key {
		case "hint":
			op.Hint = opt.Value
		case "max":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Max = doc
			} else {
				return fmt.Errorf("%s() max must be a document", methodName)
			}
		case "min":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Min = doc
			} else {
				return fmt.Errorf("%s() min must be a document", methodName)
			}
		case "maxTimeMS":
			if val, ok := opt.Value.(int32); ok {
				ms := int64(val)
				op.MaxTimeMS = &ms
			} else if val, ok := opt.Value.(int64); ok {
				op.MaxTimeMS = &val
			} else {
				return fmt.Errorf("%s() maxTimeMS must be a number", methodName)
			}
		default:
			return &UnsupportedOptionError{
				Method: methodName + "()",
				Option: opt.Key,
			}
		}
	}
	return nil
}

func extractSort(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("sort() requires a document argument")
	}
	sort, err := requireDocument(args, 0, "sort() argument")
	if err != nil {
		return err
	}
	op.Sort = sort
	return nil
}

func extractLimit(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("limit() requires a number argument")
	}
	num, ok := args[0].(*ast.NumberLiteral)
	if !ok {
		return fmt.Errorf("limit() requires a number argument")
	}
	limit, err := strconv.ParseInt(num.Value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid limit: %w", err)
	}
	op.Limit = &limit
	return nil
}

func extractSkip(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("skip() requires a number argument")
	}
	num, ok := args[0].(*ast.NumberLiteral)
	if !ok {
		return fmt.Errorf("skip() requires a number argument")
	}
	skip, err := strconv.ParseInt(num.Value, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid skip: %w", err)
	}
	op.Skip = &skip
	return nil
}

func extractProjection(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("projection() requires a document argument")
	}
	projection, err := requireDocument(args, 0, "projection() argument")
	if err != nil {
		return err
	}
	op.Projection = projection
	return nil
}

func extractCursorHint(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("hint() requires an argument")
	}
	switch a := args[0].(type) {
	case *ast.StringLiteral:
		op.Hint = a.Value
	case *ast.Document:
		doc, err := convertDocument(a)
		if err != nil {
			return fmt.Errorf("invalid hint: %w", err)
		}
		op.Hint = doc
	default:
		return fmt.Errorf("hint() argument must be a string or document")
	}
	return nil
}

func extractMax(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("max() requires a document argument")
	}
	maxDoc, err := requireDocument(args, 0, "max() argument")
	if err != nil {
		return err
	}
	op.Max = maxDoc
	return nil
}

func extractMin(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("min() requires a document argument")
	}
	minDoc, err := requireDocument(args, 0, "min() argument")
	if err != nil {
		return err
	}
	op.Min = minDoc
	return nil
}

func extractAggregateArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		op.Pipeline = bson.A{}
		return nil
	}

	// First argument: pipeline array
	arr, ok := args[0].(*ast.Array)
	if !ok {
		return fmt.Errorf("aggregate() requires an array argument, got %T", args[0])
	}
	pipeline, err := convertArray(arr)
	if err != nil {
		return fmt.Errorf("invalid aggregation pipeline: %w", err)
	}
	op.Pipeline = pipeline

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "aggregate() options")
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "hint":
				op.Hint = opt.Value
			case "maxTimeMS":
				if val, ok := opt.Value.(int32); ok {
					ms := int64(val)
					op.MaxTimeMS = &ms
				} else if val, ok := opt.Value.(int64); ok {
					op.MaxTimeMS = &val
				} else {
					return fmt.Errorf("aggregate() maxTimeMS must be a number")
				}
			default:
				return &UnsupportedOptionError{
					Method: "aggregate()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return fmt.Errorf("aggregate() takes at most 2 arguments")
	}
	return nil
}

func extractCountDocumentsArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return nil
	}

	// First argument: filter (optional)
	filter, err := requireDocument(args, 0, "countDocuments() filter")
	if err != nil {
		return err
	}
	op.Filter = filter

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "countDocuments() options")
		if err != nil {
			return err
		}
		for _, elem := range options {
			switch elem.Key {
			case "hint":
				op.Hint = elem.Value
			case "limit":
				if val, ok := elem.Value.(int32); ok {
					limit := int64(val)
					op.Limit = &limit
				} else if val, ok := elem.Value.(int64); ok {
					op.Limit = &val
				}
			case "skip":
				if val, ok := elem.Value.(int32); ok {
					skip := int64(val)
					op.Skip = &skip
				} else if val, ok := elem.Value.(int64); ok {
					op.Skip = &val
				}
			case "maxTimeMS":
				if val, ok := elem.Value.(int32); ok {
					ms := int64(val)
					op.MaxTimeMS = &ms
				} else if val, ok := elem.Value.(int64); ok {
					op.MaxTimeMS = &val
				} else {
					return fmt.Errorf("countDocuments() maxTimeMS must be a number")
				}
			default:
				return &UnsupportedOptionError{
					Method: "countDocuments()",
					Option: elem.Key,
				}
			}
		}
	}

	return nil
}

func extractEstimatedDocumentCountArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return nil
	}

	// Single optional argument: options document
	options, err := requireDocument(args, 0, "estimatedDocumentCount() options")
	if err != nil {
		return err
	}
	for _, opt := range options {
		switch opt.Key {
		case "maxTimeMS":
			if val, ok := opt.Value.(int32); ok {
				ms := int64(val)
				op.MaxTimeMS = &ms
			} else if val, ok := opt.Value.(int64); ok {
				op.MaxTimeMS = &val
			} else {
				return fmt.Errorf("estimatedDocumentCount() maxTimeMS must be a number")
			}
		default:
			return &UnsupportedOptionError{
				Method: "estimatedDocumentCount()",
				Option: opt.Key,
			}
		}
	}
	return nil
}

func extractDistinctArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("distinct() requires a field name argument")
	}

	// First argument: field name (required)
	fieldName, err := requireString(args, 0, "distinct() field name")
	if err != nil {
		return err
	}
	op.DistinctField = fieldName

	// Second argument: filter (optional)
	if len(args) >= 2 {
		filter, err := requireDocument(args, 1, "distinct() filter")
		if err != nil {
			return err
		}
		op.Filter = filter
	}

	// Third argument: options (optional)
	if len(args) >= 3 {
		options, err := requireDocument(args, 2, "distinct() options")
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "maxTimeMS":
				if val, ok := opt.Value.(int32); ok {
					ms := int64(val)
					op.MaxTimeMS = &ms
				} else if val, ok := opt.Value.(int64); ok {
					op.MaxTimeMS = &val
				} else {
					return fmt.Errorf("distinct() maxTimeMS must be a number")
				}
			default:
				return &UnsupportedOptionError{
					Method: "distinct()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 3 {
		return fmt.Errorf("distinct() takes at most 3 arguments")
	}
	return nil
}

func extractInsertOneArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("insertOne() requires a document argument")
	}

	// First argument: document (required)
	doc, err := requireDocument(args, 0, "insertOne() document")
	if err != nil {
		// Provide a better error for non-document
		if _, ok := args[0].(*ast.Document); !ok {
			return fmt.Errorf("insertOne() document must be an object")
		}
		return err
	}
	op.Document = doc

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "insertOne() options")
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					op.BypassDocumentValidation = &val
				} else {
					return fmt.Errorf("insertOne() bypassDocumentValidation must be a boolean")
				}
			case "comment":
				op.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					op.WriteConcern = doc
				} else {
					return fmt.Errorf("insertOne() writeConcern must be a document")
				}
			default:
				return &UnsupportedOptionError{
					Method: "insertOne()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return fmt.Errorf("insertOne() takes at most 2 arguments")
	}
	return nil
}

func extractInsertManyArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("insertMany() requires an array argument")
	}

	// First argument: array of documents (required)
	arr, ok := args[0].(*ast.Array)
	if !ok {
		return fmt.Errorf("insertMany() requires an array argument")
	}
	bsonArr, err := convertArray(arr)
	if err != nil {
		return fmt.Errorf("invalid documents array: %w", err)
	}
	var docs []bson.D
	for i, elem := range bsonArr {
		doc, ok := elem.(bson.D)
		if !ok {
			return fmt.Errorf("insertMany() element %d must be a document", i)
		}
		docs = append(docs, doc)
	}
	op.Documents = docs

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "insertMany() options")
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "ordered":
				if val, ok := opt.Value.(bool); ok {
					op.Ordered = &val
				} else {
					return fmt.Errorf("insertMany() ordered must be a boolean")
				}
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					op.BypassDocumentValidation = &val
				} else {
					return fmt.Errorf("insertMany() bypassDocumentValidation must be a boolean")
				}
			case "comment":
				op.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					op.WriteConcern = doc
				} else {
					return fmt.Errorf("insertMany() writeConcern must be a document")
				}
			default:
				return &UnsupportedOptionError{
					Method: "insertMany()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return fmt.Errorf("insertMany() takes at most 2 arguments")
	}
	return nil
}

func extractUpdateArgs(op *Operation, methodName string, args []ast.Node) error {
	if len(args) < 2 {
		return fmt.Errorf("%s() requires filter and update arguments", methodName)
	}

	// First argument: filter (required)
	filter, err := requireDocument(args, 0, fmt.Sprintf("%s() filter", methodName))
	if err != nil {
		return err
	}
	op.Filter = filter

	// Second argument: update (required) - can be document or array (pipeline)
	switch u := args[1].(type) {
	case *ast.Document:
		update, err := convertDocument(u)
		if err != nil {
			return fmt.Errorf("invalid update: %w", err)
		}
		op.Update = update
	case *ast.Array:
		pipeline, err := convertArray(u)
		if err != nil {
			return fmt.Errorf("invalid update pipeline: %w", err)
		}
		op.Update = pipeline
	default:
		return fmt.Errorf("%s() update must be a document or array", methodName)
	}

	// Third argument: options (optional)
	if len(args) >= 3 {
		options, err := requireDocument(args, 2, fmt.Sprintf("%s() options", methodName))
		if err != nil {
			return err
		}
		if err := extractUpdateOptions(op, methodName, options); err != nil {
			return err
		}
	}

	if len(args) > 3 {
		return fmt.Errorf("%s() takes at most 3 arguments", methodName)
	}
	return nil
}

func extractUpdateOptions(op *Operation, methodName string, options bson.D) error {
	for _, opt := range options {
		switch opt.Key {
		case "upsert":
			if val, ok := opt.Value.(bool); ok {
				op.Upsert = &val
			} else {
				return fmt.Errorf("%s() upsert must be a boolean", methodName)
			}
		case "hint":
			op.Hint = opt.Value
		case "collation":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Collation = doc
			} else {
				return fmt.Errorf("%s() collation must be a document", methodName)
			}
		case "arrayFilters":
			if arr, ok := opt.Value.(bson.A); ok {
				op.ArrayFilters = arr
			} else {
				return fmt.Errorf("%s() arrayFilters must be an array", methodName)
			}
		case "let":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Let = doc
			} else {
				return fmt.Errorf("%s() let must be a document", methodName)
			}
		case "bypassDocumentValidation":
			if val, ok := opt.Value.(bool); ok {
				op.BypassDocumentValidation = &val
			} else {
				return fmt.Errorf("%s() bypassDocumentValidation must be a boolean", methodName)
			}
		case "comment":
			op.Comment = opt.Value
		case "sort":
			if methodName != "updateOne" {
				return &UnsupportedOptionError{
					Method: methodName + "()",
					Option: opt.Key,
				}
			}
			if doc, ok := opt.Value.(bson.D); ok {
				op.Sort = doc
			} else {
				return fmt.Errorf("%s() sort must be a document", methodName)
			}
		case "writeConcern":
			if doc, ok := opt.Value.(bson.D); ok {
				op.WriteConcern = doc
			} else {
				return fmt.Errorf("%s() writeConcern must be a document", methodName)
			}
		default:
			return &UnsupportedOptionError{
				Method: methodName + "()",
				Option: opt.Key,
			}
		}
	}
	return nil
}

func extractReplaceOneArgs(op *Operation, args []ast.Node) error {
	if len(args) < 2 {
		return fmt.Errorf("replaceOne() requires filter and replacement arguments")
	}

	// First argument: filter
	filter, err := requireDocument(args, 0, "replaceOne() filter")
	if err != nil {
		return err
	}
	op.Filter = filter

	// Second argument: replacement document
	replacement, err := requireDocument(args, 1, "replaceOne() replacement")
	if err != nil {
		return err
	}
	op.Replacement = replacement

	// Third argument: options (optional)
	if len(args) >= 3 {
		options, err := requireDocument(args, 2, "replaceOne() options")
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "upsert":
				if val, ok := opt.Value.(bool); ok {
					op.Upsert = &val
				} else {
					return fmt.Errorf("replaceOne() upsert must be a boolean")
				}
			case "hint":
				op.Hint = opt.Value
			case "collation":
				if doc, ok := opt.Value.(bson.D); ok {
					op.Collation = doc
				} else {
					return fmt.Errorf("replaceOne() collation must be a document")
				}
			case "let":
				if doc, ok := opt.Value.(bson.D); ok {
					op.Let = doc
				} else {
					return fmt.Errorf("replaceOne() let must be a document")
				}
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					op.BypassDocumentValidation = &val
				} else {
					return fmt.Errorf("replaceOne() bypassDocumentValidation must be a boolean")
				}
			case "comment":
				op.Comment = opt.Value
			case "sort":
				if doc, ok := opt.Value.(bson.D); ok {
					op.Sort = doc
				} else {
					return fmt.Errorf("replaceOne() sort must be a document")
				}
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					op.WriteConcern = doc
				} else {
					return fmt.Errorf("replaceOne() writeConcern must be a document")
				}
			default:
				return &UnsupportedOptionError{
					Method: "replaceOne()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 3 {
		return fmt.Errorf("replaceOne() takes at most 3 arguments")
	}
	return nil
}

func extractDeleteArgs(op *Operation, methodName string, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("%s() requires a filter argument", methodName)
	}

	// First argument: filter (required)
	filter, err := requireDocument(args, 0, fmt.Sprintf("%s() filter", methodName))
	if err != nil {
		return err
	}
	op.Filter = filter

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, fmt.Sprintf("%s() options", methodName))
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "hint":
				op.Hint = opt.Value
			case "collation":
				if doc, ok := opt.Value.(bson.D); ok {
					op.Collation = doc
				} else {
					return fmt.Errorf("%s() collation must be a document", methodName)
				}
			case "let":
				if doc, ok := opt.Value.(bson.D); ok {
					op.Let = doc
				} else {
					return fmt.Errorf("%s() let must be a document", methodName)
				}
			case "comment":
				op.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					op.WriteConcern = doc
				} else {
					return fmt.Errorf("%s() writeConcern must be a document", methodName)
				}
			default:
				return &UnsupportedOptionError{
					Method: methodName + "()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return fmt.Errorf("%s() takes at most 2 arguments", methodName)
	}
	return nil
}

func extractFindOneAndModifyArgs(op *Operation, methodName string, args []ast.Node, hasUpdate bool) error {
	minArgs := 1
	if hasUpdate {
		minArgs = 2
	}
	if len(args) < minArgs {
		if hasUpdate {
			return fmt.Errorf("%s() requires filter and update arguments", methodName)
		}
		return fmt.Errorf("%s() requires a filter argument", methodName)
	}

	// First argument: filter
	filter, err := requireDocument(args, 0, fmt.Sprintf("%s() filter", methodName))
	if err != nil {
		return err
	}
	op.Filter = filter

	optionsArgIdx := 1
	if hasUpdate {
		// Second argument: update/replacement
		if methodName == "findOneAndReplace" {
			replacement, err := requireDocument(args, 1, fmt.Sprintf("%s() replacement", methodName))
			if err != nil {
				return err
			}
			op.Replacement = replacement
		} else {
			switch u := args[1].(type) {
			case *ast.Document:
				update, err := convertDocument(u)
				if err != nil {
					return fmt.Errorf("invalid update: %w", err)
				}
				op.Update = update
			case *ast.Array:
				pipeline, err := convertArray(u)
				if err != nil {
					return fmt.Errorf("invalid update pipeline: %w", err)
				}
				op.Update = pipeline
			default:
				return fmt.Errorf("%s() update must be a document or array", methodName)
			}
		}
		optionsArgIdx = 2
	}

	// Options argument
	if len(args) > optionsArgIdx {
		opts, err := requireDocument(args, optionsArgIdx, fmt.Sprintf("%s() options", methodName))
		if err != nil {
			return err
		}
		if err := extractFindOneAndModifyOptions(op, methodName, opts); err != nil {
			return err
		}
	}

	maxArgs := optionsArgIdx + 1
	if len(args) > maxArgs {
		return fmt.Errorf("%s() takes at most %d arguments", methodName, maxArgs)
	}
	return nil
}

func extractFindOneAndModifyOptions(op *Operation, methodName string, opts bson.D) error {
	for _, opt := range opts {
		switch opt.Key {
		case "upsert":
			if methodName == "findOneAndDelete" {
				return &UnsupportedOptionError{Method: methodName + "()", Option: opt.Key}
			}
			if val, ok := opt.Value.(bool); ok {
				op.Upsert = &val
			} else {
				return fmt.Errorf("%s() upsert must be a boolean", methodName)
			}
		case "returnDocument":
			if val, ok := opt.Value.(string); ok {
				if val != "before" && val != "after" {
					return fmt.Errorf("%s() returnDocument must be 'before' or 'after'", methodName)
				}
				op.ReturnDocument = &val
			} else {
				return fmt.Errorf("%s() returnDocument must be a string", methodName)
			}
		case "projection":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Projection = doc
			} else {
				return fmt.Errorf("%s() projection must be a document", methodName)
			}
		case "sort":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Sort = doc
			} else {
				return fmt.Errorf("%s() sort must be a document", methodName)
			}
		case "hint":
			op.Hint = opt.Value
		case "collation":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Collation = doc
			} else {
				return fmt.Errorf("%s() collation must be a document", methodName)
			}
		case "arrayFilters":
			if methodName == "findOneAndDelete" || methodName == "findOneAndReplace" {
				return &UnsupportedOptionError{Method: methodName + "()", Option: opt.Key}
			}
			if arr, ok := opt.Value.(bson.A); ok {
				op.ArrayFilters = arr
			} else {
				return fmt.Errorf("%s() arrayFilters must be an array", methodName)
			}
		case "let":
			if doc, ok := opt.Value.(bson.D); ok {
				op.Let = doc
			} else {
				return fmt.Errorf("%s() let must be a document", methodName)
			}
		case "bypassDocumentValidation":
			if methodName == "findOneAndDelete" {
				return &UnsupportedOptionError{Method: methodName + "()", Option: opt.Key}
			}
			if val, ok := opt.Value.(bool); ok {
				op.BypassDocumentValidation = &val
			} else {
				return fmt.Errorf("%s() bypassDocumentValidation must be a boolean", methodName)
			}
		case "comment":
			op.Comment = opt.Value
		case "writeConcern":
			if doc, ok := opt.Value.(bson.D); ok {
				op.WriteConcern = doc
			} else {
				return fmt.Errorf("%s() writeConcern must be a document", methodName)
			}
		default:
			return &UnsupportedOptionError{
				Method: methodName + "()",
				Option: opt.Key,
			}
		}
	}
	return nil
}

func extractCreateIndexArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("createIndex() requires a key specification")
	}

	// First argument: keys document (required)
	keys, err := requireDocument(args, 0, "createIndex() key specification")
	if err != nil {
		return err
	}
	op.IndexKeys = keys

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "createIndex() options")
		if err != nil {
			return err
		}
		for _, opt := range options {
			switch opt.Key {
			case "name":
				if val, ok := opt.Value.(string); ok {
					op.IndexName = val
				} else {
					return fmt.Errorf("createIndex() name must be a string")
				}
			case "unique":
				if val, ok := opt.Value.(bool); ok {
					op.IndexUnique = &val
				} else {
					return fmt.Errorf("createIndex() unique must be a boolean")
				}
			case "sparse":
				if val, ok := opt.Value.(bool); ok {
					op.IndexSparse = &val
				} else {
					return fmt.Errorf("createIndex() sparse must be a boolean")
				}
			case "expireAfterSeconds":
				if val, ok := ToInt32(opt.Value); ok {
					op.IndexTTL = &val
				} else {
					return fmt.Errorf("createIndex() expireAfterSeconds must be a number")
				}
			case "background":
				if _, ok := opt.Value.(bool); !ok {
					return fmt.Errorf("createIndex() background must be a boolean")
				}
				// Silently ignore - deprecated and has no effect
			default:
				return &UnsupportedOptionError{
					Method: "createIndex()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return fmt.Errorf("createIndex() takes at most 2 arguments")
	}
	return nil
}

func extractCreateIndexesArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("createIndexes() requires an array of index specifications")
	}

	// First argument: array of index spec documents (required)
	arr, ok := args[0].(*ast.Array)
	if !ok {
		return fmt.Errorf("createIndexes() requires an array argument")
	}
	bsonArr, err := convertArray(arr)
	if err != nil {
		return fmt.Errorf("invalid index specifications: %w", err)
	}

	var specs []bson.D
	for i, elem := range bsonArr {
		doc, ok := elem.(bson.D)
		if !ok {
			return fmt.Errorf("createIndexes() element %d must be a document", i)
		}
		var keyDoc bson.D
		for _, field := range doc {
			if field.Key == "key" {
				keyDoc, _ = field.Value.(bson.D)
				break
			}
		}
		if len(keyDoc) == 0 {
			return fmt.Errorf("createIndexes() element %d must have a non-empty 'key' document", i)
		}
		specs = append(specs, doc)
	}
	op.IndexSpecs = specs

	if len(args) > 1 {
		return fmt.Errorf("createIndexes() takes exactly 1 argument")
	}
	return nil
}

func extractDropIndexArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("dropIndex() requires an index name or key specification")
	}

	switch a := args[0].(type) {
	case *ast.StringLiteral:
		op.IndexName = a.Value
	case *ast.Document:
		doc, err := convertDocument(a)
		if err != nil {
			return fmt.Errorf("invalid index specification: %w", err)
		}
		op.IndexKeys = doc
	default:
		return fmt.Errorf("dropIndex() argument must be a string or document")
	}
	return nil
}

func extractDropIndexesArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		// No argument means drop all indexes
		op.IndexName = "*"
		return nil
	}

	switch a := args[0].(type) {
	case *ast.StringLiteral:
		op.IndexName = a.Value
	case *ast.Array:
		arr, err := convertArray(a)
		if err != nil {
			return fmt.Errorf("invalid index names array: %w", err)
		}
		var indexNames []string
		for i, elem := range arr {
			name, ok := elem.(string)
			if !ok {
				return fmt.Errorf("dropIndexes() array element %d must be a string", i)
			}
			indexNames = append(indexNames, name)
		}
		op.IndexNames = indexNames
	default:
		return fmt.Errorf("dropIndexes() argument must be a string or array")
	}
	return nil
}

func extractRenameCollectionArgs(op *Operation, args []ast.Node) error {
	if len(args) == 0 {
		return fmt.Errorf("renameCollection() requires a new collection name")
	}

	// First argument: new collection name (required)
	newName, err := requireString(args, 0, "renameCollection() new name")
	if err != nil {
		return err
	}
	op.NewName = newName

	// Second argument: dropTarget boolean (optional)
	if len(args) >= 2 {
		boolNode, ok := args[1].(*ast.BoolLiteral)
		if !ok {
			return fmt.Errorf("renameCollection() dropTarget must be a boolean")
		}
		op.DropTarget = &boolNode.Value
	}

	if len(args) > 2 {
		return fmt.Errorf("renameCollection() takes at most 2 arguments")
	}
	return nil
}
