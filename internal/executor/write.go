package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/internal/translator"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

// convertWriteConcern converts a bson.D writeConcern document to *writeconcern.WriteConcern.
// Note: wtimeout is not supported in MongoDB Go driver v2, so it is ignored if present.
func convertWriteConcern(doc bson.D) *writeconcern.WriteConcern {
	if doc == nil {
		return nil
	}
	wc := &writeconcern.WriteConcern{}
	for _, elem := range doc {
		switch elem.Key {
		case "w":
			// w can be an int or string like "majority"
			wc.W = elem.Value
		case "j":
			if v, ok := elem.Value.(bool); ok {
				wc.Journal = &v
			}
		case "wtimeout":
			// wtimeout is not supported in MongoDB Go driver v2
			// The field was removed from the WriteConcern struct
			// We silently ignore it to maintain compatibility with mongosh syntax
		}
	}
	return wc
}

// getCollection returns a collection, optionally cloned with a custom write concern.
func getCollection(client *mongo.Client, database, collection string, wc bson.D) *mongo.Collection {
	coll := client.Database(database).Collection(collection)
	if wc != nil {
		coll = coll.Clone(options.Collection().SetWriteConcern(convertWriteConcern(wc)))
	}
	return coll
}

// convertCollation converts a bson.D collation document to *options.Collation.
func convertCollation(doc bson.D) *options.Collation {
	if doc == nil {
		return nil
	}
	coll := &options.Collation{}
	for _, elem := range doc {
		switch elem.Key {
		case "locale":
			if v, ok := elem.Value.(string); ok {
				coll.Locale = v
			}
		case "caseLevel":
			if v, ok := elem.Value.(bool); ok {
				coll.CaseLevel = v
			}
		case "caseFirst":
			if v, ok := elem.Value.(string); ok {
				coll.CaseFirst = v
			}
		case "strength":
			if v, ok := elem.Value.(int32); ok {
				coll.Strength = int(v)
			} else if v, ok := elem.Value.(int64); ok {
				coll.Strength = int(v)
			}
		case "numericOrdering":
			if v, ok := elem.Value.(bool); ok {
				coll.NumericOrdering = v
			}
		case "alternate":
			if v, ok := elem.Value.(string); ok {
				coll.Alternate = v
			}
		case "maxVariable":
			if v, ok := elem.Value.(string); ok {
				coll.MaxVariable = v
			}
		case "normalization":
			if v, ok := elem.Value.(bool); ok {
				coll.Normalization = v
			}
		case "backwards":
			if v, ok := elem.Value.(bool); ok {
				coll.Backwards = v
			}
		}
	}
	return coll
}

// executeInsertOne executes an insertOne operation.
func executeInsertOne(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.InsertOne()
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	result, err := collection.InsertOne(ctx, op.Document, opts)
	if err != nil {
		return nil, fmt.Errorf("insertOne failed: %w", err)
	}

	// Build response document matching mongosh format
	response := bson.M{
		"acknowledged": true,
		"insertedId":   result.InsertedID,
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeInsertMany executes an insertMany operation.
func executeInsertMany(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	// Convert []bson.D to []any for InsertMany
	docs := make([]any, len(op.Documents))
	for i, doc := range op.Documents {
		docs[i] = doc
	}

	opts := options.InsertMany()
	if op.Ordered != nil {
		opts.SetOrdered(*op.Ordered)
	}
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	result, err := collection.InsertMany(ctx, docs, opts)
	if err != nil {
		return nil, fmt.Errorf("insertMany failed: %w", err)
	}

	// Build response document matching mongosh format
	response := bson.M{
		"acknowledged": true,
		"insertedIds":  result.InsertedIDs,
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeUpdateOne executes an updateOne operation.
func executeUpdateOne(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.UpdateOne()
	if op.Upsert != nil && *op.Upsert {
		opts.SetUpsert(true)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.ArrayFilters != nil {
		opts.SetArrayFilters(op.ArrayFilters)
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}

	result, err := collection.UpdateOne(ctx, op.Filter, op.Update, opts)
	if err != nil {
		return nil, fmt.Errorf("updateOne failed: %w", err)
	}

	// Build response document matching mongosh format
	response := bson.M{
		"acknowledged":  true,
		"matchedCount":  result.MatchedCount,
		"modifiedCount": result.ModifiedCount,
	}
	if result.UpsertedID != nil {
		response["upsertedId"] = result.UpsertedID
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeUpdateMany executes an updateMany operation.
func executeUpdateMany(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.UpdateMany()
	if op.Upsert != nil && *op.Upsert {
		opts.SetUpsert(true)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.ArrayFilters != nil {
		opts.SetArrayFilters(op.ArrayFilters)
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	result, err := collection.UpdateMany(ctx, op.Filter, op.Update, opts)
	if err != nil {
		return nil, fmt.Errorf("updateMany failed: %w", err)
	}

	response := bson.M{
		"acknowledged":  true,
		"matchedCount":  result.MatchedCount,
		"modifiedCount": result.ModifiedCount,
	}
	if result.UpsertedID != nil {
		response["upsertedId"] = result.UpsertedID
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeReplaceOne executes a replaceOne operation.
func executeReplaceOne(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.Replace()
	if op.Upsert != nil && *op.Upsert {
		opts.SetUpsert(true)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}

	result, err := collection.ReplaceOne(ctx, op.Filter, op.Replacement, opts)
	if err != nil {
		return nil, fmt.Errorf("replaceOne failed: %w", err)
	}

	response := bson.M{
		"acknowledged":  true,
		"matchedCount":  result.MatchedCount,
		"modifiedCount": result.ModifiedCount,
	}
	if result.UpsertedID != nil {
		response["upsertedId"] = result.UpsertedID
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeDeleteOne executes a deleteOne operation.
func executeDeleteOne(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.DeleteOne()
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	result, err := collection.DeleteOne(ctx, op.Filter, opts)
	if err != nil {
		return nil, fmt.Errorf("deleteOne failed: %w", err)
	}

	response := bson.M{
		"acknowledged": true,
		"deletedCount": result.DeletedCount,
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeDeleteMany executes a deleteMany operation.
func executeDeleteMany(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.DeleteMany()
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	result, err := collection.DeleteMany(ctx, op.Filter, opts)
	if err != nil {
		return nil, fmt.Errorf("deleteMany failed: %w", err)
	}

	response := bson.M{
		"acknowledged": true,
		"deletedCount": result.DeletedCount,
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeFindOneAndUpdate executes a findOneAndUpdate operation.
func executeFindOneAndUpdate(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.FindOneAndUpdate()
	if op.Upsert != nil && *op.Upsert {
		opts.SetUpsert(true)
	}
	if op.ReturnDocument != nil && *op.ReturnDocument == "after" {
		opts.SetReturnDocument(options.After)
	}
	if op.Projection != nil {
		opts.SetProjection(op.Projection)
	}
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.ArrayFilters != nil {
		opts.SetArrayFilters(op.ArrayFilters)
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	var doc bson.M
	err := collection.FindOneAndUpdate(ctx, op.Filter, op.Update, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &Result{
				Rows:     []string{"null"},
				RowCount: 1,
			}, nil
		}
		return nil, fmt.Errorf("findOneAndUpdate failed: %w", err)
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeFindOneAndReplace executes a findOneAndReplace operation.
func executeFindOneAndReplace(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.FindOneAndReplace()
	if op.Upsert != nil && *op.Upsert {
		opts.SetUpsert(true)
	}
	if op.ReturnDocument != nil && *op.ReturnDocument == "after" {
		opts.SetReturnDocument(options.After)
	}
	if op.Projection != nil {
		opts.SetProjection(op.Projection)
	}
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.BypassDocumentValidation != nil && *op.BypassDocumentValidation {
		opts.SetBypassDocumentValidation(true)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	var doc bson.M
	err := collection.FindOneAndReplace(ctx, op.Filter, op.Replacement, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &Result{
				Rows:     []string{"null"},
				RowCount: 1,
			}, nil
		}
		return nil, fmt.Errorf("findOneAndReplace failed: %w", err)
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeFindOneAndDelete executes a findOneAndDelete operation.
func executeFindOneAndDelete(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := getCollection(client, database, op.Collection, op.WriteConcern)

	opts := options.FindOneAndDelete()
	if op.Projection != nil {
		opts.SetProjection(op.Projection)
	}
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Collation != nil {
		opts.SetCollation(convertCollation(op.Collation))
	}
	if op.Let != nil {
		opts.SetLet(op.Let)
	}
	if op.Comment != nil {
		opts.SetComment(op.Comment)
	}

	var doc bson.M
	err := collection.FindOneAndDelete(ctx, op.Filter, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &Result{
				Rows:     []string{"null"},
				RowCount: 1,
			}, nil
		}
		return nil, fmt.Errorf("findOneAndDelete failed: %w", err)
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}
