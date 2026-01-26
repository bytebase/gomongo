package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/internal/translator"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// executeCreateIndex executes a db.collection.createIndex() command.
func executeCreateIndex(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	indexModel := mongo.IndexModel{
		Keys: op.IndexKeys,
	}

	// Build index options
	opts := options.Index()
	hasOptions := false

	if op.IndexName != "" {
		opts.SetName(op.IndexName)
		hasOptions = true
	}
	if op.IndexUnique != nil && *op.IndexUnique {
		opts.SetUnique(true)
		hasOptions = true
	}
	if op.IndexSparse != nil && *op.IndexSparse {
		opts.SetSparse(true)
		hasOptions = true
	}
	if op.IndexTTL != nil {
		opts.SetExpireAfterSeconds(*op.IndexTTL)
		hasOptions = true
	}

	if hasOptions {
		indexModel.Options = opts
	}

	indexName, err := collection.Indexes().CreateOne(ctx, indexModel)
	if err != nil {
		return nil, fmt.Errorf("createIndex failed: %w", err)
	}

	return &Result{
		Rows:     []string{indexName},
		RowCount: 1,
	}, nil
}

// executeDropIndex executes a db.collection.dropIndex() command.
func executeDropIndex(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	var err error
	if op.IndexName != "" {
		// Drop by index name
		err = collection.Indexes().DropOne(ctx, op.IndexName)
	} else if op.IndexKeys != nil {
		// Drop by index key specification - need to find the index name first
		cursor, listErr := collection.Indexes().List(ctx)
		if listErr != nil {
			return nil, fmt.Errorf("dropIndex failed: %w", listErr)
		}
		defer func() { _ = cursor.Close(ctx) }()

		var indexName string
		for cursor.Next(ctx) {
			var idx bson.M
			if decodeErr := cursor.Decode(&idx); decodeErr != nil {
				return nil, fmt.Errorf("dropIndex failed: %w", decodeErr)
			}
			// Check if keys match
			if keysMatch(idx["key"], op.IndexKeys) {
				indexName, _ = idx["name"].(string)
				break
			}
		}
		if indexName == "" {
			return nil, fmt.Errorf("dropIndex failed: index not found")
		}
		err = collection.Indexes().DropOne(ctx, indexName)
	} else {
		return nil, fmt.Errorf("dropIndex failed: no index specified")
	}

	if err != nil {
		return nil, fmt.Errorf("dropIndex failed: %w", err)
	}

	response := bson.M{"ok": 1}
	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// keysMatch compares two index key specifications.
func keysMatch(a any, b bson.D) bool {
	switch keys := a.(type) {
	case bson.D:
		if len(keys) != len(b) {
			return false
		}
		for i, elem := range keys {
			if elem.Key != b[i].Key {
				return false
			}
			// Compare values (could be int32, int64, string, etc.)
			if !valuesEqual(elem.Value, b[i].Value) {
				return false
			}
		}
		return true
	case bson.M:
		if len(keys) != len(b) {
			return false
		}
		for _, elem := range b {
			val, ok := keys[elem.Key]
			if !ok {
				return false
			}
			if !valuesEqual(val, elem.Value) {
				return false
			}
		}
		return true
	}
	return false
}

// valuesEqual compares two values that could be different numeric types.
func valuesEqual(a, b any) bool {
	// Convert both to int64 for comparison if they're numeric
	aInt, aOk := translator.ToInt64(a)
	bInt, bOk := translator.ToInt64(b)
	if aOk && bOk {
		return aInt == bInt
	}
	// Otherwise compare directly
	return a == b
}

// executeDropIndexes executes a db.collection.dropIndexes() command.
func executeDropIndexes(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	var err error
	if len(op.IndexNames) > 0 {
		// Drop each index in the array
		for _, name := range op.IndexNames {
			if dropErr := collection.Indexes().DropOne(ctx, name); dropErr != nil {
				return nil, fmt.Errorf("dropIndexes failed for index %q: %w", name, dropErr)
			}
		}
	} else if op.IndexName == "*" || op.IndexName == "" {
		// Drop all indexes (except _id)
		err = collection.Indexes().DropAll(ctx)
	} else {
		// Drop specific index
		err = collection.Indexes().DropOne(ctx, op.IndexName)
	}

	if err != nil {
		return nil, fmt.Errorf("dropIndexes failed: %w", err)
	}

	response := bson.M{"ok": 1}
	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeDrop executes a db.collection.drop() command.
func executeDrop(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	err := collection.Drop(ctx)
	if err != nil {
		return nil, fmt.Errorf("drop failed: %w", err)
	}

	return &Result{
		Rows:     []string{"true"},
		RowCount: 1,
	}, nil
}

// executeCreateCollection executes a db.createCollection() command.
func executeCreateCollection(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	db := client.Database(database)

	// Build create collection options
	opts := options.CreateCollection()
	if op.Capped != nil && *op.Capped {
		opts.SetCapped(true)
	}
	if op.CollectionSize != nil {
		opts.SetSizeInBytes(*op.CollectionSize)
	}
	if op.CollectionMax != nil {
		opts.SetMaxDocuments(*op.CollectionMax)
	}
	if op.Validator != nil {
		opts.SetValidator(op.Validator)
	}
	if op.ValidationLevel != "" {
		opts.SetValidationLevel(op.ValidationLevel)
	}
	if op.ValidationAction != "" {
		opts.SetValidationAction(op.ValidationAction)
	}

	err := db.CreateCollection(ctx, op.Collection, opts)
	if err != nil {
		return nil, fmt.Errorf("createCollection failed: %w", err)
	}

	response := bson.M{"ok": 1}
	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeDropDatabase executes a db.dropDatabase() command.
func executeDropDatabase(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	err := client.Database(database).Drop(ctx)
	if err != nil {
		return nil, fmt.Errorf("dropDatabase failed: %w", err)
	}

	response := bson.M{"ok": 1, "dropped": database}
	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}

// executeRenameCollection executes a db.collection.renameCollection() command.
func executeRenameCollection(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	// MongoDB's renameCollection command needs to be run on admin database
	// The source is in the form "database.collection"
	command := bson.D{
		{Key: "renameCollection", Value: database + "." + op.Collection},
		{Key: "to", Value: database + "." + op.NewName},
	}
	if op.DropTarget != nil && *op.DropTarget {
		command = append(command, bson.E{Key: "dropTarget", Value: true})
	}

	result := client.Database("admin").RunCommand(ctx, command)
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("renameCollection failed: %w", err)
	}

	response := bson.M{"ok": 1}
	jsonBytes, err := bson.MarshalExtJSONIndent(response, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
	}, nil
}
