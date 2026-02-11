package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/internal/translator"
	"github.com/bytebase/gomongo/types"
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
		Operation: types.OpCreateIndex,
		Value:     []any{indexName},
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

	response := bson.D{{Key: "ok", Value: int32(1)}}

	return &Result{
		Operation: types.OpDropIndex,
		Value:     []any{response},
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

	response := bson.D{{Key: "ok", Value: int32(1)}}

	return &Result{
		Operation: types.OpDropIndexes,
		Value:     []any{response},
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
		Operation: types.OpDrop,
		Value:     []any{true},
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

	response := bson.D{{Key: "ok", Value: int32(1)}}

	return &Result{
		Operation: types.OpCreateCollection,
		Value:     []any{response},
	}, nil
}

// executeDropDatabase executes a db.dropDatabase() command.
func executeDropDatabase(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	err := client.Database(database).Drop(ctx)
	if err != nil {
		return nil, fmt.Errorf("dropDatabase failed: %w", err)
	}

	response := bson.D{
		{Key: "ok", Value: int32(1)},
		{Key: "dropped", Value: database},
	}

	return &Result{
		Operation: types.OpDropDatabase,
		Value:     []any{response},
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

	response := bson.D{{Key: "ok", Value: int32(1)}}

	return &Result{
		Operation: types.OpRenameCollection,
		Value:     []any{response},
	}, nil
}

// executeCreateIndexes executes a db.collection.createIndexes() command.
func executeCreateIndexes(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	var models []mongo.IndexModel
	for _, spec := range op.IndexSpecs {
		model := mongo.IndexModel{}
		opts := options.Index()
		hasOptions := false

		for _, field := range spec {
			switch field.Key {
			case "key":
				keys, ok := field.Value.(bson.D)
				if !ok {
					return nil, fmt.Errorf("createIndexes failed: 'key' must be a document")
				}
				model.Keys = keys
			case "name":
				name, ok := field.Value.(string)
				if !ok {
					return nil, fmt.Errorf("createIndexes failed: 'name' must be a string")
				}
				opts.SetName(name)
				hasOptions = true
			case "unique":
				unique, ok := field.Value.(bool)
				if !ok {
					return nil, fmt.Errorf("createIndexes failed: 'unique' must be a bool")
				}
				if unique {
					opts.SetUnique(true)
					hasOptions = true
				}
			case "sparse":
				sparse, ok := field.Value.(bool)
				if !ok {
					return nil, fmt.Errorf("createIndexes failed: 'sparse' must be a bool")
				}
				if sparse {
					opts.SetSparse(true)
					hasOptions = true
				}
			case "expireAfterSeconds":
				val, ok := translator.ToInt32(field.Value)
				if !ok {
					return nil, fmt.Errorf("createIndexes failed: 'expireAfterSeconds' must be a number")
				}
				opts.SetExpireAfterSeconds(val)
				hasOptions = true
			}
		}

		if hasOptions {
			model.Options = opts
		}
		models = append(models, model)
	}

	names, err := collection.Indexes().CreateMany(ctx, models)
	if err != nil {
		return nil, fmt.Errorf("createIndexes failed: %w", err)
	}

	values := make([]any, len(names))
	for i, name := range names {
		values[i] = name
	}

	return &Result{
		Operation: types.OpCreateIndexes,
		Value:     values,
	}, nil
}

// runCommand executes a RunCommand and returns the result as bson.D.
func runCommand(ctx context.Context, db *mongo.Database, command bson.D) (bson.D, error) {
	var result bson.D
	if err := db.RunCommand(ctx, command).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}

// runCollStats executes the collStats command and returns the full result.
func runCollStats(ctx context.Context, client *mongo.Client, database, collection string) (bson.D, error) {
	return runCommand(ctx, client.Database(database), bson.D{{Key: "collStats", Value: collection}})
}

// findField finds a field value in a bson.D by key.
func findField(doc bson.D, key string) any {
	for _, elem := range doc {
		if elem.Key == key {
			return elem.Value
		}
	}
	return nil
}

// executeDbStats executes a db.stats() command.
func executeDbStats(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "dbStats", Value: int32(1)}})
	if err != nil {
		return nil, fmt.Errorf("dbStats failed: %w", err)
	}
	return &Result{Operation: types.OpDbStats, Value: []any{result}}, nil
}

// executeCollectionStats executes a db.collection.stats() command.
func executeCollectionStats(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	result, err := runCollStats(ctx, client, database, op.Collection)
	if err != nil {
		return nil, fmt.Errorf("collStats failed: %w", err)
	}
	return &Result{Operation: types.OpCollectionStats, Value: []any{result}}, nil
}

// executeServerStatus executes a db.serverStatus() command.
func executeServerStatus(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "serverStatus", Value: int32(1)}})
	if err != nil {
		return nil, fmt.Errorf("serverStatus failed: %w", err)
	}
	return &Result{Operation: types.OpServerStatus, Value: []any{result}}, nil
}

// executeServerBuildInfo executes a db.serverBuildInfo() command.
func executeServerBuildInfo(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "buildInfo", Value: int32(1)}})
	if err != nil {
		return nil, fmt.Errorf("serverBuildInfo failed: %w", err)
	}
	return &Result{Operation: types.OpServerBuildInfo, Value: []any{result}}, nil
}

// executeDbVersion executes a db.version() command.
func executeDbVersion(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "buildInfo", Value: int32(1)}})
	if err != nil {
		return nil, fmt.Errorf("version failed: %w", err)
	}
	version, ok := findField(result, "version").(string)
	if !ok {
		return nil, fmt.Errorf("version failed: version field missing or not a string in buildInfo result")
	}
	return &Result{Operation: types.OpDbVersion, Value: []any{version}}, nil
}

// executeHostInfo executes a db.hostInfo() command.
func executeHostInfo(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "hostInfo", Value: int32(1)}})
	if err != nil {
		return nil, fmt.Errorf("hostInfo failed: %w", err)
	}
	return &Result{Operation: types.OpHostInfo, Value: []any{result}}, nil
}

// executeListCommands executes a db.listCommands() command.
func executeListCommands(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "listCommands", Value: int32(1)}})
	if err != nil {
		return nil, fmt.Errorf("listCommands failed: %w", err)
	}
	return &Result{Operation: types.OpListCommands, Value: []any{result}}, nil
}

// executeDataSize executes a db.collection.dataSize() command.
func executeDataSize(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	stats, err := runCollStats(ctx, client, database, op.Collection)
	if err != nil {
		return nil, fmt.Errorf("dataSize failed: %w", err)
	}
	size := findField(stats, "size")
	if size == nil {
		size = int32(0)
	}
	return &Result{Operation: types.OpDataSize, Value: []any{size}}, nil
}

// executeStorageSize executes a db.collection.storageSize() command.
func executeStorageSize(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	stats, err := runCollStats(ctx, client, database, op.Collection)
	if err != nil {
		return nil, fmt.Errorf("storageSize failed: %w", err)
	}
	storageSize := findField(stats, "storageSize")
	if storageSize == nil {
		storageSize = int32(0)
	}
	return &Result{Operation: types.OpStorageSize, Value: []any{storageSize}}, nil
}

// executeTotalIndexSize executes a db.collection.totalIndexSize() command.
func executeTotalIndexSize(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	stats, err := runCollStats(ctx, client, database, op.Collection)
	if err != nil {
		return nil, fmt.Errorf("totalIndexSize failed: %w", err)
	}
	totalIndexSize := findField(stats, "totalIndexSize")
	if totalIndexSize == nil {
		totalIndexSize = int32(0)
	}
	return &Result{Operation: types.OpTotalIndexSize, Value: []any{totalIndexSize}}, nil
}

// executeTotalSize executes a db.collection.totalSize() command.
func executeTotalSize(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	stats, err := runCollStats(ctx, client, database, op.Collection)
	if err != nil {
		return nil, fmt.Errorf("totalSize failed: %w", err)
	}
	storage, _ := translator.ToInt64(findField(stats, "storageSize"))
	indexSize, _ := translator.ToInt64(findField(stats, "totalIndexSize"))
	return &Result{Operation: types.OpTotalSize, Value: []any{storage + indexSize}}, nil
}

// executeIsCapped executes a db.collection.isCapped() command.
func executeIsCapped(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	stats, err := runCollStats(ctx, client, database, op.Collection)
	if err != nil {
		return nil, fmt.Errorf("isCapped failed: %w", err)
	}
	capped, _ := findField(stats, "capped").(bool)
	return &Result{Operation: types.OpIsCapped, Value: []any{capped}}, nil
}

// executeValidate executes a db.collection.validate() command.
func executeValidate(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	result, err := runCommand(ctx, client.Database(database), bson.D{{Key: "validate", Value: op.Collection}})
	if err != nil {
		return nil, fmt.Errorf("validate failed: %w", err)
	}
	return &Result{Operation: types.OpValidate, Value: []any{result}}, nil
}

// executeLatencyStats executes a db.collection.latencyStats() command via $collStats aggregation.
func executeLatencyStats(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	pipeline := bson.A{
		bson.D{{Key: "$collStats", Value: bson.D{
			{Key: "latencyStats", Value: bson.D{{Key: "histograms", Value: true}}},
		}}},
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("latencyStats failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var values []any
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}
		values = append(values, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{Operation: types.OpLatencyStats, Value: values}, nil
}
