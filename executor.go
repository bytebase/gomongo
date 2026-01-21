package gomongo

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// execute parses and executes a MongoDB shell statement.
func execute(ctx context.Context, client *mongo.Client, database, statement string) (*Result, error) {
	// Parse the statement
	tree, parseErrors := parseMongoShell(statement)
	if len(parseErrors) > 0 {
		err := parseErrors[0]
		return nil, &ParseError{
			Line:    err.Line,
			Column:  err.Column,
			Message: err.Message,
		}
	}

	// Extract operation from parse tree
	visitor := newMongoShellVisitor()
	visitor.Visit(tree)
	if visitor.err != nil {
		return nil, visitor.err
	}

	// Execute operation
	return executeOperation(ctx, client, database, visitor.operation, statement)
}

// parseMongoShell parses a MongoDB shell statement and returns the parse tree.
func parseMongoShell(statement string) (mongodb.IProgramContext, []*mongodb.MongoShellParseError) {
	is := antlr.NewInputStream(statement)
	lexer := mongodb.NewMongoShellLexer(is)

	errorListener := mongodb.NewMongoShellErrorListener()
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := mongodb.NewMongoShellParser(stream)

	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)

	parser.BuildParseTrees = true
	tree := parser.Program()

	return tree, errorListener.Errors
}

// executeOperation executes a parsed MongoDB operation.
func executeOperation(ctx context.Context, client *mongo.Client, database string, op *mongoOperation, statement string) (*Result, error) {
	switch op.opType {
	case opFind:
		return executeFind(ctx, client, database, op)
	case opFindOne:
		return executeFindOne(ctx, client, database, op)
	case opAggregate:
		return executeAggregate(ctx, client, database, op)
	case opShowDatabases:
		return executeShowDatabases(ctx, client)
	case opShowCollections:
		return executeShowCollections(ctx, client, database)
	case opGetCollectionNames:
		return executeGetCollectionNames(ctx, client, database)
	case opGetCollectionInfos:
		return executeGetCollectionInfos(ctx, client, database, op)
	case opGetIndexes:
		return executeGetIndexes(ctx, client, database, op)
	case opCountDocuments:
		return executeCountDocuments(ctx, client, database, op)
	case opEstimatedDocumentCount:
		return executeEstimatedDocumentCount(ctx, client, database, op)
	case opDistinct:
		return executeDistinct(ctx, client, database, op)
	default:
		return nil, &UnsupportedOperationError{
			Operation: statement,
		}
	}
}

// executeFind executes a find operation.
func executeFind(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	filter := op.filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.Find()
	if op.sort != nil {
		opts.SetSort(op.sort)
	}
	if op.limit != nil {
		opts.SetLimit(*op.limit)
	}
	if op.skip != nil {
		opts.SetSkip(*op.skip)
	}
	if op.projection != nil {
		opts.SetProjection(op.projection)
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		// Marshal to Extended JSON (Relaxed)
		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeAggregate executes an aggregation pipeline.
func executeAggregate(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	pipeline := op.pipeline
	if pipeline == nil {
		pipeline = bson.A{}
	}

	cursor, err := collection.Aggregate(ctx, pipeline)
	if err != nil {
		return nil, fmt.Errorf("aggregate failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeFindOne executes a findOne operation.
func executeFindOne(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	filter := op.filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.FindOne()
	if op.sort != nil {
		opts.SetSort(op.sort)
	}
	if op.skip != nil {
		opts.SetSkip(*op.skip)
	}
	if op.projection != nil {
		opts.SetProjection(op.projection)
	}

	var doc bson.M
	err := collection.FindOne(ctx, filter, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &Result{
				Rows:     nil,
				RowCount: 0,
			}, nil
		}
		return nil, fmt.Errorf("findOne failed: %w", err)
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

// executeShowDatabases executes a show dbs/databases command.
func executeShowDatabases(ctx context.Context, client *mongo.Client) (*Result, error) {
	names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list databases failed: %w", err)
	}

	rows := make([]string, len(names))
	copy(rows, names)

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeShowCollections executes a show collections command.
func executeShowCollections(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	names, err := client.Database(database).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections failed: %w", err)
	}

	rows := make([]string, len(names))
	copy(rows, names)

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeGetCollectionNames executes a db.getCollectionNames() command.
func executeGetCollectionNames(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	return executeShowCollections(ctx, client, database)
}

// executeGetCollectionInfos executes a db.getCollectionInfos() command.
func executeGetCollectionInfos(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	filter := op.filter
	if filter == nil {
		filter = bson.D{}
	}

	cursor, err := client.Database(database).ListCollections(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("list collections failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeGetIndexes executes a db.collection.getIndexes() command.
func executeGetIndexes(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list indexes failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeCountDocuments executes a db.collection.countDocuments() command.
func executeCountDocuments(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	filter := op.filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.Count()
	if op.hint != nil {
		opts.SetHint(op.hint)
	}
	if op.limit != nil {
		opts.SetLimit(*op.limit)
	}
	if op.skip != nil {
		opts.SetSkip(*op.skip)
	}

	count, err := collection.CountDocuments(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("count documents failed: %w", err)
	}

	return &Result{
		Rows:     []string{fmt.Sprintf("%d", count)},
		RowCount: 1,
	}, nil
}

// executeEstimatedDocumentCount executes a db.collection.estimatedDocumentCount() command.
func executeEstimatedDocumentCount(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	count, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("estimated document count failed: %w", err)
	}

	return &Result{
		Rows:     []string{fmt.Sprintf("%d", count)},
		RowCount: 1,
	}, nil
}

// executeDistinct executes a db.collection.distinct() command.
func executeDistinct(ctx context.Context, client *mongo.Client, database string, op *mongoOperation) (*Result, error) {
	collection := client.Database(database).Collection(op.collection)

	filter := op.filter
	if filter == nil {
		filter = bson.D{}
	}

	result := collection.Distinct(ctx, op.distinctField, filter)
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("distinct failed: %w", err)
	}

	var values []any
	if err := result.Decode(&values); err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}

	var rows []string
	for _, val := range values {
		jsonBytes, err := marshalValue(val)
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// marshalValue marshals a value to JSON.
// bson.MarshalExtJSONIndent only works for documents/arrays at top level,
// so we use encoding/json for primitive values (strings, numbers, booleans).
func marshalValue(val any) ([]byte, error) {
	switch v := val.(type) {
	case bson.M, bson.D, map[string]any:
		return bson.MarshalExtJSONIndent(v, false, false, "", "  ")
	case bson.A, []any:
		return bson.MarshalExtJSONIndent(v, false, false, "", "  ")
	default:
		return json.Marshal(v)
	}
}
