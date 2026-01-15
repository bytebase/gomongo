package gomongo

import (
	"context"
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
	case opShowDatabases:
		return executeShowDatabases(ctx, client)
	case opShowCollections:
		return executeShowCollections(ctx, client, database)
	case opGetCollectionNames:
		return executeGetCollectionNames(ctx, client, database)
	default:
		return nil, &UnsupportedOperationError{
			Operation: statement,
			Hint:      "could not determine operation type",
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
