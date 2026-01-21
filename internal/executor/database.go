package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/internal/translator"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

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
func executeGetCollectionInfos(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.ListCollections()
	if op.NameOnly != nil {
		opts.SetNameOnly(*op.NameOnly)
	}
	if op.AuthorizedCollections != nil {
		opts.SetAuthorizedCollections(*op.AuthorizedCollections)
	}

	cursor, err := client.Database(database).ListCollections(ctx, filter, opts)
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
