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

// executeShowCollections executes a show collections command.
func executeShowCollections(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	names, err := client.Database(database).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections failed: %w", err)
	}

	// Convert []string to []any
	values := make([]any, len(names))
	for i, name := range names {
		values[i] = name
	}

	return &Result{
		Operation: types.OpShowCollections,
		Value:     values,
	}, nil
}

// executeGetCollectionNames executes a db.getCollectionNames() command.
func executeGetCollectionNames(ctx context.Context, client *mongo.Client, database string) (*Result, error) {
	names, err := client.Database(database).ListCollectionNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list collections failed: %w", err)
	}

	// Convert []string to []any
	values := make([]any, len(names))
	for i, name := range names {
		values[i] = name
	}

	return &Result{
		Operation: types.OpGetCollectionNames,
		Value:     values,
	}, nil
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

	return &Result{
		Operation: types.OpGetCollectionInfos,
		Value:     values,
	}, nil
}
