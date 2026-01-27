package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/types"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// executeShowDatabases executes a show dbs/databases command.
func executeShowDatabases(ctx context.Context, client *mongo.Client) (*Result, error) {
	names, err := client.ListDatabaseNames(ctx, bson.D{})
	if err != nil {
		return nil, fmt.Errorf("list databases failed: %w", err)
	}

	// Convert []string to []any
	values := make([]any, len(names))
	for i, name := range names {
		values[i] = name
	}

	return &Result{
		Operation: types.OpShowDatabases,
		Value:     values,
	}, nil
}
