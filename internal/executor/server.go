package executor

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

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
