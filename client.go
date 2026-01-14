package gomongo

import (
	"context"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Client wraps a MongoDB client and provides query execution.
type Client struct {
	client *mongo.Client
}

// NewClient creates a new gomongo client from an existing MongoDB client.
func NewClient(client *mongo.Client) *Client {
	return &Client{client: client}
}

// Result represents query execution results.
type Result struct {
	Rows      []string
	RowCount  int
	Statement string
}

// Execute parses and executes a MongoDB shell statement.
// Returns results as Extended JSON (Relaxed) strings.
func (c *Client) Execute(ctx context.Context, database, statement string) (*Result, error) {
	return execute(ctx, c.client, database, statement)
}
