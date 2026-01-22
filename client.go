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

// executeConfig holds configuration for Execute.
type executeConfig struct {
	maxRows *int64
}

// ExecuteOption configures Execute behavior.
type ExecuteOption func(*executeConfig)

// WithMaxRows limits the maximum number of rows returned by find() and
// countDocuments() operations. If the query includes .limit(N), the effective
// limit is min(N, maxRows). Aggregate operations are not affected.
func WithMaxRows(n int64) ExecuteOption {
	return func(c *executeConfig) {
		c.maxRows = &n
	}
}

// Execute parses and executes a MongoDB shell statement.
// Returns results as Extended JSON (Relaxed) strings.
func (c *Client) Execute(ctx context.Context, database, statement string, opts ...ExecuteOption) (*Result, error) {
	cfg := &executeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return execute(ctx, c.client, database, statement, cfg.maxRows)
}
