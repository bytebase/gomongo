package gomongo

import (
	"context"

	"github.com/bytebase/gomongo/types"
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
//
// The Value slice contains the operation's return data. The element type varies by operation:
//
//   - OpFind, OpAggregate, OpGetIndexes, OpGetCollectionInfos: each element is bson.D (document)
//   - OpFindOne, OpFindOneAndUpdate, OpFindOneAndReplace, OpFindOneAndDelete: 0 or 1 element of bson.D
//   - OpCountDocuments, OpEstimatedDocumentCount: single element of int64
//   - OpDistinct: elements are the distinct values (various types)
//   - OpShowDatabases, OpShowCollections, OpGetCollectionNames: each element is string
//   - OpInsertOne, OpInsertMany, OpUpdateOne, OpUpdateMany, OpReplaceOne, OpDeleteOne, OpDeleteMany: single bson.D with operation result
//   - OpCreateIndex: single element of string (index name)
//   - OpDropIndex, OpDropIndexes, OpCreateCollection, OpDropDatabase, OpRenameCollection: single bson.D with {ok: 1}
//   - OpDrop: single element of bool (true)
type Result struct {
	Operation types.OperationType
	Value     []any
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
// Returns a Result containing the operation type and native Go values.
// Use Result.Operation to determine the expected type of elements in Result.Value.
func (c *Client) Execute(ctx context.Context, database, statement string, opts ...ExecuteOption) (*Result, error) {
	cfg := &executeConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	return execute(ctx, c.client, database, statement, cfg.maxRows)
}
