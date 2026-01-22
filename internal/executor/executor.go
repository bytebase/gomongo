package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/internal/translator"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Result represents query execution results.
type Result struct {
	Rows      []string
	RowCount  int
	Statement string
}

// Execute executes a parsed operation against MongoDB.
func Execute(ctx context.Context, client *mongo.Client, database string, op *translator.Operation, statement string, maxRows *int64) (*Result, error) {
	switch op.OpType {
	case translator.OpFind:
		return executeFind(ctx, client, database, op, maxRows)
	case translator.OpFindOne:
		return executeFindOne(ctx, client, database, op)
	case translator.OpAggregate:
		return executeAggregate(ctx, client, database, op)
	case translator.OpShowDatabases:
		return executeShowDatabases(ctx, client)
	case translator.OpShowCollections:
		return executeShowCollections(ctx, client, database)
	case translator.OpGetCollectionNames:
		return executeGetCollectionNames(ctx, client, database)
	case translator.OpGetCollectionInfos:
		return executeGetCollectionInfos(ctx, client, database, op)
	case translator.OpGetIndexes:
		return executeGetIndexes(ctx, client, database, op)
	case translator.OpCountDocuments:
		return executeCountDocuments(ctx, client, database, op, maxRows)
	case translator.OpEstimatedDocumentCount:
		return executeEstimatedDocumentCount(ctx, client, database, op)
	case translator.OpDistinct:
		return executeDistinct(ctx, client, database, op)
	case translator.OpInsertOne:
		return executeInsertOne(ctx, client, database, op)
	case translator.OpInsertMany:
		return executeInsertMany(ctx, client, database, op)
	case translator.OpUpdateOne:
		return executeUpdateOne(ctx, client, database, op)
	case translator.OpUpdateMany:
		return executeUpdateMany(ctx, client, database, op)
	case translator.OpReplaceOne:
		return executeReplaceOne(ctx, client, database, op)
	case translator.OpDeleteOne:
		return executeDeleteOne(ctx, client, database, op)
	case translator.OpDeleteMany:
		return executeDeleteMany(ctx, client, database, op)
	case translator.OpFindOneAndUpdate:
		return executeFindOneAndUpdate(ctx, client, database, op)
	case translator.OpFindOneAndReplace:
		return executeFindOneAndReplace(ctx, client, database, op)
	case translator.OpFindOneAndDelete:
		return executeFindOneAndDelete(ctx, client, database, op)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", statement)
	}
}
