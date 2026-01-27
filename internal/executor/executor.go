package executor

import (
	"context"
	"fmt"

	"github.com/bytebase/gomongo/internal/translator"
	"github.com/bytebase/gomongo/types"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Result represents query execution results.
type Result struct {
	Operation types.OperationType
	Value     []any // slice of results; element types vary by operation
}

// Execute executes a parsed operation against MongoDB.
func Execute(ctx context.Context, client *mongo.Client, database string, op *translator.Operation, statement string, maxRows *int64) (*Result, error) {
	switch op.OpType {
	case types.OpFind:
		return executeFind(ctx, client, database, op, maxRows)
	case types.OpFindOne:
		return executeFindOne(ctx, client, database, op)
	case types.OpAggregate:
		return executeAggregate(ctx, client, database, op)
	case types.OpShowDatabases:
		return executeShowDatabases(ctx, client)
	case types.OpShowCollections:
		return executeShowCollections(ctx, client, database)
	case types.OpGetCollectionNames:
		return executeGetCollectionNames(ctx, client, database)
	case types.OpGetCollectionInfos:
		return executeGetCollectionInfos(ctx, client, database, op)
	case types.OpGetIndexes:
		return executeGetIndexes(ctx, client, database, op)
	case types.OpCountDocuments:
		return executeCountDocuments(ctx, client, database, op, maxRows)
	case types.OpEstimatedDocumentCount:
		return executeEstimatedDocumentCount(ctx, client, database, op)
	case types.OpDistinct:
		return executeDistinct(ctx, client, database, op)
	case types.OpInsertOne:
		return executeInsertOne(ctx, client, database, op)
	case types.OpInsertMany:
		return executeInsertMany(ctx, client, database, op)
	case types.OpUpdateOne:
		return executeUpdateOne(ctx, client, database, op)
	case types.OpUpdateMany:
		return executeUpdateMany(ctx, client, database, op)
	case types.OpReplaceOne:
		return executeReplaceOne(ctx, client, database, op)
	case types.OpDeleteOne:
		return executeDeleteOne(ctx, client, database, op)
	case types.OpDeleteMany:
		return executeDeleteMany(ctx, client, database, op)
	case types.OpFindOneAndUpdate:
		return executeFindOneAndUpdate(ctx, client, database, op)
	case types.OpFindOneAndReplace:
		return executeFindOneAndReplace(ctx, client, database, op)
	case types.OpFindOneAndDelete:
		return executeFindOneAndDelete(ctx, client, database, op)
	// M3: Administrative Operations
	case types.OpCreateIndex:
		return executeCreateIndex(ctx, client, database, op)
	case types.OpDropIndex:
		return executeDropIndex(ctx, client, database, op)
	case types.OpDropIndexes:
		return executeDropIndexes(ctx, client, database, op)
	case types.OpDrop:
		return executeDrop(ctx, client, database, op)
	case types.OpCreateCollection:
		return executeCreateCollection(ctx, client, database, op)
	case types.OpDropDatabase:
		return executeDropDatabase(ctx, client, database)
	case types.OpRenameCollection:
		return executeRenameCollection(ctx, client, database, op)
	default:
		return nil, fmt.Errorf("unsupported operation: %s", statement)
	}
}
