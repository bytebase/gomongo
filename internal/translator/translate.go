package translator

import (
	"fmt"
	"strings"

	"github.com/bytebase/gomongo/types"
	"github.com/bytebase/omni/mongo/ast"
)

func translateNode(node ast.Node) (*Operation, error) {
	op := &Operation{OpType: types.OpUnknown}
	switch n := node.(type) {
	case *ast.CollectionStatement:
		return translateCollectionStatement(op, n)
	case *ast.DatabaseStatement:
		return translateDatabaseStatement(op, n)
	case *ast.ShowCommand:
		return translateShowCommand(op, n)
	default:
		return nil, &UnsupportedOperationError{Operation: fmt.Sprintf("%T", node)}
	}
}

func translateShowCommand(op *Operation, cmd *ast.ShowCommand) (*Operation, error) {
	switch cmd.Target {
	case "dbs", "databases":
		op.OpType = types.OpShowDatabases
	case "collections", "tables":
		op.OpType = types.OpShowCollections
	default:
		return nil, &UnsupportedOperationError{Operation: "show " + cmd.Target}
	}
	return op, nil
}

func translateDatabaseStatement(op *Operation, stmt *ast.DatabaseStatement) (*Operation, error) {
	switch stmt.Method {
	case "getCollectionNames":
		op.OpType = types.OpGetCollectionNames
	case "getCollectionInfos":
		op.OpType = types.OpGetCollectionInfos
		return extractGetCollectionInfosArgs(op, stmt.Args)
	case "createCollection":
		op.OpType = types.OpCreateCollection
		return extractCreateCollectionArgs(op, stmt.Args)
	case "dropDatabase":
		op.OpType = types.OpDropDatabase
	case "stats":
		op.OpType = types.OpDbStats
	case "serverStatus":
		op.OpType = types.OpServerStatus
	case "serverBuildInfo":
		op.OpType = types.OpServerBuildInfo
	case "version":
		op.OpType = types.OpDbVersion
	case "hostInfo":
		op.OpType = types.OpHostInfo
	case "listCommands":
		op.OpType = types.OpListCommands
	default:
		return nil, &UnsupportedOperationError{Operation: stmt.Method + "()"}
	}
	return op, nil
}

func translateCollectionStatement(op *Operation, stmt *ast.CollectionStatement) (*Operation, error) {
	op.Collection = stmt.Collection

	switch stmt.Method {
	// Read operations
	case "find":
		op.OpType = types.OpFind
		if err := extractFindArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "findOne":
		op.OpType = types.OpFindOne
		if err := extractFindOneArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "countDocuments":
		op.OpType = types.OpCountDocuments
		if err := extractCountDocumentsArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "estimatedDocumentCount":
		op.OpType = types.OpEstimatedDocumentCount
		if err := extractEstimatedDocumentCountArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "distinct":
		op.OpType = types.OpDistinct
		if err := extractDistinctArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "aggregate":
		op.OpType = types.OpAggregate
		if err := extractAggregateArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "getIndexes":
		op.OpType = types.OpGetIndexes

	// Write operations
	case "insertOne":
		op.OpType = types.OpInsertOne
		if err := extractInsertOneArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "insertMany":
		op.OpType = types.OpInsertMany
		if err := extractInsertManyArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "updateOne":
		op.OpType = types.OpUpdateOne
		if err := extractUpdateArgs(op, "updateOne", stmt.Args); err != nil {
			return nil, err
		}
	case "updateMany":
		op.OpType = types.OpUpdateMany
		if err := extractUpdateArgs(op, "updateMany", stmt.Args); err != nil {
			return nil, err
		}
	case "deleteOne":
		op.OpType = types.OpDeleteOne
		if err := extractDeleteArgs(op, "deleteOne", stmt.Args); err != nil {
			return nil, err
		}
	case "deleteMany":
		op.OpType = types.OpDeleteMany
		if err := extractDeleteArgs(op, "deleteMany", stmt.Args); err != nil {
			return nil, err
		}
	case "replaceOne":
		op.OpType = types.OpReplaceOne
		if err := extractReplaceOneArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "findOneAndUpdate":
		op.OpType = types.OpFindOneAndUpdate
		if err := extractFindOneAndModifyArgs(op, "findOneAndUpdate", stmt.Args, true); err != nil {
			return nil, err
		}
	case "findOneAndReplace":
		op.OpType = types.OpFindOneAndReplace
		if err := extractFindOneAndModifyArgs(op, "findOneAndReplace", stmt.Args, true); err != nil {
			return nil, err
		}
	case "findOneAndDelete":
		op.OpType = types.OpFindOneAndDelete
		if err := extractFindOneAndModifyArgs(op, "findOneAndDelete", stmt.Args, false); err != nil {
			return nil, err
		}

	// Index operations
	case "createIndex":
		op.OpType = types.OpCreateIndex
		if err := extractCreateIndexArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "createIndexes":
		op.OpType = types.OpCreateIndexes
		if err := extractCreateIndexesArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "dropIndex":
		op.OpType = types.OpDropIndex
		if err := extractDropIndexArgs(op, stmt.Args); err != nil {
			return nil, err
		}
	case "dropIndexes":
		op.OpType = types.OpDropIndexes
		if err := extractDropIndexesArgs(op, stmt.Args); err != nil {
			return nil, err
		}

	// Collection management
	case "drop":
		op.OpType = types.OpDrop
	case "renameCollection":
		op.OpType = types.OpRenameCollection
		if err := extractRenameCollectionArgs(op, stmt.Args); err != nil {
			return nil, err
		}

	// Collection information
	case "stats":
		op.OpType = types.OpCollectionStats
	case "storageSize":
		op.OpType = types.OpStorageSize
	case "totalIndexSize":
		op.OpType = types.OpTotalIndexSize
	case "totalSize":
		op.OpType = types.OpTotalSize
	case "dataSize":
		op.OpType = types.OpDataSize
	case "isCapped":
		op.OpType = types.OpIsCapped
	case "validate":
		op.OpType = types.OpValidate
	case "latencyStats":
		op.OpType = types.OpLatencyStats

	default:
		methodName := extractMethodName(stmt.Method)
		if methodName != "" {
			return nil, &UnsupportedOperationError{Operation: methodName + "()"}
		}
		return nil, &UnsupportedOperationError{Operation: stmt.Method}
	}

	// Process cursor methods.
	for _, cm := range stmt.CursorMethods {
		if err := translateCursorMethod(op, cm); err != nil {
			return nil, err
		}
	}

	return op, nil
}

func translateCursorMethod(op *Operation, cm ast.CursorMethod) error {
	switch cm.Method {
	case "sort":
		return extractSort(op, cm.Args)
	case "limit":
		return extractLimit(op, cm.Args)
	case "skip":
		return extractSkip(op, cm.Args)
	case "projection":
		return extractProjection(op, cm.Args)
	case "hint":
		return extractCursorHint(op, cm.Args)
	case "max":
		return extractMax(op, cm.Args)
	case "min":
		return extractMin(op, cm.Args)
	case "pretty":
		return nil // no-op
	default:
		return &UnsupportedOperationError{Operation: cm.Method + "()"}
	}
}

// extractMethodName extracts the method name before any parenthesis.
func extractMethodName(text string) string {
	if idx := strings.Index(text, "("); idx > 0 {
		return text[:idx]
	}
	return text
}
