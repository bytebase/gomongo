package translator

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/gomongo/types"
	"github.com/bytebase/parser/mongodb"
)

// visitor extracts operations from a parse tree.
type visitor struct {
	mongodb.BaseMongoShellParserVisitor
	operation *Operation
	err       error
}

func newVisitor() *visitor {
	return &visitor{
		operation: &Operation{OpType: types.OpUnknown},
	}
}

func (v *visitor) Visit(tree antlr.ParseTree) any {
	return tree.Accept(v)
}

func (v *visitor) VisitProgram(ctx *mongodb.ProgramContext) any {
	v.visitProgram(ctx)
	return nil
}

func (v *visitor) visitProgram(ctx mongodb.IProgramContext) {
	for _, stmt := range ctx.AllStatement() {
		v.visitStatement(stmt)
		if v.err != nil {
			return
		}
	}
}

func (v *visitor) VisitStatement(ctx *mongodb.StatementContext) any {
	v.visitStatement(ctx)
	return nil
}

func (v *visitor) visitStatement(ctx mongodb.IStatementContext) {
	if ctx.DbStatement() != nil {
		v.visitDbStatement(ctx.DbStatement())
	} else if ctx.ShellCommand() != nil {
		v.visitShellCommand(ctx.ShellCommand())
	}
}

func (v *visitor) visitDbStatement(ctx mongodb.IDbStatementContext) {
	switch c := ctx.(type) {
	case *mongodb.CollectionOperationContext:
		v.visitCollectionOperation(c)
	case *mongodb.GetCollectionNamesContext:
		v.operation.OpType = types.OpGetCollectionNames
	case *mongodb.GetCollectionInfosContext:
		v.operation.OpType = types.OpGetCollectionInfos
		v.extractGetCollectionInfosArgs(c)
	case *mongodb.CreateCollectionContext:
		v.operation.OpType = types.OpCreateCollection
		v.extractCreateCollectionArgs(c)
	case *mongodb.DropDatabaseContext:
		v.operation.OpType = types.OpDropDatabase
	// Database information commands
	case *mongodb.DbStatsContext:
		v.operation.OpType = types.OpDbStats
	case *mongodb.ServerStatusContext:
		v.operation.OpType = types.OpServerStatus
	case *mongodb.ServerBuildInfoContext:
		v.operation.OpType = types.OpServerBuildInfo
	case *mongodb.DbVersionContext:
		v.operation.OpType = types.OpDbVersion
	case *mongodb.HostInfoContext:
		v.operation.OpType = types.OpHostInfo
	case *mongodb.ListCommandsContext:
		v.operation.OpType = types.OpListCommands
	}
}

func (v *visitor) visitShellCommand(ctx mongodb.IShellCommandContext) {
	switch ctx.(type) {
	case *mongodb.ShowDatabasesContext:
		v.operation.OpType = types.OpShowDatabases
	case *mongodb.ShowCollectionsContext:
		v.operation.OpType = types.OpShowCollections
	default:
		v.err = &UnsupportedOperationError{
			Operation: ctx.GetText(),
		}
	}
}

func (v *visitor) VisitCollectionOperation(ctx *mongodb.CollectionOperationContext) any {
	v.visitCollectionOperation(ctx)
	return nil
}

func (v *visitor) visitCollectionOperation(ctx *mongodb.CollectionOperationContext) {
	v.operation.Collection = v.extractCollectionName(ctx.CollectionAccess())

	if ctx.MethodChain() != nil {
		v.visitMethodChain(ctx.MethodChain())
	}
}

func (v *visitor) VisitGetCollectionNames(_ *mongodb.GetCollectionNamesContext) any {
	v.operation.OpType = types.OpGetCollectionNames
	return nil
}

func (v *visitor) VisitGetCollectionInfos(ctx *mongodb.GetCollectionInfosContext) any {
	v.operation.OpType = types.OpGetCollectionInfos
	v.extractGetCollectionInfosArgs(ctx)
	return nil
}

func (v *visitor) extractCollectionName(ctx mongodb.ICollectionAccessContext) string {
	switch c := ctx.(type) {
	case *mongodb.DotAccessContext:
		return c.Identifier().GetText()
	case *mongodb.BracketAccessContext:
		return unquoteString(c.StringLiteral().GetText())
	case *mongodb.GetCollectionAccessContext:
		return unquoteString(c.StringLiteral().GetText())
	}
	return ""
}

func (v *visitor) visitMethodChain(ctx mongodb.IMethodChainContext) {
	mc, ok := ctx.(*mongodb.MethodChainContext)
	if !ok {
		return
	}

	if mc.CollectionMethodCall() != nil {
		v.visitCollectionMethodCall(mc.CollectionMethodCall())
		if v.err != nil {
			return
		}
	}

	for _, cursorCall := range mc.AllCursorMethodCall() {
		v.visitCursorMethodCall(cursorCall)
		if v.err != nil {
			return
		}
	}
}

func (v *visitor) visitCollectionMethodCall(ctx mongodb.ICollectionMethodCallContext) {
	mc, ok := ctx.(*mongodb.CollectionMethodCallContext)
	if !ok {
		return
	}

	switch {
	// Supported read operations
	case mc.FindMethod() != nil:
		v.operation.OpType = types.OpFind
		v.extractFindArgs(mc.FindMethod())
	case mc.FindOneMethod() != nil:
		v.operation.OpType = types.OpFindOne
		v.extractFindOneArgs(mc.FindOneMethod())
	case mc.CountDocumentsMethod() != nil:
		v.operation.OpType = types.OpCountDocuments
		v.extractCountDocumentsArgsFromMethod(mc.CountDocumentsMethod())
	case mc.EstimatedDocumentCountMethod() != nil:
		v.operation.OpType = types.OpEstimatedDocumentCount
		v.extractEstimatedDocumentCountArgs(mc.EstimatedDocumentCountMethod())
	case mc.DistinctMethod() != nil:
		v.operation.OpType = types.OpDistinct
		v.extractDistinctArgsFromMethod(mc.DistinctMethod())
	case mc.AggregateMethod() != nil:
		v.operation.OpType = types.OpAggregate
		v.extractAggregationPipelineFromMethod(mc.AggregateMethod())
	case mc.GetIndexesMethod() != nil:
		v.operation.OpType = types.OpGetIndexes

	// Supported write operations
	case mc.InsertOneMethod() != nil:
		v.operation.OpType = types.OpInsertOne
		v.extractInsertOneArgs(mc.InsertOneMethod())
	case mc.InsertManyMethod() != nil:
		v.operation.OpType = types.OpInsertMany
		v.extractInsertManyArgs(mc.InsertManyMethod())
	case mc.UpdateOneMethod() != nil:
		v.operation.OpType = types.OpUpdateOne
		v.extractUpdateOneArgs(mc.UpdateOneMethod())
	case mc.UpdateManyMethod() != nil:
		v.operation.OpType = types.OpUpdateMany
		v.extractUpdateManyArgs(mc.UpdateManyMethod())
	case mc.DeleteOneMethod() != nil:
		v.operation.OpType = types.OpDeleteOne
		v.extractDeleteOneArgs(mc.DeleteOneMethod())
	case mc.DeleteManyMethod() != nil:
		v.operation.OpType = types.OpDeleteMany
		v.extractDeleteManyArgs(mc.DeleteManyMethod())
	case mc.ReplaceOneMethod() != nil:
		v.operation.OpType = types.OpReplaceOne
		v.extractReplaceOneArgs(mc.ReplaceOneMethod())
	case mc.FindOneAndUpdateMethod() != nil:
		v.operation.OpType = types.OpFindOneAndUpdate
		v.extractFindOneAndUpdateArgs(mc.FindOneAndUpdateMethod())
	case mc.FindOneAndReplaceMethod() != nil:
		v.operation.OpType = types.OpFindOneAndReplace
		v.extractFindOneAndReplaceArgs(mc.FindOneAndReplaceMethod())
	case mc.FindOneAndDeleteMethod() != nil:
		v.operation.OpType = types.OpFindOneAndDelete
		v.extractFindOneAndDeleteArgs(mc.FindOneAndDeleteMethod())

	// Supported index operations
	case mc.CreateIndexMethod() != nil:
		v.operation.OpType = types.OpCreateIndex
		v.extractCreateIndexArgs(mc.CreateIndexMethod())
	case mc.CreateIndexesMethod() != nil:
		v.operation.OpType = types.OpCreateIndexes
		v.extractCreateIndexesArgs(mc.CreateIndexesMethod())
	case mc.DropIndexMethod() != nil:
		v.operation.OpType = types.OpDropIndex
		v.extractDropIndexArgs(mc.DropIndexMethod())
	case mc.DropIndexesMethod() != nil:
		v.operation.OpType = types.OpDropIndexes
		v.extractDropIndexesArgs(mc.DropIndexesMethod())

	// Supported collection management
	case mc.DropMethod() != nil:
		v.operation.OpType = types.OpDrop
	case mc.RenameCollectionMethod() != nil:
		v.operation.OpType = types.OpRenameCollection
		v.extractRenameCollectionArgs(mc.RenameCollectionMethod())

	// Collection information commands
	case mc.StatsMethod() != nil:
		v.operation.OpType = types.OpCollectionStats
	case mc.StorageSizeMethod() != nil:
		v.operation.OpType = types.OpStorageSize
	case mc.TotalIndexSizeMethod() != nil:
		v.operation.OpType = types.OpTotalIndexSize
	case mc.TotalSizeMethod() != nil:
		v.operation.OpType = types.OpTotalSize
	case mc.DataSizeMethod() != nil:
		v.operation.OpType = types.OpDataSize
	case mc.IsCappedMethod() != nil:
		v.operation.OpType = types.OpIsCapped
	case mc.ValidateMethod() != nil:
		v.operation.OpType = types.OpValidate
	case mc.LatencyStatsMethod() != nil:
		v.operation.OpType = types.OpLatencyStats

	default:
		methodName := extractMethodNameFromText(mc.GetText())
		if methodName != "" {
			v.handleUnsupportedMethod("collection", methodName)
		}
	}
}

func (v *visitor) visitCursorMethodCall(ctx mongodb.ICursorMethodCallContext) {
	mc, ok := ctx.(*mongodb.CursorMethodCallContext)
	if !ok {
		return
	}

	switch {
	case mc.SortMethod() != nil:
		v.extractSort(mc.SortMethod())
	case mc.LimitMethod() != nil:
		v.extractLimit(mc.LimitMethod())
	case mc.SkipMethod() != nil:
		v.extractSkip(mc.SkipMethod())
	case mc.ProjectionMethod() != nil:
		v.extractProjection(mc.ProjectionMethod())
	case mc.HintMethod() != nil:
		v.extractHint(mc.HintMethod())
	case mc.MaxMethod() != nil:
		v.extractMax(mc.MaxMethod())
	case mc.MinMethod() != nil:
		v.extractMin(mc.MinMethod())
	case mc.PrettyMethod() != nil:
		// pretty() is a no-op — output is already formatted.
	default:
		methodName := extractMethodNameFromText(mc.GetText())
		if methodName != "" {
			v.handleUnsupportedMethod("cursor", methodName)
		}
	}
}

// extractMethodNameFromText extracts the method name from a parse tree text before the opening parenthesis.
func extractMethodNameFromText(text string) string {
	if idx := strings.Index(text, "("); idx > 0 {
		return text[:idx]
	}
	return text
}

// handleUnsupportedMethod returns an UnsupportedOperationError for unknown methods.
func (v *visitor) handleUnsupportedMethod(_, methodName string) {
	v.err = &UnsupportedOperationError{
		Operation: methodName + "()",
	}
}
