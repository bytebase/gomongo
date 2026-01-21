package translator

import (
	"strings"

	"github.com/antlr4-go/antlr/v4"
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
		operation: &Operation{OpType: OpUnknown},
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
		v.operation.OpType = OpGetCollectionNames
	case *mongodb.GetCollectionInfosContext:
		v.operation.OpType = OpGetCollectionInfos
		v.extractGetCollectionInfosArgs(c)
	}
}

func (v *visitor) visitShellCommand(ctx mongodb.IShellCommandContext) {
	switch ctx.(type) {
	case *mongodb.ShowDatabasesContext:
		v.operation.OpType = OpShowDatabases
	case *mongodb.ShowCollectionsContext:
		v.operation.OpType = OpShowCollections
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
	v.operation.OpType = OpGetCollectionNames
	return nil
}

func (v *visitor) VisitGetCollectionInfos(ctx *mongodb.GetCollectionInfosContext) any {
	v.operation.OpType = OpGetCollectionInfos
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
	for _, methodCall := range mc.AllMethodCall() {
		v.visitMethodCall(methodCall)
		if v.err != nil {
			return
		}
	}
}

func (v *visitor) visitMethodCall(ctx mongodb.IMethodCallContext) {
	mc, ok := ctx.(*mongodb.MethodCallContext)
	if !ok {
		return
	}

	// Determine method context for registry lookup
	getMethodContext := func() string {
		if v.operation.OpType == OpFind || v.operation.OpType == OpFindOne {
			return "cursor"
		}
		return "collection"
	}

	switch {
	// Supported read operations
	case mc.FindMethod() != nil:
		v.operation.OpType = OpFind
		v.extractFindArgs(mc.FindMethod())
	case mc.FindOneMethod() != nil:
		v.operation.OpType = OpFindOne
		v.extractFindOneArgs(mc.FindOneMethod())
	case mc.CountDocumentsMethod() != nil:
		v.operation.OpType = OpCountDocuments
		v.extractCountDocumentsArgsFromMethod(mc.CountDocumentsMethod())
	case mc.EstimatedDocumentCountMethod() != nil:
		v.operation.OpType = OpEstimatedDocumentCount
		v.extractEstimatedDocumentCountArgs(mc.EstimatedDocumentCountMethod())
	case mc.DistinctMethod() != nil:
		v.operation.OpType = OpDistinct
		v.extractDistinctArgsFromMethod(mc.DistinctMethod())
	case mc.AggregateMethod() != nil:
		v.operation.OpType = OpAggregate
		v.extractAggregationPipelineFromMethod(mc.AggregateMethod())
	case mc.GetIndexesMethod() != nil:
		v.operation.OpType = OpGetIndexes

	// Supported cursor modifiers
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

	// Planned M2 write operations - return PlannedOperationError for fallback
	case mc.InsertOneMethod() != nil:
		v.handleUnsupportedMethod("collection", "insertOne")
	case mc.InsertManyMethod() != nil:
		v.handleUnsupportedMethod("collection", "insertMany")
	case mc.UpdateOneMethod() != nil:
		v.handleUnsupportedMethod("collection", "updateOne")
	case mc.UpdateManyMethod() != nil:
		v.handleUnsupportedMethod("collection", "updateMany")
	case mc.DeleteOneMethod() != nil:
		v.handleUnsupportedMethod("collection", "deleteOne")
	case mc.DeleteManyMethod() != nil:
		v.handleUnsupportedMethod("collection", "deleteMany")
	case mc.ReplaceOneMethod() != nil:
		v.handleUnsupportedMethod("collection", "replaceOne")
	case mc.FindOneAndUpdateMethod() != nil:
		v.handleUnsupportedMethod("collection", "findOneAndUpdate")
	case mc.FindOneAndReplaceMethod() != nil:
		v.handleUnsupportedMethod("collection", "findOneAndReplace")
	case mc.FindOneAndDeleteMethod() != nil:
		v.handleUnsupportedMethod("collection", "findOneAndDelete")

	// Planned M3 index operations - return PlannedOperationError for fallback
	case mc.CreateIndexMethod() != nil:
		v.handleUnsupportedMethod("collection", "createIndex")
	case mc.CreateIndexesMethod() != nil:
		v.handleUnsupportedMethod("collection", "createIndexes")
	case mc.DropIndexMethod() != nil:
		v.handleUnsupportedMethod("collection", "dropIndex")
	case mc.DropIndexesMethod() != nil:
		v.handleUnsupportedMethod("collection", "dropIndexes")

	// Planned M3 collection management - return PlannedOperationError for fallback
	case mc.DropMethod() != nil:
		v.handleUnsupportedMethod("collection", "drop")
	case mc.RenameCollectionMethod() != nil:
		v.handleUnsupportedMethod("collection", "renameCollection")

	// Planned M3 stats operations - return PlannedOperationError for fallback
	case mc.StatsMethod() != nil:
		v.handleUnsupportedMethod("collection", "stats")
	case mc.StorageSizeMethod() != nil:
		v.handleUnsupportedMethod("collection", "storageSize")
	case mc.TotalIndexSizeMethod() != nil:
		v.handleUnsupportedMethod("collection", "totalIndexSize")
	case mc.TotalSizeMethod() != nil:
		v.handleUnsupportedMethod("collection", "totalSize")
	case mc.DataSizeMethod() != nil:
		v.handleUnsupportedMethod("collection", "dataSize")
	case mc.IsCappedMethod() != nil:
		v.handleUnsupportedMethod("collection", "isCapped")
	case mc.ValidateMethod() != nil:
		v.handleUnsupportedMethod("collection", "validate")
	case mc.LatencyStatsMethod() != nil:
		v.handleUnsupportedMethod("collection", "latencyStats")

	// Generic method fallback - all methods going through genericMethod are unsupported
	case mc.GenericMethod() != nil:
		gmCtx, ok := mc.GenericMethod().(*mongodb.GenericMethodContext)
		if !ok {
			return
		}
		methodName := gmCtx.Identifier().GetText()
		v.handleUnsupportedMethod(getMethodContext(), methodName)

	// Default: all other methods not explicitly handled
	// These go to handleUnsupportedMethod which returns UnsupportedOperationError
	// since they're not in the planned registry
	default:
		// Extract method name from the parse tree for error message
		methodName := v.extractMethodName(mc)
		if methodName != "" {
			v.handleUnsupportedMethod(getMethodContext(), methodName)
		}
	}
}

// extractMethodName extracts the method name from a MethodCallContext for error messages.
func (v *visitor) extractMethodName(mc *mongodb.MethodCallContext) string {
	// Try to get method name from various method contexts
	// The parser creates specific method contexts for known methods
	// For unknown methods, they go through GenericMethod which is handled separately
	text := mc.GetText()
	// Extract method name before the opening parenthesis
	if idx := strings.Index(text, "("); idx > 0 {
		return text[:idx]
	}
	return text
}

// handleUnsupportedMethod checks the method registry and returns appropriate errors.
// If method is in registry (planned for M2/M3) -> PlannedOperationError (fallback to mongosh)
// If method is NOT in registry -> UnsupportedOperationError (no fallback)
func (v *visitor) handleUnsupportedMethod(context, methodName string) {
	if IsPlannedMethod(context, methodName) {
		v.err = &PlannedOperationError{
			Operation: methodName + "()",
		}
		return
	}
	v.err = &UnsupportedOperationError{
		Operation: methodName + "()",
	}
}
