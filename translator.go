package gomongo

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type operationType int

const (
	opUnknown operationType = iota
	opFind
	opFindOne
	opAggregate
	opShowDatabases
	opShowCollections
	opGetCollectionNames
	opGetCollectionInfos
	opGetIndexes
	opCountDocuments
	opEstimatedDocumentCount
	opDistinct
)

// mongoOperation represents a parsed MongoDB operation.
type mongoOperation struct {
	opType     operationType
	collection string
	filter     bson.D
	// Read operation options (find, findOne)
	sort       bson.D
	limit      *int64
	skip       *int64
	projection bson.D
	// Aggregation pipeline
	pipeline bson.A
	// countDocuments options
	hint any // string (index name) or document (index spec)
	// distinct field name
	distinctField string
}

// mongoShellVisitor extracts operations from a parse tree.
type mongoShellVisitor struct {
	mongodb.BaseMongoShellParserVisitor
	operation *mongoOperation
	err       error
}

func newMongoShellVisitor() *mongoShellVisitor {
	return &mongoShellVisitor{
		operation: &mongoOperation{opType: opUnknown},
	}
}

func (v *mongoShellVisitor) Visit(tree antlr.ParseTree) any {
	return tree.Accept(v)
}

func (v *mongoShellVisitor) VisitProgram(ctx *mongodb.ProgramContext) any {
	v.visitProgram(ctx)
	return nil
}

func (v *mongoShellVisitor) visitProgram(ctx mongodb.IProgramContext) {
	for _, stmt := range ctx.AllStatement() {
		v.visitStatement(stmt)
		if v.err != nil {
			return
		}
	}
}

func (v *mongoShellVisitor) VisitStatement(ctx *mongodb.StatementContext) any {
	v.visitStatement(ctx)
	return nil
}

func (v *mongoShellVisitor) visitStatement(ctx mongodb.IStatementContext) {
	if ctx.DbStatement() != nil {
		v.visitDbStatement(ctx.DbStatement())
	} else if ctx.ShellCommand() != nil {
		v.visitShellCommand(ctx.ShellCommand())
	}
}

func (v *mongoShellVisitor) visitDbStatement(ctx mongodb.IDbStatementContext) {
	switch c := ctx.(type) {
	case *mongodb.CollectionOperationContext:
		v.visitCollectionOperation(c)
	case *mongodb.GetCollectionNamesContext:
		v.operation.opType = opGetCollectionNames
	case *mongodb.GetCollectionInfosContext:
		v.operation.opType = opGetCollectionInfos
		v.extractGetCollectionInfosArgs(c)
	}
}

func (v *mongoShellVisitor) visitShellCommand(ctx mongodb.IShellCommandContext) {
	switch ctx.(type) {
	case *mongodb.ShowDatabasesContext:
		v.operation.opType = opShowDatabases
	case *mongodb.ShowCollectionsContext:
		v.operation.opType = opShowCollections
	default:
		v.err = &UnsupportedOperationError{
			Operation: ctx.GetText(),
			Hint:      "unknown shell command",
		}
	}
}

func (v *mongoShellVisitor) VisitCollectionOperation(ctx *mongodb.CollectionOperationContext) any {
	v.visitCollectionOperation(ctx)
	return nil
}

func (v *mongoShellVisitor) visitCollectionOperation(ctx *mongodb.CollectionOperationContext) {
	v.operation.collection = v.extractCollectionName(ctx.CollectionAccess())

	if ctx.MethodChain() != nil {
		v.visitMethodChain(ctx.MethodChain())
	}
}

func (v *mongoShellVisitor) VisitGetCollectionNames(_ *mongodb.GetCollectionNamesContext) any {
	v.operation.opType = opGetCollectionNames
	return nil
}

func (v *mongoShellVisitor) VisitGetCollectionInfos(ctx *mongodb.GetCollectionInfosContext) any {
	v.operation.opType = opGetCollectionInfos
	v.extractGetCollectionInfosArgs(ctx)
	return nil
}

func (v *mongoShellVisitor) extractGetCollectionInfosArgs(ctx *mongodb.GetCollectionInfosContext) {
	args := ctx.Arguments()
	if args == nil {
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		return
	}

	// First argument is the filter (optional)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		return
	}

	docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("getCollectionInfos() filter must be a document")
		return
	}

	filter, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.filter = filter
}

// extractCountDocumentsArgsFromMethod extracts arguments from CountDocumentsMethodContext.
func (v *mongoShellVisitor) extractCountDocumentsArgsFromMethod(ctx mongodb.ICountDocumentsMethodContext) {
	method, ok := ctx.(*mongodb.CountDocumentsMethodContext)
	if !ok {
		return
	}
	v.extractArgumentsForCountDocuments(method.Arguments())
}

// extractArgumentsForCountDocuments extracts countDocuments arguments from IArgumentsContext.
func (v *mongoShellVisitor) extractArgumentsForCountDocuments(args mongodb.IArgumentsContext) {
	if args == nil {
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		return
	}

	// First argument is the filter (optional)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		return
	}

	docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("countDocuments() filter must be a document")
		return
	}

	filter, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.filter = filter

	// Second argument is the options (optional)
	if len(allArgs) < 2 {
		return
	}

	secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	optionsValueCtx := secondArg.Value()
	if optionsValueCtx == nil {
		return
	}

	optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("countDocuments() options must be a document")
		return
	}

	optionsDoc, err := convertDocument(optionsDocValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid options: %w", err)
		return
	}

	// Extract supported options: hint, limit, skip
	for _, elem := range optionsDoc {
		switch elem.Key {
		case "hint":
			v.operation.hint = elem.Value
		case "limit":
			if val, ok := elem.Value.(int32); ok {
				limit := int64(val)
				v.operation.limit = &limit
			} else if val, ok := elem.Value.(int64); ok {
				v.operation.limit = &val
			}
		case "skip":
			if val, ok := elem.Value.(int32); ok {
				skip := int64(val)
				v.operation.skip = &skip
			} else if val, ok := elem.Value.(int64); ok {
				v.operation.skip = &val
			}
		}
	}
}

// extractDistinctArgsFromMethod extracts arguments from DistinctMethodContext.
func (v *mongoShellVisitor) extractDistinctArgsFromMethod(ctx mongodb.IDistinctMethodContext) {
	method, ok := ctx.(*mongodb.DistinctMethodContext)
	if !ok {
		return
	}
	v.extractArgumentsForDistinct(method.Arguments())
}

// extractArgumentsForDistinct extracts distinct arguments from IArgumentsContext.
func (v *mongoShellVisitor) extractArgumentsForDistinct(args mongodb.IArgumentsContext) {
	if args == nil {
		v.err = fmt.Errorf("distinct() requires a field name argument")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("distinct() requires a field name argument")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("distinct() requires a field name argument")
		return
	}

	// First argument is the field name (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("distinct() requires a field name argument")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("distinct() requires a field name argument")
		return
	}

	literalValue, ok := valueCtx.(*mongodb.LiteralValueContext)
	if !ok {
		v.err = fmt.Errorf("distinct() field name must be a string")
		return
	}

	stringLiteral, ok := literalValue.Literal().(*mongodb.StringLiteralValueContext)
	if !ok {
		v.err = fmt.Errorf("distinct() field name must be a string")
		return
	}

	v.operation.distinctField = unquoteString(stringLiteral.StringLiteral().GetText())

	// Second argument is the filter (optional)
	if len(allArgs) < 2 {
		return
	}

	secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	filterValueCtx := secondArg.Value()
	if filterValueCtx == nil {
		return
	}

	docValue, ok := filterValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("distinct() filter must be a document")
		return
	}

	filter, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.filter = filter
}

// extractAggregationPipelineFromMethod extracts pipeline from AggregateMethodContext.
func (v *mongoShellVisitor) extractAggregationPipelineFromMethod(ctx mongodb.IAggregateMethodContext) {
	method, ok := ctx.(*mongodb.AggregateMethodContext)
	if !ok {
		return
	}
	v.extractArgumentsForAggregate(method.Arguments())
}

// extractArgumentsForAggregate extracts aggregate pipeline from IArgumentsContext.
func (v *mongoShellVisitor) extractArgumentsForAggregate(args mongodb.IArgumentsContext) {
	if args == nil {
		// Empty pipeline: aggregate()
		v.operation.pipeline = bson.A{}
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("aggregate() requires an array argument")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.operation.pipeline = bson.A{}
		return
	}

	// First argument should be the pipeline array
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("aggregate() requires an array argument")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("aggregate() requires an array argument")
		return
	}

	arrayValue, ok := valueCtx.(*mongodb.ArrayValueContext)
	if !ok {
		v.err = fmt.Errorf("aggregate() requires an array argument, got %T", valueCtx)
		return
	}

	pipeline, err := convertArray(arrayValue.Array())
	if err != nil {
		v.err = fmt.Errorf("invalid aggregation pipeline: %w", err)
		return
	}

	v.operation.pipeline = pipeline
}

func (v *mongoShellVisitor) extractCollectionName(ctx mongodb.ICollectionAccessContext) string {
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

func (v *mongoShellVisitor) visitMethodChain(ctx mongodb.IMethodChainContext) {
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

func (v *mongoShellVisitor) extractFindFilter(ctx mongodb.IFindMethodContext) {
	fm, ok := ctx.(*mongodb.FindMethodContext)
	if !ok {
		return
	}

	arg := fm.Argument()
	if arg == nil {
		return
	}

	argCtx, ok := arg.(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	valueCtx := argCtx.Value()
	if valueCtx == nil {
		return
	}

	docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("find() filter must be a document")
		return
	}

	filter, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.filter = filter
}

func (v *mongoShellVisitor) extractFindOneFilter(ctx mongodb.IFindOneMethodContext) {
	fm, ok := ctx.(*mongodb.FindOneMethodContext)
	if !ok {
		return
	}

	arg := fm.Argument()
	if arg == nil {
		return
	}

	argCtx, ok := arg.(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	valueCtx := argCtx.Value()
	if valueCtx == nil {
		return
	}

	docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("findOne() filter must be a document")
		return
	}

	filter, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.filter = filter
}

func (v *mongoShellVisitor) extractSort(ctx mongodb.ISortMethodContext) {
	sm, ok := ctx.(*mongodb.SortMethodContext)
	if !ok {
		return
	}

	doc := sm.Document()
	if doc == nil {
		v.err = fmt.Errorf("sort() requires a document argument")
		return
	}

	sort, err := convertDocument(doc)
	if err != nil {
		v.err = fmt.Errorf("invalid sort: %w", err)
		return
	}
	v.operation.sort = sort
}

func (v *mongoShellVisitor) extractLimit(ctx mongodb.ILimitMethodContext) {
	lm, ok := ctx.(*mongodb.LimitMethodContext)
	if !ok {
		return
	}

	numNode := lm.NUMBER()
	if numNode == nil {
		v.err = fmt.Errorf("limit() requires a number argument")
		return
	}

	limit, err := strconv.ParseInt(numNode.GetText(), 10, 64)
	if err != nil {
		v.err = fmt.Errorf("invalid limit: %w", err)
		return
	}
	v.operation.limit = &limit
}

func (v *mongoShellVisitor) extractSkip(ctx mongodb.ISkipMethodContext) {
	sm, ok := ctx.(*mongodb.SkipMethodContext)
	if !ok {
		return
	}

	numNode := sm.NUMBER()
	if numNode == nil {
		v.err = fmt.Errorf("skip() requires a number argument")
		return
	}

	skip, err := strconv.ParseInt(numNode.GetText(), 10, 64)
	if err != nil {
		v.err = fmt.Errorf("invalid skip: %w", err)
		return
	}
	v.operation.skip = &skip
}

func (v *mongoShellVisitor) extractProjection(ctx mongodb.IProjectionMethodContext) {
	pm, ok := ctx.(*mongodb.ProjectionMethodContext)
	if !ok {
		return
	}

	doc := pm.Document()
	if doc == nil {
		v.err = fmt.Errorf("projection() requires a document argument")
		return
	}

	projection, err := convertDocument(doc)
	if err != nil {
		v.err = fmt.Errorf("invalid projection: %w", err)
		return
	}
	v.operation.projection = projection
}

func (v *mongoShellVisitor) visitMethodCall(ctx mongodb.IMethodCallContext) {
	mc, ok := ctx.(*mongodb.MethodCallContext)
	if !ok {
		return
	}

	// Determine method context for error messages
	getMethodContext := func() methodContext {
		if v.operation.opType == opFind || v.operation.opType == opFindOne {
			return contextCursor
		}
		return contextCollection
	}

	// Supported read operations
	if mc.FindMethod() != nil {
		v.operation.opType = opFind
		v.extractFindFilter(mc.FindMethod())
	} else if mc.FindOneMethod() != nil {
		v.operation.opType = opFindOne
		v.extractFindOneFilter(mc.FindOneMethod())
	} else if mc.CountDocumentsMethod() != nil {
		v.operation.opType = opCountDocuments
		v.extractCountDocumentsArgsFromMethod(mc.CountDocumentsMethod())
	} else if mc.EstimatedDocumentCountMethod() != nil {
		v.operation.opType = opEstimatedDocumentCount
	} else if mc.DistinctMethod() != nil {
		v.operation.opType = opDistinct
		v.extractDistinctArgsFromMethod(mc.DistinctMethod())
	} else if mc.AggregateMethod() != nil {
		v.operation.opType = opAggregate
		v.extractAggregationPipelineFromMethod(mc.AggregateMethod())
	} else if mc.GetIndexesMethod() != nil {
		v.operation.opType = opGetIndexes
	} else if mc.SortMethod() != nil {
		// Supported cursor modifiers
		v.extractSort(mc.SortMethod())
	} else if mc.LimitMethod() != nil {
		v.extractLimit(mc.LimitMethod())
	} else if mc.SkipMethod() != nil {
		v.extractSkip(mc.SkipMethod())
	} else if mc.ProjectionMethod() != nil {
		v.extractProjection(mc.ProjectionMethod())
	} else if mc.CountMethod() != nil {
		// Deprecated cursor method
		v.handleUnsupportedMethod(contextCursor, "count")
	} else if mc.InsertOneMethod() != nil {
		// Unsupported write operations
		v.handleUnsupportedMethod(contextCollection, "insertOne")
	} else if mc.InsertManyMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "insertMany")
	} else if mc.UpdateOneMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "updateOne")
	} else if mc.UpdateManyMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "updateMany")
	} else if mc.DeleteOneMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "deleteOne")
	} else if mc.DeleteManyMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "deleteMany")
	} else if mc.ReplaceOneMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "replaceOne")
	} else if mc.FindOneAndUpdateMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "findOneAndUpdate")
	} else if mc.FindOneAndReplaceMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "findOneAndReplace")
	} else if mc.FindOneAndDeleteMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "findOneAndDelete")
	} else if mc.CreateIndexMethod() != nil {
		// Unsupported index operations
		v.handleUnsupportedMethod(contextCollection, "createIndex")
	} else if mc.CreateIndexesMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "createIndexes")
	} else if mc.DropIndexMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "dropIndex")
	} else if mc.DropIndexesMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "dropIndexes")
	} else if mc.DropMethod() != nil {
		// Unsupported collection management
		v.handleUnsupportedMethod(contextCollection, "drop")
	} else if mc.RenameCollectionMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "renameCollection")
	} else if mc.StatsMethod() != nil {
		// Unsupported stats operations
		v.handleUnsupportedMethod(contextCollection, "stats")
	} else if mc.StorageSizeMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "storageSize")
	} else if mc.TotalIndexSizeMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "totalIndexSize")
	} else if mc.TotalSizeMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "totalSize")
	} else if mc.DataSizeMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "dataSize")
	} else if mc.IsCappedMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "isCapped")
	} else if mc.ValidateMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "validate")
	} else if mc.LatencyStatsMethod() != nil {
		v.handleUnsupportedMethod(contextCollection, "latencyStats")
	} else if mc.BatchSizeMethod() != nil {
		// Unsupported cursor methods
		v.handleUnsupportedMethod(contextCursor, "batchSize")
	} else if mc.CloseMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "close")
	} else if mc.CollationMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "collation")
	} else if mc.CommentMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "comment")
	} else if mc.ExplainMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "explain")
	} else if mc.ForEachMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "forEach")
	} else if mc.HasNextMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "hasNext")
	} else if mc.HintMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "hint")
	} else if mc.IsClosedMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "isClosed")
	} else if mc.IsExhaustedMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "isExhausted")
	} else if mc.ItcountMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "itcount")
	} else if mc.MapMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "map")
	} else if mc.MaxMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "max")
	} else if mc.MaxAwaitTimeMSMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "maxAwaitTimeMS")
	} else if mc.MaxTimeMSMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "maxTimeMS")
	} else if mc.MinMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "min")
	} else if mc.NextMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "next")
	} else if mc.NoCursorTimeoutMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "noCursorTimeout")
	} else if mc.ObjsLeftInBatchMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "objsLeftInBatch")
	} else if mc.PrettyMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "pretty")
	} else if mc.ReadConcernMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "readConcern")
	} else if mc.ReadPrefMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "readPref")
	} else if mc.ReturnKeyMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "returnKey")
	} else if mc.ShowRecordIdMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "showRecordId")
	} else if mc.SizeMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "size")
	} else if mc.TailableMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "tailable")
	} else if mc.ToArrayMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "toArray")
	} else if mc.TryNextMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "tryNext")
	} else if mc.AllowDiskUseMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "allowDiskUse")
	} else if mc.AddOptionMethod() != nil {
		v.handleUnsupportedMethod(contextCursor, "addOption")
	} else if gm := mc.GenericMethod(); gm != nil {
		// Fallback for any methods not explicitly handled above
		gmCtx, ok := gm.(*mongodb.GenericMethodContext)
		if !ok {
			return
		}
		methodName := gmCtx.Identifier().GetText()

		// Handle supported methods that may come through genericMethod
		// (e.g., aggregate() with no arguments goes to genericMethod, not aggregateMethod)
		switch methodName {
		case "aggregate":
			v.operation.opType = opAggregate
			v.extractArgumentsForAggregate(gmCtx.Arguments())
		case "countDocuments":
			v.operation.opType = opCountDocuments
			v.extractArgumentsForCountDocuments(gmCtx.Arguments())
		case "estimatedDocumentCount":
			v.operation.opType = opEstimatedDocumentCount
		case "distinct":
			v.operation.opType = opDistinct
			v.extractArgumentsForDistinct(gmCtx.Arguments())
		case "getIndexes":
			v.operation.opType = opGetIndexes
		default:
			v.handleUnsupportedMethod(getMethodContext(), methodName)
		}
	}
}

// handleUnsupportedMethod checks the method registry and returns appropriate errors.
func (v *mongoShellVisitor) handleUnsupportedMethod(ctx methodContext, methodName string) {
	info, found := lookupMethod(ctx, methodName)
	if !found {
		// Method not in registry - unknown method
		v.err = &UnsupportedOperationError{
			Operation: methodName + "()",
			Hint:      "unknown method",
		}
		return
	}

	switch info.status {
	case statusDeprecated:
		v.err = &DeprecatedOperationError{
			Operation:   methodName + "()",
			Alternative: info.alternative,
		}
	case statusUnsupported:
		v.err = &UnsupportedOperationError{
			Operation: methodName + "()",
			Hint:      info.hint,
		}
	case statusSupported:
		// This shouldn't happen - supported methods should be handled explicitly
		v.err = &UnsupportedOperationError{
			Operation: methodName + "()",
			Hint:      "method is supported but not handled",
		}
	}
}

// unquoteString removes quotes from a string literal.
func unquoteString(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// convertValue converts a parsed value context to a Go value for BSON.
func convertValue(ctx mongodb.IValueContext) (any, error) {
	switch v := ctx.(type) {
	case *mongodb.DocumentValueContext:
		return convertDocument(v.Document())
	case *mongodb.ArrayValueContext:
		return convertArray(v.Array())
	case *mongodb.LiteralValueContext:
		return convertLiteral(v.Literal())
	case *mongodb.HelperValueContext:
		return convertHelperFunction(v.HelperFunction())
	case *mongodb.RegexLiteralValueContext:
		return convertRegexLiteral(v.REGEX_LITERAL().GetText())
	case *mongodb.RegexpConstructorValueContext:
		return convertRegExpConstructor(v.RegExpConstructor())
	default:
		return nil, fmt.Errorf("unsupported value type: %T", ctx)
	}
}

// convertDocument converts a document context to bson.D.
func convertDocument(ctx mongodb.IDocumentContext) (bson.D, error) {
	doc, ok := ctx.(*mongodb.DocumentContext)
	if !ok {
		return nil, fmt.Errorf("invalid document context")
	}

	result := bson.D{}
	for _, pair := range doc.AllPair() {
		key, value, err := convertPair(pair)
		if err != nil {
			return nil, err
		}
		result = append(result, bson.E{Key: key, Value: value})
	}
	return result, nil
}

// convertPair converts a pair context to key-value.
func convertPair(ctx mongodb.IPairContext) (string, any, error) {
	pair, ok := ctx.(*mongodb.PairContext)
	if !ok {
		return "", nil, fmt.Errorf("invalid pair context")
	}

	key := extractKey(pair.Key())
	value, err := convertValue(pair.Value())
	if err != nil {
		return "", nil, fmt.Errorf("error converting value for key %q: %w", key, err)
	}
	return key, value, nil
}

// extractKey extracts the key string from a key context.
func extractKey(ctx mongodb.IKeyContext) string {
	switch k := ctx.(type) {
	case *mongodb.UnquotedKeyContext:
		return k.Identifier().GetText()
	case *mongodb.QuotedKeyContext:
		return unquoteString(k.StringLiteral().GetText())
	default:
		return ""
	}
}

// convertArray converts an array context to bson.A.
func convertArray(ctx mongodb.IArrayContext) (bson.A, error) {
	arr, ok := ctx.(*mongodb.ArrayContext)
	if !ok {
		return nil, fmt.Errorf("invalid array context")
	}

	result := bson.A{}
	for _, val := range arr.AllValue() {
		v, err := convertValue(val)
		if err != nil {
			return nil, err
		}
		result = append(result, v)
	}
	return result, nil
}

// convertLiteral converts a literal context to a Go value.
func convertLiteral(ctx mongodb.ILiteralContext) (any, error) {
	switch l := ctx.(type) {
	case *mongodb.NumberLiteralContext:
		return parseNumber(l.NUMBER().GetText())
	case *mongodb.StringLiteralValueContext:
		return unquoteString(l.StringLiteral().GetText()), nil
	case *mongodb.TrueLiteralContext:
		return true, nil
	case *mongodb.FalseLiteralContext:
		return false, nil
	case *mongodb.NullLiteralContext:
		return nil, nil
	default:
		return nil, fmt.Errorf("unsupported literal type: %T", ctx)
	}
}

// parseNumber parses a number string to int32, int64, or float64.
func parseNumber(s string) (any, error) {
	if strings.Contains(s, ".") || strings.Contains(s, "e") || strings.Contains(s, "E") {
		f, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number: %s", s)
		}
		return f, nil
	}

	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid number: %s", s)
	}

	if i >= -2147483648 && i <= 2147483647 {
		return int32(i), nil
	}
	return i, nil
}

// convertHelperFunction converts a helper function to a BSON value.
func convertHelperFunction(ctx mongodb.IHelperFunctionContext) (any, error) {
	helper, ok := ctx.(*mongodb.HelperFunctionContext)
	if !ok {
		return nil, fmt.Errorf("invalid helper function context")
	}

	if helper.ObjectIdHelper() != nil {
		return convertObjectIdHelper(helper.ObjectIdHelper())
	}
	if helper.IsoDateHelper() != nil {
		return convertIsoDateHelper(helper.IsoDateHelper())
	}
	if helper.DateHelper() != nil {
		return convertDateHelper(helper.DateHelper())
	}
	if helper.UuidHelper() != nil {
		return convertUuidHelper(helper.UuidHelper())
	}
	if helper.LongHelper() != nil {
		return convertLongHelper(helper.LongHelper())
	}
	if helper.Int32Helper() != nil {
		return convertInt32Helper(helper.Int32Helper())
	}
	if helper.DoubleHelper() != nil {
		return convertDoubleHelper(helper.DoubleHelper())
	}
	if helper.Decimal128Helper() != nil {
		return convertDecimal128Helper(helper.Decimal128Helper())
	}
	if helper.TimestampHelper() != nil {
		return convertTimestampHelper(helper.TimestampHelper())
	}

	return nil, fmt.Errorf("unsupported helper function")
}

// convertRegexLiteral converts a regex literal like /pattern/flags to bson.Regex.
func convertRegexLiteral(text string) (bson.Regex, error) {
	if len(text) < 2 || text[0] != '/' {
		return bson.Regex{}, fmt.Errorf("invalid regex literal: %s", text)
	}

	lastSlash := strings.LastIndex(text, "/")
	if lastSlash <= 0 {
		return bson.Regex{}, fmt.Errorf("invalid regex literal: %s", text)
	}

	pattern := text[1:lastSlash]
	options := ""
	if lastSlash < len(text)-1 {
		options = text[lastSlash+1:]
	}

	return bson.Regex{Pattern: pattern, Options: options}, nil
}

// convertRegExpConstructor converts RegExp("pattern", "flags") to bson.Regex.
func convertRegExpConstructor(ctx mongodb.IRegExpConstructorContext) (bson.Regex, error) {
	constructor, ok := ctx.(*mongodb.RegExpConstructorContext)
	if !ok {
		return bson.Regex{}, fmt.Errorf("invalid RegExp constructor context")
	}

	strings := constructor.AllStringLiteral()
	if len(strings) == 0 {
		return bson.Regex{}, fmt.Errorf("RegExp requires at least a pattern argument")
	}

	pattern := unquoteString(strings[0].GetText())
	options := ""
	if len(strings) > 1 {
		options = unquoteString(strings[1].GetText())
	}

	return bson.Regex{Pattern: pattern, Options: options}, nil
}
