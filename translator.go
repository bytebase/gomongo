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

func (v *mongoShellVisitor) extractAggregationPipeline(ctx *mongodb.GenericMethodContext) {
	args := ctx.Arguments()
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

func (v *mongoShellVisitor) visitMethodCall(ctx mongodb.IMethodCallContext) {
	mc, ok := ctx.(*mongodb.MethodCallContext)
	if !ok {
		return
	}

	if mc.FindMethod() != nil {
		v.operation.opType = opFind
		v.extractFindFilter(mc.FindMethod())
	} else if mc.FindOneMethod() != nil {
		v.operation.opType = opFindOne
		v.extractFindOneFilter(mc.FindOneMethod())
	} else if mc.SortMethod() != nil {
		v.extractSort(mc.SortMethod())
	} else if mc.LimitMethod() != nil {
		v.extractLimit(mc.LimitMethod())
	} else if mc.SkipMethod() != nil {
		v.extractSkip(mc.SkipMethod())
	} else if mc.ProjectionMethod() != nil {
		v.extractProjection(mc.ProjectionMethod())
	} else if gm := mc.GenericMethod(); gm != nil {
		gmCtx, ok := gm.(*mongodb.GenericMethodContext)
		if !ok {
			return
		}
		methodName := gmCtx.Identifier().GetText()
		if methodName == "aggregate" {
			v.operation.opType = opAggregate
			v.extractAggregationPipeline(gmCtx)
		} else {
			v.err = &UnsupportedOperationError{
				Operation: methodName,
				Hint:      "unknown method",
			}
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
