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
	// Index scan bounds and query options
	hint      any    // string (index name) or document (index spec)
	max       bson.D // upper bound for index scan
	min       bson.D // lower bound for index scan
	maxTimeMS *int64 // max execution time in milliseconds
	// Aggregation pipeline
	pipeline bson.A
	// distinct field name
	distinctField string
	// getCollectionInfos options
	nameOnly              *bool
	authorizedCollections *bool
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

	// Second argument is the options (optional)
	if len(allArgs) >= 2 {
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
			v.err = fmt.Errorf("getCollectionInfos() options must be a document")
			return
		}

		optionsDoc, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range optionsDoc {
			switch opt.Key {
			case "nameOnly":
				if val, ok := opt.Value.(bool); ok {
					v.operation.nameOnly = &val
				} else {
					v.err = fmt.Errorf("getCollectionInfos() nameOnly must be a boolean")
					return
				}
			case "authorizedCollections":
				if val, ok := opt.Value.(bool); ok {
					v.operation.authorizedCollections = &val
				} else {
					v.err = fmt.Errorf("getCollectionInfos() authorizedCollections must be a boolean")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "getCollectionInfos()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("getCollectionInfos() takes at most 2 arguments")
		return
	}
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

	// Extract supported options: hint, limit, skip, maxTimeMS
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
		case "maxTimeMS":
			if val, ok := elem.Value.(int32); ok {
				ms := int64(val)
				v.operation.maxTimeMS = &ms
			} else if val, ok := elem.Value.(int64); ok {
				v.operation.maxTimeMS = &val
			} else {
				v.err = fmt.Errorf("countDocuments() maxTimeMS must be a number")
				return
			}
		default:
			v.err = &UnsupportedOptionError{
				Method: "countDocuments()",
				Option: elem.Key,
			}
			return
		}
	}
}

// extractEstimatedDocumentCountArgs extracts arguments from EstimatedDocumentCountMethodContext.
func (v *mongoShellVisitor) extractEstimatedDocumentCountArgs(ctx mongodb.IEstimatedDocumentCountMethodContext) {
	method, ok := ctx.(*mongodb.EstimatedDocumentCountMethodContext)
	if !ok {
		return
	}

	// EstimatedDocumentCountMethodContext has Argument() (singular) that returns a single optional argument
	arg := method.Argument()
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
		v.err = fmt.Errorf("estimatedDocumentCount() options must be a document")
		return
	}

	options, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid options: %w", err)
		return
	}

	for _, opt := range options {
		switch opt.Key {
		case "maxTimeMS":
			if val, ok := opt.Value.(int32); ok {
				ms := int64(val)
				v.operation.maxTimeMS = &ms
			} else if val, ok := opt.Value.(int64); ok {
				v.operation.maxTimeMS = &val
			} else {
				v.err = fmt.Errorf("estimatedDocumentCount() maxTimeMS must be a number")
				return
			}
		default:
			v.err = &UnsupportedOptionError{
				Method: "estimatedDocumentCount()",
				Option: opt.Key,
			}
			return
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

	// Third argument: options (optional)
	if len(allArgs) >= 3 {
		thirdArg, ok := allArgs[2].(*mongodb.ArgumentContext)
		if !ok {
			return
		}

		optionsValueCtx := thirdArg.Value()
		if optionsValueCtx == nil {
			return
		}

		docValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("distinct() options must be a document")
			return
		}

		options, err := convertDocument(docValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "maxTimeMS":
				if val, ok := opt.Value.(int32); ok {
					ms := int64(val)
					v.operation.maxTimeMS = &ms
				} else if val, ok := opt.Value.(int64); ok {
					v.operation.maxTimeMS = &val
				} else {
					v.err = fmt.Errorf("distinct() maxTimeMS must be a number")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "distinct()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 3 {
		v.err = fmt.Errorf("distinct() takes at most 3 arguments")
		return
	}
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

	// Second argument: options (optional)
	if len(allArgs) >= 2 {
		secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
		if !ok {
			return
		}
		optionsValueCtx := secondArg.Value()
		if optionsValueCtx == nil {
			return
		}
		docValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("aggregate() options must be a document")
			return
		}
		options, err := convertDocument(docValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}
		for _, opt := range options {
			switch opt.Key {
			case "hint":
				v.operation.hint = opt.Value
			case "maxTimeMS":
				if val, ok := opt.Value.(int32); ok {
					ms := int64(val)
					v.operation.maxTimeMS = &ms
				} else if val, ok := opt.Value.(int64); ok {
					v.operation.maxTimeMS = &val
				} else {
					v.err = fmt.Errorf("aggregate() maxTimeMS must be a number")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "aggregate()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	// More than 2 arguments is an error
	if len(allArgs) > 2 {
		v.err = fmt.Errorf("aggregate() takes at most 2 arguments")
		return
	}
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

func (v *mongoShellVisitor) extractFindArgs(ctx mongodb.IFindMethodContext) {
	fm, ok := ctx.(*mongodb.FindMethodContext)
	if !ok {
		return
	}

	args := fm.Arguments()
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

	// First argument: filter
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}
	valueCtx := firstArg.Value()
	if valueCtx != nil {
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

	// Second argument: projection (optional)
	if len(allArgs) >= 2 {
		secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
		if !ok {
			return
		}
		valueCtx := secondArg.Value()
		if valueCtx != nil {
			docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
			if !ok {
				v.err = fmt.Errorf("find() projection must be a document")
				return
			}
			projection, err := convertDocument(docValue.Document())
			if err != nil {
				v.err = fmt.Errorf("invalid projection: %w", err)
				return
			}
			v.operation.projection = projection
		}
	}

	// Third argument: options (optional)
	if len(allArgs) >= 3 {
		thirdArg, ok := allArgs[2].(*mongodb.ArgumentContext)
		if !ok {
			return
		}
		valueCtx := thirdArg.Value()
		if valueCtx != nil {
			docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
			if !ok {
				v.err = fmt.Errorf("find() options must be a document")
				return
			}
			options, err := convertDocument(docValue.Document())
			if err != nil {
				v.err = fmt.Errorf("invalid options: %w", err)
				return
			}
			// Validate and extract supported options
			for _, opt := range options {
				switch opt.Key {
				case "hint":
					v.operation.hint = opt.Value
				case "max":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.max = doc
					} else {
						v.err = fmt.Errorf("find() max must be a document")
						return
					}
				case "min":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.min = doc
					} else {
						v.err = fmt.Errorf("find() min must be a document")
						return
					}
				case "maxTimeMS":
					if val, ok := opt.Value.(int32); ok {
						ms := int64(val)
						v.operation.maxTimeMS = &ms
					} else if val, ok := opt.Value.(int64); ok {
						v.operation.maxTimeMS = &val
					} else {
						v.err = fmt.Errorf("find() maxTimeMS must be a number")
						return
					}
				default:
					v.err = &UnsupportedOptionError{
						Method: "find()",
						Option: opt.Key,
					}
					return
				}
			}
		}
	}

	// More than 3 arguments is an error
	if len(allArgs) > 3 {
		v.err = fmt.Errorf("find() takes at most 3 arguments")
		return
	}
}

func (v *mongoShellVisitor) extractFindOneArgs(ctx mongodb.IFindOneMethodContext) {
	fm, ok := ctx.(*mongodb.FindOneMethodContext)
	if !ok {
		return
	}

	args := fm.Arguments()
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

	// First argument: filter
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}
	valueCtx := firstArg.Value()
	if valueCtx != nil {
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

	// Second argument: projection (optional)
	if len(allArgs) >= 2 {
		secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
		if !ok {
			return
		}
		valueCtx := secondArg.Value()
		if valueCtx != nil {
			docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
			if !ok {
				v.err = fmt.Errorf("findOne() projection must be a document")
				return
			}
			projection, err := convertDocument(docValue.Document())
			if err != nil {
				v.err = fmt.Errorf("invalid projection: %w", err)
				return
			}
			v.operation.projection = projection
		}
	}

	// Third argument: options (optional)
	if len(allArgs) >= 3 {
		thirdArg, ok := allArgs[2].(*mongodb.ArgumentContext)
		if !ok {
			return
		}
		valueCtx := thirdArg.Value()
		if valueCtx != nil {
			docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
			if !ok {
				v.err = fmt.Errorf("findOne() options must be a document")
				return
			}
			options, err := convertDocument(docValue.Document())
			if err != nil {
				v.err = fmt.Errorf("invalid options: %w", err)
				return
			}
			// Validate and extract supported options
			for _, opt := range options {
				switch opt.Key {
				case "hint":
					v.operation.hint = opt.Value
				case "max":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.max = doc
					} else {
						v.err = fmt.Errorf("findOne() max must be a document")
						return
					}
				case "min":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.min = doc
					} else {
						v.err = fmt.Errorf("findOne() min must be a document")
						return
					}
				case "maxTimeMS":
					if val, ok := opt.Value.(int32); ok {
						ms := int64(val)
						v.operation.maxTimeMS = &ms
					} else if val, ok := opt.Value.(int64); ok {
						v.operation.maxTimeMS = &val
					} else {
						v.err = fmt.Errorf("findOne() maxTimeMS must be a number")
						return
					}
				default:
					v.err = &UnsupportedOptionError{
						Method: "findOne()",
						Option: opt.Key,
					}
					return
				}
			}
		}
	}

	// More than 3 arguments is an error
	if len(allArgs) > 3 {
		v.err = fmt.Errorf("findOne() takes at most 3 arguments")
		return
	}
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

func (v *mongoShellVisitor) extractHint(ctx mongodb.IHintMethodContext) {
	hm, ok := ctx.(*mongodb.HintMethodContext)
	if !ok {
		return
	}

	arg := hm.Argument()
	if arg == nil {
		v.err = fmt.Errorf("hint() requires an argument")
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

	// hint can be a string (index name) or document (index spec)
	switch val := valueCtx.(type) {
	case *mongodb.LiteralValueContext:
		strLit, ok := val.Literal().(*mongodb.StringLiteralValueContext)
		if !ok {
			v.err = fmt.Errorf("hint() argument must be a string or document")
			return
		}
		v.operation.hint = unquoteString(strLit.StringLiteral().GetText())
	case *mongodb.DocumentValueContext:
		doc, err := convertDocument(val.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid hint: %w", err)
			return
		}
		v.operation.hint = doc
	default:
		v.err = fmt.Errorf("hint() argument must be a string or document")
	}
}

func (v *mongoShellVisitor) extractMax(ctx mongodb.IMaxMethodContext) {
	mm, ok := ctx.(*mongodb.MaxMethodContext)
	if !ok {
		return
	}

	doc := mm.Document()
	if doc == nil {
		v.err = fmt.Errorf("max() requires a document argument")
		return
	}

	maxDoc, err := convertDocument(doc)
	if err != nil {
		v.err = fmt.Errorf("invalid max: %w", err)
		return
	}
	v.operation.max = maxDoc
}

func (v *mongoShellVisitor) extractMin(ctx mongodb.IMinMethodContext) {
	mm, ok := ctx.(*mongodb.MinMethodContext)
	if !ok {
		return
	}

	doc := mm.Document()
	if doc == nil {
		v.err = fmt.Errorf("min() requires a document argument")
		return
	}

	minDoc, err := convertDocument(doc)
	if err != nil {
		v.err = fmt.Errorf("invalid min: %w", err)
		return
	}
	v.operation.min = minDoc
}

func (v *mongoShellVisitor) visitMethodCall(ctx mongodb.IMethodCallContext) {
	mc, ok := ctx.(*mongodb.MethodCallContext)
	if !ok {
		return
	}

	// Determine method context for registry lookup
	getMethodContext := func() string {
		if v.operation.opType == opFind || v.operation.opType == opFindOne {
			return "cursor"
		}
		return "collection"
	}

	switch {
	// Supported read operations
	case mc.FindMethod() != nil:
		v.operation.opType = opFind
		v.extractFindArgs(mc.FindMethod())
	case mc.FindOneMethod() != nil:
		v.operation.opType = opFindOne
		v.extractFindOneArgs(mc.FindOneMethod())
	case mc.CountDocumentsMethod() != nil:
		v.operation.opType = opCountDocuments
		v.extractCountDocumentsArgsFromMethod(mc.CountDocumentsMethod())
	case mc.EstimatedDocumentCountMethod() != nil:
		v.operation.opType = opEstimatedDocumentCount
		v.extractEstimatedDocumentCountArgs(mc.EstimatedDocumentCountMethod())
	case mc.DistinctMethod() != nil:
		v.operation.opType = opDistinct
		v.extractDistinctArgsFromMethod(mc.DistinctMethod())
	case mc.AggregateMethod() != nil:
		v.operation.opType = opAggregate
		v.extractAggregationPipelineFromMethod(mc.AggregateMethod())
	case mc.GetIndexesMethod() != nil:
		v.operation.opType = opGetIndexes

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
func (v *mongoShellVisitor) extractMethodName(mc *mongodb.MethodCallContext) string {
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
func (v *mongoShellVisitor) handleUnsupportedMethod(context, methodName string) {
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
