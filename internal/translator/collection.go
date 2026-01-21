package translator

import (
	"fmt"
	"strconv"

	"github.com/bytebase/parser/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (v *visitor) extractFindArgs(ctx mongodb.IFindMethodContext) {
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
		v.operation.Filter = filter
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
			v.operation.Projection = projection
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
					v.operation.Hint = opt.Value
				case "max":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.Max = doc
					} else {
						v.err = fmt.Errorf("find() max must be a document")
						return
					}
				case "min":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.Min = doc
					} else {
						v.err = fmt.Errorf("find() min must be a document")
						return
					}
				case "maxTimeMS":
					if val, ok := opt.Value.(int32); ok {
						ms := int64(val)
						v.operation.MaxTimeMS = &ms
					} else if val, ok := opt.Value.(int64); ok {
						v.operation.MaxTimeMS = &val
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

func (v *visitor) extractFindOneArgs(ctx mongodb.IFindOneMethodContext) {
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
		v.operation.Filter = filter
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
			v.operation.Projection = projection
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
					v.operation.Hint = opt.Value
				case "max":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.Max = doc
					} else {
						v.err = fmt.Errorf("findOne() max must be a document")
						return
					}
				case "min":
					if doc, ok := opt.Value.(bson.D); ok {
						v.operation.Min = doc
					} else {
						v.err = fmt.Errorf("findOne() min must be a document")
						return
					}
				case "maxTimeMS":
					if val, ok := opt.Value.(int32); ok {
						ms := int64(val)
						v.operation.MaxTimeMS = &ms
					} else if val, ok := opt.Value.(int64); ok {
						v.operation.MaxTimeMS = &val
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

func (v *visitor) extractSort(ctx mongodb.ISortMethodContext) {
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
	v.operation.Sort = sort
}

func (v *visitor) extractLimit(ctx mongodb.ILimitMethodContext) {
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
	v.operation.Limit = &limit
}

func (v *visitor) extractSkip(ctx mongodb.ISkipMethodContext) {
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
	v.operation.Skip = &skip
}

func (v *visitor) extractProjection(ctx mongodb.IProjectionMethodContext) {
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
	v.operation.Projection = projection
}

func (v *visitor) extractHint(ctx mongodb.IHintMethodContext) {
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
		v.operation.Hint = unquoteString(strLit.StringLiteral().GetText())
	case *mongodb.DocumentValueContext:
		doc, err := convertDocument(val.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid hint: %w", err)
			return
		}
		v.operation.Hint = doc
	default:
		v.err = fmt.Errorf("hint() argument must be a string or document")
	}
}

func (v *visitor) extractMax(ctx mongodb.IMaxMethodContext) {
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
	v.operation.Max = maxDoc
}

func (v *visitor) extractMin(ctx mongodb.IMinMethodContext) {
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
	v.operation.Min = minDoc
}

// extractAggregationPipelineFromMethod extracts pipeline from AggregateMethodContext.
func (v *visitor) extractAggregationPipelineFromMethod(ctx mongodb.IAggregateMethodContext) {
	method, ok := ctx.(*mongodb.AggregateMethodContext)
	if !ok {
		return
	}
	v.extractArgumentsForAggregate(method.Arguments())
}

// extractArgumentsForAggregate extracts aggregate pipeline from IArgumentsContext.
func (v *visitor) extractArgumentsForAggregate(args mongodb.IArgumentsContext) {
	if args == nil {
		// Empty pipeline: aggregate()
		v.operation.Pipeline = bson.A{}
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("aggregate() requires an array argument")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.operation.Pipeline = bson.A{}
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

	v.operation.Pipeline = pipeline

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
				v.operation.Hint = opt.Value
			case "maxTimeMS":
				if val, ok := opt.Value.(int32); ok {
					ms := int64(val)
					v.operation.MaxTimeMS = &ms
				} else if val, ok := opt.Value.(int64); ok {
					v.operation.MaxTimeMS = &val
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

// extractCountDocumentsArgsFromMethod extracts arguments from CountDocumentsMethodContext.
func (v *visitor) extractCountDocumentsArgsFromMethod(ctx mongodb.ICountDocumentsMethodContext) {
	method, ok := ctx.(*mongodb.CountDocumentsMethodContext)
	if !ok {
		return
	}
	v.extractArgumentsForCountDocuments(method.Arguments())
}

// extractArgumentsForCountDocuments extracts countDocuments arguments from IArgumentsContext.
func (v *visitor) extractArgumentsForCountDocuments(args mongodb.IArgumentsContext) {
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
	v.operation.Filter = filter

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
			v.operation.Hint = elem.Value
		case "limit":
			if val, ok := elem.Value.(int32); ok {
				limit := int64(val)
				v.operation.Limit = &limit
			} else if val, ok := elem.Value.(int64); ok {
				v.operation.Limit = &val
			}
		case "skip":
			if val, ok := elem.Value.(int32); ok {
				skip := int64(val)
				v.operation.Skip = &skip
			} else if val, ok := elem.Value.(int64); ok {
				v.operation.Skip = &val
			}
		case "maxTimeMS":
			if val, ok := elem.Value.(int32); ok {
				ms := int64(val)
				v.operation.MaxTimeMS = &ms
			} else if val, ok := elem.Value.(int64); ok {
				v.operation.MaxTimeMS = &val
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
func (v *visitor) extractEstimatedDocumentCountArgs(ctx mongodb.IEstimatedDocumentCountMethodContext) {
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
				v.operation.MaxTimeMS = &ms
			} else if val, ok := opt.Value.(int64); ok {
				v.operation.MaxTimeMS = &val
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
func (v *visitor) extractDistinctArgsFromMethod(ctx mongodb.IDistinctMethodContext) {
	method, ok := ctx.(*mongodb.DistinctMethodContext)
	if !ok {
		return
	}
	v.extractArgumentsForDistinct(method.Arguments())
}

// extractArgumentsForDistinct extracts distinct arguments from IArgumentsContext.
func (v *visitor) extractArgumentsForDistinct(args mongodb.IArgumentsContext) {
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

	v.operation.DistinctField = unquoteString(stringLiteral.StringLiteral().GetText())

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
	v.operation.Filter = filter

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
					v.operation.MaxTimeMS = &ms
				} else if val, ok := opt.Value.(int64); ok {
					v.operation.MaxTimeMS = &val
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
