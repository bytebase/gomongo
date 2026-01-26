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

// extractInsertOneArgs extracts arguments from InsertOneMethodContext.
func (v *visitor) extractInsertOneArgs(ctx mongodb.IInsertOneMethodContext) {
	method, ok := ctx.(*mongodb.InsertOneMethodContext)
	if !ok {
		return
	}

	args := method.Arguments()
	if args == nil {
		v.err = fmt.Errorf("insertOne() requires a document argument")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("insertOne() requires a document argument")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("insertOne() requires a document argument")
		return
	}

	// First argument: document (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("insertOne() requires a document argument")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("insertOne() requires a document argument")
		return
	}

	docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("insertOne() document must be an object")
		return
	}

	doc, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid document: %w", err)
		return
	}
	v.operation.Document = doc

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

		optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("insertOne() options must be a document")
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					v.operation.BypassDocumentValidation = &val
				} else {
					v.err = fmt.Errorf("insertOne() bypassDocumentValidation must be a boolean")
					return
				}
			case "comment":
				v.operation.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.WriteConcern = doc
				} else {
					v.err = fmt.Errorf("insertOne() writeConcern must be a document")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "insertOne()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("insertOne() takes at most 2 arguments")
		return
	}
}

// extractUpdateOneArgs extracts arguments from UpdateOneMethodContext.
func (v *visitor) extractUpdateOneArgs(ctx mongodb.IUpdateOneMethodContext) {
	method, ok := ctx.(*mongodb.UpdateOneMethodContext)
	if !ok {
		return
	}
	v.extractUpdateArgs("updateOne", method.Arguments())
}

// extractUpdateManyArgs extracts arguments from UpdateManyMethodContext.
func (v *visitor) extractUpdateManyArgs(ctx mongodb.IUpdateManyMethodContext) {
	method, ok := ctx.(*mongodb.UpdateManyMethodContext)
	if !ok {
		return
	}
	v.extractUpdateArgs("updateMany", method.Arguments())
}

// extractUpdateArgs is shared between updateOne and updateMany.
func (v *visitor) extractUpdateArgs(methodName string, args mongodb.IArgumentsContext) {
	if args == nil {
		v.err = fmt.Errorf("%s() requires filter and update arguments", methodName)
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("%s() requires filter and update arguments", methodName)
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) < 2 {
		v.err = fmt.Errorf("%s() requires filter and update arguments", methodName)
		return
	}

	// First argument: filter (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("%s() filter must be a document", methodName)
		return
	}

	filterValueCtx := firstArg.Value()
	if filterValueCtx == nil {
		v.err = fmt.Errorf("%s() filter must be a document", methodName)
		return
	}

	filterDocValue, ok := filterValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("%s() filter must be a document", methodName)
		return
	}

	filter, err := convertDocument(filterDocValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.Filter = filter

	// Second argument: update (required) - can be document or pipeline
	secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("%s() update must be a document or array", methodName)
		return
	}

	updateValueCtx := secondArg.Value()
	if updateValueCtx == nil {
		v.err = fmt.Errorf("%s() update must be a document or array", methodName)
		return
	}

	switch uv := updateValueCtx.(type) {
	case *mongodb.DocumentValueContext:
		update, err := convertDocument(uv.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid update: %w", err)
			return
		}
		v.operation.Update = update
	case *mongodb.ArrayValueContext:
		// Aggregation pipeline update
		pipeline, err := convertArray(uv.Array())
		if err != nil {
			v.err = fmt.Errorf("invalid update pipeline: %w", err)
			return
		}
		v.operation.Update = pipeline
	default:
		v.err = fmt.Errorf("%s() update must be a document or array", methodName)
		return
	}

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

		optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("%s() options must be a document", methodName)
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "upsert":
				if val, ok := opt.Value.(bool); ok {
					v.operation.Upsert = &val
				} else {
					v.err = fmt.Errorf("%s() upsert must be a boolean", methodName)
					return
				}
			case "hint":
				v.operation.Hint = opt.Value
			case "collation":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Collation = doc
				} else {
					v.err = fmt.Errorf("%s() collation must be a document", methodName)
					return
				}
			case "arrayFilters":
				if arr, ok := opt.Value.(bson.A); ok {
					v.operation.ArrayFilters = arr
				} else {
					v.err = fmt.Errorf("%s() arrayFilters must be an array", methodName)
					return
				}
			case "let":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Let = doc
				} else {
					v.err = fmt.Errorf("%s() let must be a document", methodName)
					return
				}
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					v.operation.BypassDocumentValidation = &val
				} else {
					v.err = fmt.Errorf("%s() bypassDocumentValidation must be a boolean", methodName)
					return
				}
			case "comment":
				v.operation.Comment = opt.Value
			case "sort":
				// sort is only valid for updateOne (MongoDB 8.0+)
				if methodName != "updateOne" {
					v.err = &UnsupportedOptionError{
						Method: methodName + "()",
						Option: opt.Key,
					}
					return
				}
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Sort = doc
				} else {
					v.err = fmt.Errorf("%s() sort must be a document", methodName)
					return
				}
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.WriteConcern = doc
				} else {
					v.err = fmt.Errorf("%s() writeConcern must be a document", methodName)
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: methodName + "()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 3 {
		v.err = fmt.Errorf("%s() takes at most 3 arguments", methodName)
		return
	}
}

// extractInsertManyArgs extracts arguments from InsertManyMethodContext.
func (v *visitor) extractInsertManyArgs(ctx mongodb.IInsertManyMethodContext) {
	method, ok := ctx.(*mongodb.InsertManyMethodContext)
	if !ok {
		return
	}

	args := method.Arguments()
	if args == nil {
		v.err = fmt.Errorf("insertMany() requires an array argument")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("insertMany() requires an array argument")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("insertMany() requires an array argument")
		return
	}

	// First argument: array of documents (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("insertMany() requires an array argument")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("insertMany() requires an array argument")
		return
	}

	arrayValue, ok := valueCtx.(*mongodb.ArrayValueContext)
	if !ok {
		v.err = fmt.Errorf("insertMany() requires an array argument")
		return
	}

	arr, err := convertArray(arrayValue.Array())
	if err != nil {
		v.err = fmt.Errorf("invalid documents array: %w", err)
		return
	}

	// Convert array elements to bson.D
	var docs []bson.D
	for i, elem := range arr {
		doc, ok := elem.(bson.D)
		if !ok {
			v.err = fmt.Errorf("insertMany() element %d must be a document", i)
			return
		}
		docs = append(docs, doc)
	}
	v.operation.Documents = docs

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

		optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("insertMany() options must be a document")
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "ordered":
				if val, ok := opt.Value.(bool); ok {
					v.operation.Ordered = &val
				} else {
					v.err = fmt.Errorf("insertMany() ordered must be a boolean")
					return
				}
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					v.operation.BypassDocumentValidation = &val
				} else {
					v.err = fmt.Errorf("insertMany() bypassDocumentValidation must be a boolean")
					return
				}
			case "comment":
				v.operation.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.WriteConcern = doc
				} else {
					v.err = fmt.Errorf("insertMany() writeConcern must be a document")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "insertMany()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("insertMany() takes at most 2 arguments")
		return
	}
}

// extractReplaceOneArgs extracts arguments from ReplaceOneMethodContext.
func (v *visitor) extractReplaceOneArgs(ctx mongodb.IReplaceOneMethodContext) {
	method, ok := ctx.(*mongodb.ReplaceOneMethodContext)
	if !ok {
		return
	}

	args := method.Arguments()
	if args == nil {
		v.err = fmt.Errorf("replaceOne() requires filter and replacement arguments")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("replaceOne() requires filter and replacement arguments")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) < 2 {
		v.err = fmt.Errorf("replaceOne() requires filter and replacement arguments")
		return
	}

	// First argument: filter
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	filterValueCtx := firstArg.Value()
	if filterValueCtx == nil {
		return
	}

	filterDocValue, ok := filterValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("replaceOne() filter must be a document")
		return
	}

	filter, err := convertDocument(filterDocValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.Filter = filter

	// Second argument: replacement document
	secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	replacementValueCtx := secondArg.Value()
	if replacementValueCtx == nil {
		return
	}

	replacementDocValue, ok := replacementValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("replaceOne() replacement must be a document")
		return
	}

	replacement, err := convertDocument(replacementDocValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid replacement: %w", err)
		return
	}
	v.operation.Replacement = replacement

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

		optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("replaceOne() options must be a document")
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "upsert":
				if val, ok := opt.Value.(bool); ok {
					v.operation.Upsert = &val
				} else {
					v.err = fmt.Errorf("replaceOne() upsert must be a boolean")
					return
				}
			case "hint":
				v.operation.Hint = opt.Value
			case "collation":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Collation = doc
				} else {
					v.err = fmt.Errorf("replaceOne() collation must be a document")
					return
				}
			case "let":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Let = doc
				} else {
					v.err = fmt.Errorf("replaceOne() let must be a document")
					return
				}
			case "bypassDocumentValidation":
				if val, ok := opt.Value.(bool); ok {
					v.operation.BypassDocumentValidation = &val
				} else {
					v.err = fmt.Errorf("replaceOne() bypassDocumentValidation must be a boolean")
					return
				}
			case "comment":
				v.operation.Comment = opt.Value
			case "sort":
				// sort is supported for replaceOne (MongoDB 8.0+)
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Sort = doc
				} else {
					v.err = fmt.Errorf("replaceOne() sort must be a document")
					return
				}
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.WriteConcern = doc
				} else {
					v.err = fmt.Errorf("replaceOne() writeConcern must be a document")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "replaceOne()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 3 {
		v.err = fmt.Errorf("replaceOne() takes at most 3 arguments")
		return
	}
}

// extractDeleteOneArgs extracts arguments from DeleteOneMethodContext.
func (v *visitor) extractDeleteOneArgs(ctx mongodb.IDeleteOneMethodContext) {
	method, ok := ctx.(*mongodb.DeleteOneMethodContext)
	if !ok {
		return
	}
	v.extractDeleteArgs("deleteOne", method.Arguments())
}

// extractDeleteManyArgs extracts arguments from DeleteManyMethodContext.
func (v *visitor) extractDeleteManyArgs(ctx mongodb.IDeleteManyMethodContext) {
	method, ok := ctx.(*mongodb.DeleteManyMethodContext)
	if !ok {
		return
	}
	v.extractDeleteArgs("deleteMany", method.Arguments())
}

// extractDeleteArgs is shared between deleteOne and deleteMany.
func (v *visitor) extractDeleteArgs(methodName string, args mongodb.IArgumentsContext) {
	if args == nil {
		v.err = fmt.Errorf("%s() requires a filter argument", methodName)
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("%s() requires a filter argument", methodName)
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("%s() requires a filter argument", methodName)
		return
	}

	// First argument: filter (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	filterValueCtx := firstArg.Value()
	if filterValueCtx == nil {
		return
	}

	filterDocValue, ok := filterValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("%s() filter must be a document", methodName)
		return
	}

	filter, err := convertDocument(filterDocValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.Filter = filter

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

		optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("%s() options must be a document", methodName)
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "hint":
				v.operation.Hint = opt.Value
			case "collation":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Collation = doc
				} else {
					v.err = fmt.Errorf("%s() collation must be a document", methodName)
					return
				}
			case "let":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Let = doc
				} else {
					v.err = fmt.Errorf("%s() let must be a document", methodName)
					return
				}
			case "comment":
				v.operation.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.WriteConcern = doc
				} else {
					v.err = fmt.Errorf("%s() writeConcern must be a document", methodName)
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: methodName + "()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("%s() takes at most 2 arguments", methodName)
		return
	}
}

// extractFindOneAndUpdateArgs extracts arguments from FindOneAndUpdateMethodContext.
func (v *visitor) extractFindOneAndUpdateArgs(ctx mongodb.IFindOneAndUpdateMethodContext) {
	method, ok := ctx.(*mongodb.FindOneAndUpdateMethodContext)
	if !ok {
		return
	}
	v.extractFindOneAndModifyArgs("findOneAndUpdate", method.Arguments(), true)
}

// extractFindOneAndReplaceArgs extracts arguments from FindOneAndReplaceMethodContext.
func (v *visitor) extractFindOneAndReplaceArgs(ctx mongodb.IFindOneAndReplaceMethodContext) {
	method, ok := ctx.(*mongodb.FindOneAndReplaceMethodContext)
	if !ok {
		return
	}
	v.extractFindOneAndModifyArgs("findOneAndReplace", method.Arguments(), true)
}

// extractFindOneAndDeleteArgs extracts arguments from FindOneAndDeleteMethodContext.
func (v *visitor) extractFindOneAndDeleteArgs(ctx mongodb.IFindOneAndDeleteMethodContext) {
	method, ok := ctx.(*mongodb.FindOneAndDeleteMethodContext)
	if !ok {
		return
	}
	v.extractFindOneAndModifyArgs("findOneAndDelete", method.Arguments(), false)
}

// extractFindOneAndModifyArgs handles arguments for findOneAndUpdate/Replace/Delete.
// hasUpdate indicates whether the second arg is update/replacement (true) or not (false for delete).
func (v *visitor) extractFindOneAndModifyArgs(methodName string, args mongodb.IArgumentsContext, hasUpdate bool) {
	if args == nil {
		if hasUpdate {
			v.err = fmt.Errorf("%s() requires filter and update arguments", methodName)
		} else {
			v.err = fmt.Errorf("%s() requires a filter argument", methodName)
		}
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		return
	}

	allArgs := argsCtx.AllArgument()
	minArgs := 1
	if hasUpdate {
		minArgs = 2
	}
	if len(allArgs) < minArgs {
		if hasUpdate {
			v.err = fmt.Errorf("%s() requires filter and update arguments", methodName)
		} else {
			v.err = fmt.Errorf("%s() requires a filter argument", methodName)
		}
		return
	}

	// First argument: filter
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	filterValueCtx := firstArg.Value()
	if filterValueCtx == nil {
		return
	}

	filterDocValue, ok := filterValueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("%s() filter must be a document", methodName)
		return
	}

	filter, err := convertDocument(filterDocValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid filter: %w", err)
		return
	}
	v.operation.Filter = filter

	optionsArgIdx := 1
	if hasUpdate {
		// Second argument: update/replacement
		secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
		if !ok {
			return
		}

		updateValueCtx := secondArg.Value()
		if updateValueCtx == nil {
			return
		}

		if methodName == "findOneAndReplace" {
			// Replacement must be a document
			docValue, ok := updateValueCtx.(*mongodb.DocumentValueContext)
			if !ok {
				v.err = fmt.Errorf("%s() replacement must be a document", methodName)
				return
			}
			replacement, err := convertDocument(docValue.Document())
			if err != nil {
				v.err = fmt.Errorf("invalid replacement: %w", err)
				return
			}
			v.operation.Replacement = replacement
		} else {
			// Update can be document or pipeline
			switch uv := updateValueCtx.(type) {
			case *mongodb.DocumentValueContext:
				update, err := convertDocument(uv.Document())
				if err != nil {
					v.err = fmt.Errorf("invalid update: %w", err)
					return
				}
				v.operation.Update = update
			case *mongodb.ArrayValueContext:
				pipeline, err := convertArray(uv.Array())
				if err != nil {
					v.err = fmt.Errorf("invalid update pipeline: %w", err)
					return
				}
				v.operation.Update = pipeline
			default:
				v.err = fmt.Errorf("%s() update must be a document or array", methodName)
				return
			}
		}
		optionsArgIdx = 2
	}

	// Options argument
	if len(allArgs) > optionsArgIdx {
		optArg, ok := allArgs[optionsArgIdx].(*mongodb.ArgumentContext)
		if !ok {
			return
		}

		optionsValueCtx := optArg.Value()
		if optionsValueCtx == nil {
			return
		}

		optionsDocValue, ok := optionsValueCtx.(*mongodb.DocumentValueContext)
		if !ok {
			v.err = fmt.Errorf("%s() options must be a document", methodName)
			return
		}

		opts, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range opts {
			switch opt.Key {
			case "upsert":
				if methodName == "findOneAndDelete" {
					v.err = &UnsupportedOptionError{Method: methodName + "()", Option: opt.Key}
					return
				}
				if val, ok := opt.Value.(bool); ok {
					v.operation.Upsert = &val
				} else {
					v.err = fmt.Errorf("%s() upsert must be a boolean", methodName)
					return
				}
			case "returnDocument":
				if val, ok := opt.Value.(string); ok {
					if val != "before" && val != "after" {
						v.err = fmt.Errorf("%s() returnDocument must be 'before' or 'after'", methodName)
						return
					}
					v.operation.ReturnDocument = &val
				} else {
					v.err = fmt.Errorf("%s() returnDocument must be a string", methodName)
					return
				}
			case "projection":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Projection = doc
				} else {
					v.err = fmt.Errorf("%s() projection must be a document", methodName)
					return
				}
			case "sort":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Sort = doc
				} else {
					v.err = fmt.Errorf("%s() sort must be a document", methodName)
					return
				}
			case "hint":
				v.operation.Hint = opt.Value
			case "collation":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Collation = doc
				} else {
					v.err = fmt.Errorf("%s() collation must be a document", methodName)
					return
				}
			case "arrayFilters":
				if methodName == "findOneAndDelete" || methodName == "findOneAndReplace" {
					v.err = &UnsupportedOptionError{Method: methodName + "()", Option: opt.Key}
					return
				}
				if arr, ok := opt.Value.(bson.A); ok {
					v.operation.ArrayFilters = arr
				} else {
					v.err = fmt.Errorf("%s() arrayFilters must be an array", methodName)
					return
				}
			case "let":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Let = doc
				} else {
					v.err = fmt.Errorf("%s() let must be a document", methodName)
					return
				}
			case "bypassDocumentValidation":
				if methodName == "findOneAndDelete" {
					v.err = &UnsupportedOptionError{Method: methodName + "()", Option: opt.Key}
					return
				}
				if val, ok := opt.Value.(bool); ok {
					v.operation.BypassDocumentValidation = &val
				} else {
					v.err = fmt.Errorf("%s() bypassDocumentValidation must be a boolean", methodName)
					return
				}
			case "comment":
				v.operation.Comment = opt.Value
			case "writeConcern":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.WriteConcern = doc
				} else {
					v.err = fmt.Errorf("%s() writeConcern must be a document", methodName)
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: methodName + "()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	maxArgs := optionsArgIdx + 1
	if len(allArgs) > maxArgs {
		v.err = fmt.Errorf("%s() takes at most %d arguments", methodName, maxArgs)
		return
	}
}

// extractCreateIndexArgs extracts arguments from CreateIndexMethodContext.
func (v *visitor) extractCreateIndexArgs(ctx mongodb.ICreateIndexMethodContext) {
	method, ok := ctx.(*mongodb.CreateIndexMethodContext)
	if !ok {
		return
	}

	args := method.Arguments()
	if args == nil {
		v.err = fmt.Errorf("createIndex() requires a key specification")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("createIndex() requires a key specification")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("createIndex() requires a key specification")
		return
	}

	// First argument: keys document (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("createIndex() key specification must be a document")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("createIndex() key specification must be a document")
		return
	}

	docValue, ok := valueCtx.(*mongodb.DocumentValueContext)
	if !ok {
		v.err = fmt.Errorf("createIndex() key specification must be a document")
		return
	}

	keys, err := convertDocument(docValue.Document())
	if err != nil {
		v.err = fmt.Errorf("invalid key specification: %w", err)
		return
	}
	v.operation.IndexKeys = keys

	// Second argument: options (optional)
	// Currently only the "name" option is supported.
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
			v.err = fmt.Errorf("createIndex() options must be a document")
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "name":
				if val, ok := opt.Value.(string); ok {
					v.operation.IndexName = val
				} else {
					v.err = fmt.Errorf("createIndex() name must be a string")
					return
				}
			case "unique":
				if val, ok := opt.Value.(bool); ok {
					v.operation.IndexUnique = &val
				} else {
					v.err = fmt.Errorf("createIndex() unique must be a boolean")
					return
				}
			case "sparse":
				if val, ok := opt.Value.(bool); ok {
					v.operation.IndexSparse = &val
				} else {
					v.err = fmt.Errorf("createIndex() sparse must be a boolean")
					return
				}
			case "expireAfterSeconds":
				if val, ok := toInt32(opt.Value); ok {
					v.operation.IndexTTL = &val
				} else {
					v.err = fmt.Errorf("createIndex() expireAfterSeconds must be a number")
					return
				}
			case "background":
				// background option is deprecated but still accepted for compatibility
				// The Go driver ignores it since MongoDB 4.2
				if _, ok := opt.Value.(bool); !ok {
					v.err = fmt.Errorf("createIndex() background must be a boolean")
					return
				}
				// Silently ignore - it's deprecated and has no effect
			default:
				v.err = &UnsupportedOptionError{
					Method: "createIndex()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("createIndex() takes at most 2 arguments")
		return
	}
}

// extractDropIndexArgs extracts arguments from DropIndexMethodContext.
func (v *visitor) extractDropIndexArgs(ctx mongodb.IDropIndexMethodContext) {
	method, ok := ctx.(*mongodb.DropIndexMethodContext)
	if !ok {
		return
	}

	arg := method.Argument()
	if arg == nil {
		v.err = fmt.Errorf("dropIndex() requires an index name or key specification")
		return
	}

	argCtx, ok := arg.(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	valueCtx := argCtx.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("dropIndex() requires an index name or key specification")
		return
	}

	// dropIndex can accept a string (index name) or document (index key spec)
	switch val := valueCtx.(type) {
	case *mongodb.LiteralValueContext:
		strLit, ok := val.Literal().(*mongodb.StringLiteralValueContext)
		if !ok {
			v.err = fmt.Errorf("dropIndex() argument must be a string or document")
			return
		}
		v.operation.IndexName = unquoteString(strLit.StringLiteral().GetText())
	case *mongodb.DocumentValueContext:
		doc, err := convertDocument(val.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid index specification: %w", err)
			return
		}
		v.operation.IndexKeys = doc
	default:
		v.err = fmt.Errorf("dropIndex() argument must be a string or document")
	}
}

// extractDropIndexesArgs extracts arguments from DropIndexesMethodContext.
func (v *visitor) extractDropIndexesArgs(ctx mongodb.IDropIndexesMethodContext) {
	method, ok := ctx.(*mongodb.DropIndexesMethodContext)
	if !ok {
		return
	}

	// dropIndexes() can be called without arguments (drops all indexes except _id)
	// or with a single argument (index name, array of names, or "*")
	arg := method.Argument()
	if arg == nil {
		// No argument means drop all indexes (represented as "*" to the driver)
		v.operation.IndexName = "*"
		return
	}

	argCtx, ok := arg.(*mongodb.ArgumentContext)
	if !ok {
		return
	}

	valueCtx := argCtx.Value()
	if valueCtx == nil {
		v.operation.IndexName = "*"
		return
	}

	switch val := valueCtx.(type) {
	case *mongodb.LiteralValueContext:
		strLit, ok := val.Literal().(*mongodb.StringLiteralValueContext)
		if !ok {
			v.err = fmt.Errorf("dropIndexes() argument must be a string or array")
			return
		}
		v.operation.IndexName = unquoteString(strLit.StringLiteral().GetText())
	case *mongodb.ArrayValueContext:
		// Array of index names - iterate and extract each name
		arr, err := convertArray(val.Array())
		if err != nil {
			v.err = fmt.Errorf("invalid index names array: %w", err)
			return
		}
		var indexNames []string
		for i, elem := range arr {
			name, ok := elem.(string)
			if !ok {
				v.err = fmt.Errorf("dropIndexes() array element %d must be a string", i)
				return
			}
			indexNames = append(indexNames, name)
		}
		v.operation.IndexNames = indexNames
	default:
		v.err = fmt.Errorf("dropIndexes() argument must be a string or array")
	}
}

// extractRenameCollectionArgs extracts arguments from RenameCollectionMethodContext.
func (v *visitor) extractRenameCollectionArgs(ctx mongodb.IRenameCollectionMethodContext) {
	method, ok := ctx.(*mongodb.RenameCollectionMethodContext)
	if !ok {
		return
	}

	args := method.Arguments()
	if args == nil {
		v.err = fmt.Errorf("renameCollection() requires a new collection name")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("renameCollection() requires a new collection name")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("renameCollection() requires a new collection name")
		return
	}

	// First argument: new collection name (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("renameCollection() new name must be a string")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("renameCollection() new name must be a string")
		return
	}

	literalValue, ok := valueCtx.(*mongodb.LiteralValueContext)
	if !ok {
		v.err = fmt.Errorf("renameCollection() new name must be a string")
		return
	}

	stringLiteral, ok := literalValue.Literal().(*mongodb.StringLiteralValueContext)
	if !ok {
		v.err = fmt.Errorf("renameCollection() new name must be a string")
		return
	}

	v.operation.NewName = unquoteString(stringLiteral.StringLiteral().GetText())

	// Second argument: dropTarget boolean (optional)
	if len(allArgs) >= 2 {
		secondArg, ok := allArgs[1].(*mongodb.ArgumentContext)
		if !ok {
			return
		}

		dropTargetValueCtx := secondArg.Value()
		if dropTargetValueCtx == nil {
			return
		}

		literalVal, ok := dropTargetValueCtx.(*mongodb.LiteralValueContext)
		if !ok {
			v.err = fmt.Errorf("renameCollection() dropTarget must be a boolean")
			return
		}

		switch literalVal.Literal().(type) {
		case *mongodb.TrueLiteralContext:
			dropTarget := true
			v.operation.DropTarget = &dropTarget
		case *mongodb.FalseLiteralContext:
			dropTarget := false
			v.operation.DropTarget = &dropTarget
		default:
			v.err = fmt.Errorf("renameCollection() dropTarget must be a boolean")
			return
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("renameCollection() takes at most 2 arguments")
		return
	}
}
