package translator

import (
	"fmt"

	"github.com/bytebase/parser/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func (v *visitor) extractGetCollectionInfosArgs(ctx *mongodb.GetCollectionInfosContext) {
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
	v.operation.Filter = filter

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
					v.operation.NameOnly = &val
				} else {
					v.err = fmt.Errorf("getCollectionInfos() nameOnly must be a boolean")
					return
				}
			case "authorizedCollections":
				if val, ok := opt.Value.(bool); ok {
					v.operation.AuthorizedCollections = &val
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

// extractCreateCollectionArgs extracts arguments from CreateCollectionContext.
func (v *visitor) extractCreateCollectionArgs(ctx *mongodb.CreateCollectionContext) {
	args := ctx.Arguments()
	if args == nil {
		v.err = fmt.Errorf("createCollection() requires a collection name")
		return
	}

	argsCtx, ok := args.(*mongodb.ArgumentsContext)
	if !ok {
		v.err = fmt.Errorf("createCollection() requires a collection name")
		return
	}

	allArgs := argsCtx.AllArgument()
	if len(allArgs) == 0 {
		v.err = fmt.Errorf("createCollection() requires a collection name")
		return
	}

	// First argument: collection name (required)
	firstArg, ok := allArgs[0].(*mongodb.ArgumentContext)
	if !ok {
		v.err = fmt.Errorf("createCollection() collection name must be a string")
		return
	}

	valueCtx := firstArg.Value()
	if valueCtx == nil {
		v.err = fmt.Errorf("createCollection() collection name must be a string")
		return
	}

	literalValue, ok := valueCtx.(*mongodb.LiteralValueContext)
	if !ok {
		v.err = fmt.Errorf("createCollection() collection name must be a string")
		return
	}

	stringLiteral, ok := literalValue.Literal().(*mongodb.StringLiteralValueContext)
	if !ok {
		v.err = fmt.Errorf("createCollection() collection name must be a string")
		return
	}

	v.operation.Collection = unquoteString(stringLiteral.StringLiteral().GetText())

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
			v.err = fmt.Errorf("createCollection() options must be a document")
			return
		}

		options, err := convertDocument(optionsDocValue.Document())
		if err != nil {
			v.err = fmt.Errorf("invalid options: %w", err)
			return
		}

		for _, opt := range options {
			switch opt.Key {
			case "capped":
				if val, ok := opt.Value.(bool); ok {
					v.operation.Capped = &val
				} else {
					v.err = fmt.Errorf("createCollection() capped must be a boolean")
					return
				}
			case "size":
				if val, ok := toInt64(opt.Value); ok {
					v.operation.CollectionSize = &val
				} else {
					v.err = fmt.Errorf("createCollection() size must be a number")
					return
				}
			case "max":
				if val, ok := toInt64(opt.Value); ok {
					v.operation.CollectionMax = &val
				} else {
					v.err = fmt.Errorf("createCollection() max must be a number")
					return
				}
			case "validator":
				if doc, ok := opt.Value.(bson.D); ok {
					v.operation.Validator = doc
				} else {
					v.err = fmt.Errorf("createCollection() validator must be a document")
					return
				}
			case "validationLevel":
				if val, ok := opt.Value.(string); ok {
					v.operation.ValidationLevel = val
				} else {
					v.err = fmt.Errorf("createCollection() validationLevel must be a string")
					return
				}
			case "validationAction":
				if val, ok := opt.Value.(string); ok {
					v.operation.ValidationAction = val
				} else {
					v.err = fmt.Errorf("createCollection() validationAction must be a string")
					return
				}
			default:
				v.err = &UnsupportedOptionError{
					Method: "createCollection()",
					Option: opt.Key,
				}
				return
			}
		}
	}

	if len(allArgs) > 2 {
		v.err = fmt.Errorf("createCollection() takes at most 2 arguments")
		return
	}
}
