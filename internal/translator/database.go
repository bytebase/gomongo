package translator

import (
	"fmt"

	"github.com/bytebase/parser/mongodb"
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
	// Options are currently not supported; reject calls that attempt to use them
	// to avoid silently ignoring user-specified options.
	if len(allArgs) > 1 {
		v.err = fmt.Errorf("createCollection() options argument is not supported yet")
		return
	}
}
