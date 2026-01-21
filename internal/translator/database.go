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
