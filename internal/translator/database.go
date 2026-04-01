package translator

import (
	"fmt"

	"github.com/bytebase/omni/mongo/ast"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func extractGetCollectionInfosArgs(op *Operation, args []ast.Node) (*Operation, error) {
	if len(args) == 0 {
		return op, nil
	}

	// First argument: filter (optional)
	filter, err := requireDocument(args, 0, "getCollectionInfos() filter")
	if err != nil {
		return nil, err
	}
	op.Filter = filter

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "getCollectionInfos() options")
		if err != nil {
			return nil, err
		}
		for _, opt := range options {
			switch opt.Key {
			case "nameOnly":
				if val, ok := opt.Value.(bool); ok {
					op.NameOnly = &val
				} else {
					return nil, fmt.Errorf("getCollectionInfos() nameOnly must be a boolean")
				}
			case "authorizedCollections":
				if val, ok := opt.Value.(bool); ok {
					op.AuthorizedCollections = &val
				} else {
					return nil, fmt.Errorf("getCollectionInfos() authorizedCollections must be a boolean")
				}
			default:
				return nil, &UnsupportedOptionError{
					Method: "getCollectionInfos()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return nil, fmt.Errorf("getCollectionInfos() takes at most 2 arguments")
	}
	return op, nil
}

func extractCreateCollectionArgs(op *Operation, args []ast.Node) (*Operation, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("createCollection() requires a collection name")
	}

	// First argument: collection name (required)
	name, err := requireString(args, 0, "createCollection() collection name")
	if err != nil {
		return nil, err
	}
	op.Collection = name

	// Second argument: options (optional)
	if len(args) >= 2 {
		options, err := requireDocument(args, 1, "createCollection() options")
		if err != nil {
			return nil, err
		}
		for _, opt := range options {
			switch opt.Key {
			case "capped":
				if val, ok := opt.Value.(bool); ok {
					op.Capped = &val
				} else {
					return nil, fmt.Errorf("createCollection() capped must be a boolean")
				}
			case "size":
				if val, ok := ToInt64(opt.Value); ok {
					op.CollectionSize = &val
				} else {
					return nil, fmt.Errorf("createCollection() size must be a number")
				}
			case "max":
				if val, ok := ToInt64(opt.Value); ok {
					op.CollectionMax = &val
				} else {
					return nil, fmt.Errorf("createCollection() max must be a number")
				}
			case "validator":
				if doc, ok := opt.Value.(bson.D); ok {
					op.Validator = doc
				} else {
					return nil, fmt.Errorf("createCollection() validator must be a document")
				}
			case "validationLevel":
				if val, ok := opt.Value.(string); ok {
					op.ValidationLevel = val
				} else {
					return nil, fmt.Errorf("createCollection() validationLevel must be a string")
				}
			case "validationAction":
				if val, ok := opt.Value.(string); ok {
					op.ValidationAction = val
				} else {
					return nil, fmt.Errorf("createCollection() validationAction must be a string")
				}
			default:
				return nil, &UnsupportedOptionError{
					Method: "createCollection()",
					Option: opt.Key,
				}
			}
		}
	}

	if len(args) > 2 {
		return nil, fmt.Errorf("createCollection() takes at most 2 arguments")
	}
	return op, nil
}
