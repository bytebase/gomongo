package gomongo

import (
	"context"

	"github.com/bytebase/gomongo/internal/executor"
	"github.com/bytebase/gomongo/internal/translator"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// execute parses and executes a MongoDB shell statement.
func execute(ctx context.Context, client *mongo.Client, database, statement string, maxRows *int64) (*Result, error) {
	op, err := translator.Parse(statement)
	if err != nil {
		// Convert internal errors to public errors
		switch e := err.(type) {
		case *translator.ParseError:
			return nil, &ParseError{
				Line:     e.Line,
				Column:   e.Column,
				Message:  e.Message,
				Found:    e.Found,
				Expected: e.Expected,
			}
		case *translator.UnsupportedOperationError:
			return nil, &UnsupportedOperationError{Operation: e.Operation}
		case *translator.PlannedOperationError:
			return nil, &PlannedOperationError{Operation: e.Operation}
		case *translator.UnsupportedOptionError:
			return nil, &UnsupportedOptionError{Method: e.Method, Option: e.Option}
		default:
			return nil, err
		}
	}

	result, err := executor.Execute(ctx, client, database, op, statement, maxRows)
	if err != nil {
		return nil, err
	}

	return &Result{
		Rows:      result.Rows,
		RowCount:  result.RowCount,
		Statement: result.Statement,
	}, nil
}
