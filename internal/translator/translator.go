package translator

import (
	"errors"
	"fmt"

	"github.com/bytebase/omni/mongo"
	"github.com/bytebase/omni/mongo/parser"
)

// Parse parses a MongoDB shell statement and returns the operation.
func Parse(statement string) (*Operation, error) {
	stmts, err := mongo.Parse(statement)
	if err != nil {
		var pe *parser.ParseError
		if errors.As(err, &pe) {
			return nil, &ParseError{
				Line:    pe.Line,
				Column:  pe.Column,
				Message: pe.Message,
			}
		}
		return nil, err
	}

	// Find the first non-empty statement.
	for _, s := range stmts {
		if !s.Empty() {
			return translateNode(s.AST)
		}
	}

	return nil, &ParseError{Message: fmt.Sprintf("empty statement: %s", statement)}
}
