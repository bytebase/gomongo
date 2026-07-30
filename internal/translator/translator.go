package translator

import (
	"errors"

	"github.com/bytebase/gomongo/types"
	"github.com/bytebase/omni/mongo"
	"github.com/bytebase/omni/mongo/ast"
	"github.com/bytebase/omni/mongo/parser"
)

// Parse parses a single MongoDB shell statement and returns the operation.
// The input must contain exactly one executable statement: parse errors and
// extra statements are rejected rather than silently dropped.
//
// Input that contains no executable statements (e.g., only comments or
// whitespace) is treated as a no-op: Parse returns an Operation with
// OpType = OpNoOp and no error, mirroring mongosh, where evaluating a
// comment-only line is silently successful.
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

	var node ast.Node
	for _, s := range stmts {
		if s.Empty() {
			continue
		}
		if node != nil {
			return nil, &ParseError{
				Line:    s.Start.Line,
				Column:  s.Start.Column,
				Message: "expected a single statement, got multiple",
			}
		}
		node = s.AST
	}
	if node == nil {
		// Comment-only / whitespace-only input: no-op, no error.
		return &Operation{OpType: types.OpNoOp}, nil
	}
	return translateNode(node)
}
