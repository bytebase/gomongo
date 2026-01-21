package translator

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mongodb"
)

// Parse parses a MongoDB shell statement and returns the operation.
func Parse(statement string) (*Operation, error) {
	tree, parseErrors := parseMongoShell(statement)
	if len(parseErrors) > 0 {
		return nil, &ParseError{
			Line:    parseErrors[0].Line,
			Column:  parseErrors[0].Column,
			Message: parseErrors[0].Message,
		}
	}

	visitor := newVisitor()
	visitor.Visit(tree)
	if visitor.err != nil {
		return nil, visitor.err
	}

	return visitor.operation, nil
}

// parseMongoShell parses a MongoDB shell statement and returns the parse tree.
func parseMongoShell(statement string) (mongodb.IProgramContext, []*mongodb.MongoShellParseError) {
	is := antlr.NewInputStream(statement)
	lexer := mongodb.NewMongoShellLexer(is)

	errorListener := mongodb.NewMongoShellErrorListener()
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errorListener)

	stream := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	parser := mongodb.NewMongoShellParser(stream)

	parser.RemoveErrorListeners()
	parser.AddErrorListener(errorListener)

	parser.BuildParseTrees = true
	tree := parser.Program()

	return tree, errorListener.Errors
}
