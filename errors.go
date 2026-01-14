package gomongo

import "fmt"

// ParseError represents a syntax error during parsing.
type ParseError struct {
	Line     int
	Column   int
	Message  string
	Found    string
	Expected string
}

func (e *ParseError) Error() string {
	if e.Found != "" && e.Expected != "" {
		return fmt.Sprintf("parse error at line %d, column %d: found %q, expected %s", e.Line, e.Column, e.Found, e.Expected)
	}
	return fmt.Sprintf("parse error at line %d, column %d: %s", e.Line, e.Column, e.Message)
}

// UnsupportedOperationError represents an unsupported operation.
type UnsupportedOperationError struct {
	Operation string
	Hint      string
}

func (e *UnsupportedOperationError) Error() string {
	if e.Hint != "" {
		return fmt.Sprintf("unsupported operation %q: %s", e.Operation, e.Hint)
	}
	return fmt.Sprintf("unsupported operation %q", e.Operation)
}
