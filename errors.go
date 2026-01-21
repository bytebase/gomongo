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
// This is returned for operations that are not planned for implementation.
type UnsupportedOperationError struct {
	Operation string
}

func (e *UnsupportedOperationError) Error() string {
	return fmt.Sprintf("unsupported operation: %s", e.Operation)
}

// PlannedOperationError represents an operation that is planned but not yet implemented.
// When the caller receives this error, it should fallback to mongosh.
type PlannedOperationError struct {
	Operation string
}

func (e *PlannedOperationError) Error() string {
	return fmt.Sprintf("operation %s is not yet implemented", e.Operation)
}
