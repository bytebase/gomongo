package gomongo

import (
	"github.com/antlr4-go/antlr/v4"
	"github.com/bytebase/parser/mongodb"
)

type operationType int

const (
	opUnknown operationType = iota
	opFind
)

// mongoOperation represents a parsed MongoDB operation.
type mongoOperation struct {
	opType     operationType
	collection string
}

// mongoShellVisitor extracts operations from a parse tree.
type mongoShellVisitor struct {
	mongodb.BaseMongoShellParserVisitor
	operation *mongoOperation
	err       error
}

func newMongoShellVisitor() *mongoShellVisitor {
	return &mongoShellVisitor{
		operation: &mongoOperation{opType: opUnknown},
	}
}

func (v *mongoShellVisitor) Visit(tree antlr.ParseTree) any {
	return tree.Accept(v)
}

func (v *mongoShellVisitor) VisitProgram(ctx *mongodb.ProgramContext) any {
	v.visitProgram(ctx)
	return nil
}

func (v *mongoShellVisitor) visitProgram(ctx mongodb.IProgramContext) {
	for _, stmt := range ctx.AllStatement() {
		v.visitStatement(stmt)
		if v.err != nil {
			return
		}
	}
}

func (v *mongoShellVisitor) VisitStatement(ctx *mongodb.StatementContext) any {
	v.visitStatement(ctx)
	return nil
}

func (v *mongoShellVisitor) visitStatement(ctx mongodb.IStatementContext) {
	if ctx.DbStatement() != nil {
		v.visitDbStatement(ctx.DbStatement())
	} else if ctx.ShellCommand() != nil {
		v.err = &UnsupportedOperationError{
			Operation: ctx.ShellCommand().GetText(),
			Hint:      "shell commands not yet supported",
		}
	}
}

func (v *mongoShellVisitor) visitDbStatement(ctx mongodb.IDbStatementContext) {
	switch c := ctx.(type) {
	case *mongodb.CollectionOperationContext:
		v.visitCollectionOperation(c)
	case *mongodb.GetCollectionNamesContext:
		v.err = &UnsupportedOperationError{
			Operation: "getCollectionNames",
			Hint:      "not yet supported",
		}
	}
}

func (v *mongoShellVisitor) VisitCollectionOperation(ctx *mongodb.CollectionOperationContext) any {
	v.visitCollectionOperation(ctx)
	return nil
}

func (v *mongoShellVisitor) visitCollectionOperation(ctx *mongodb.CollectionOperationContext) {
	v.operation.collection = v.extractCollectionName(ctx.CollectionAccess())

	if ctx.MethodChain() != nil {
		v.visitMethodChain(ctx.MethodChain())
	}
}

func (v *mongoShellVisitor) VisitGetCollectionNames(_ *mongodb.GetCollectionNamesContext) any {
	v.err = &UnsupportedOperationError{
		Operation: "getCollectionNames",
		Hint:      "not yet supported",
	}
	return nil
}

func (v *mongoShellVisitor) extractCollectionName(ctx mongodb.ICollectionAccessContext) string {
	switch c := ctx.(type) {
	case *mongodb.DotAccessContext:
		return c.Identifier().GetText()
	case *mongodb.BracketAccessContext:
		return unquoteString(c.StringLiteral().GetText())
	case *mongodb.GetCollectionAccessContext:
		return unquoteString(c.StringLiteral().GetText())
	}
	return ""
}

func (v *mongoShellVisitor) visitMethodChain(ctx mongodb.IMethodChainContext) {
	mc, ok := ctx.(*mongodb.MethodChainContext)
	if !ok {
		return
	}
	for _, methodCall := range mc.AllMethodCall() {
		v.visitMethodCall(methodCall)
		if v.err != nil {
			return
		}
	}
}

func (v *mongoShellVisitor) visitMethodCall(ctx mongodb.IMethodCallContext) {
	mc, ok := ctx.(*mongodb.MethodCallContext)
	if !ok {
		return
	}

	if mc.FindMethod() != nil {
		v.operation.opType = opFind
		// MVP: ignore filter argument
	} else if mc.FindOneMethod() != nil {
		v.err = &UnsupportedOperationError{
			Operation: "findOne",
			Hint:      "not yet supported",
		}
	} else if mc.SortMethod() != nil {
		// MVP: ignore sort
	} else if mc.LimitMethod() != nil {
		// MVP: ignore limit
	} else if mc.SkipMethod() != nil {
		// MVP: ignore skip
	} else if mc.ProjectionMethod() != nil {
		// MVP: ignore projection
	} else if gm := mc.GenericMethod(); gm != nil {
		methodName := gm.Identifier().GetText()
		v.err = &UnsupportedOperationError{
			Operation: methodName,
			Hint:      "unknown method",
		}
	}
}

// unquoteString removes quotes from a string literal.
func unquoteString(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '\'' && s[len(s)-1] == '\'') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
