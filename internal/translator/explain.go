package translator

import (
	"fmt"

	"github.com/bytebase/gomongo/types"
	"github.com/bytebase/omni/mongo/ast"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const defaultExplainVerbosity = "queryPlanner"

func isValidExplainVerbosity(v string) bool {
	switch v {
	case "queryPlanner", "executionStats", "allPlansExecution":
		return true
	}
	return false
}

// applyExplainCursor handles a trailing .explain() / .explain("verbosity")
// cursor terminator by recording the requested verbosity on the operation.
// The actual rewrite into a runCommand happens in applyExplainIfRequested
// once cursor-method translation is complete.
func applyExplainCursor(op *Operation, args []ast.Node) error {
	verbosity := defaultExplainVerbosity
	switch len(args) {
	case 0:
	case 1:
		v, err := requireString(args, 0, "explain() verbosity")
		if err != nil {
			return err
		}
		verbosity = v
	default:
		return fmt.Errorf("explain() takes at most 1 argument")
	}
	if !isValidExplainVerbosity(verbosity) {
		return fmt.Errorf("explain() verbosity must be queryPlanner, executionStats, or allPlansExecution; got %q", verbosity)
	}
	op.Explain = &verbosity
	return nil
}

// applyExplainIfRequested rewrites op into an OpRunCommand carrying
// {explain: <innerCommand>, verbosity: <v>} when op.Explain is set.
// Supported inner operations: find, aggregate, count (zero-arg + filtered),
// countDocuments, distinct.
func applyExplainIfRequested(op *Operation) error {
	if op.Explain == nil {
		return nil
	}
	inner, err := buildExplainInner(op)
	if err != nil {
		return err
	}
	op.Command = bson.D{
		{Key: "explain", Value: inner},
		{Key: "verbosity", Value: *op.Explain},
	}
	op.OpType = types.OpRunCommand
	return nil
}

func buildExplainInner(op *Operation) (bson.D, error) {
	switch op.OpType {
	case types.OpFind:
		return buildFindCommand(op), nil
	case types.OpAggregate:
		return buildAggregateCommand(op), nil
	case types.OpEstimatedDocumentCount, types.OpCountDocuments:
		return buildCountCommand(op), nil
	case types.OpDistinct:
		return buildDistinctCommand(op), nil
	default:
		return nil, fmt.Errorf("explain() is not supported for this operation; supported: find, aggregate, count, countDocuments, distinct")
	}
}

func buildFindCommand(op *Operation) bson.D {
	cmd := bson.D{{Key: "find", Value: op.Collection}}
	if len(op.Filter) > 0 {
		cmd = append(cmd, bson.E{Key: "filter", Value: op.Filter})
	}
	if len(op.Projection) > 0 {
		cmd = append(cmd, bson.E{Key: "projection", Value: op.Projection})
	}
	if len(op.Sort) > 0 {
		cmd = append(cmd, bson.E{Key: "sort", Value: op.Sort})
	}
	if op.Limit != nil {
		cmd = append(cmd, bson.E{Key: "limit", Value: *op.Limit})
	}
	if op.Skip != nil {
		cmd = append(cmd, bson.E{Key: "skip", Value: *op.Skip})
	}
	if op.Hint != nil {
		cmd = append(cmd, bson.E{Key: "hint", Value: op.Hint})
	}
	if len(op.Max) > 0 {
		cmd = append(cmd, bson.E{Key: "max", Value: op.Max})
	}
	if len(op.Min) > 0 {
		cmd = append(cmd, bson.E{Key: "min", Value: op.Min})
	}
	return cmd
}

func buildAggregateCommand(op *Operation) bson.D {
	pipeline := op.Pipeline
	if pipeline == nil {
		pipeline = bson.A{}
	}
	cmd := bson.D{
		{Key: "aggregate", Value: op.Collection},
		{Key: "pipeline", Value: pipeline},
		// The aggregate command requires either a "cursor" field or
		// "explain: true" at the inner level. We're at the explain wrapper
		// layer, so the inner command still needs a cursor field; an empty
		// document is the conventional "default cursor settings" form.
		{Key: "cursor", Value: bson.D{}},
	}
	if op.Hint != nil {
		cmd = append(cmd, bson.E{Key: "hint", Value: op.Hint})
	}
	if op.MaxTimeMS != nil {
		cmd = append(cmd, bson.E{Key: "maxTimeMS", Value: *op.MaxTimeMS})
	}
	return cmd
}

func buildCountCommand(op *Operation) bson.D {
	cmd := bson.D{{Key: "count", Value: op.Collection}}
	if len(op.Filter) > 0 {
		cmd = append(cmd, bson.E{Key: "query", Value: op.Filter})
	}
	if op.Limit != nil {
		cmd = append(cmd, bson.E{Key: "limit", Value: *op.Limit})
	}
	if op.Skip != nil {
		cmd = append(cmd, bson.E{Key: "skip", Value: *op.Skip})
	}
	if op.Hint != nil {
		cmd = append(cmd, bson.E{Key: "hint", Value: op.Hint})
	}
	return cmd
}

func buildDistinctCommand(op *Operation) bson.D {
	cmd := bson.D{
		{Key: "distinct", Value: op.Collection},
		{Key: "key", Value: op.DistinctField},
	}
	if len(op.Filter) > 0 {
		cmd = append(cmd, bson.E{Key: "query", Value: op.Filter})
	}
	return cmd
}
