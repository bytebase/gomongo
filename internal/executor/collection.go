package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/bytebase/gomongo/internal/translator"
	"github.com/bytebase/gomongo/types"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// computeEffectiveLimit returns the minimum of opLimit and maxRows.
// Returns nil if both are nil.
func computeEffectiveLimit(opLimit, maxRows *int64) *int64 {
	if opLimit == nil && maxRows == nil {
		return nil
	}
	if opLimit == nil {
		return maxRows
	}
	if maxRows == nil {
		return opLimit
	}
	// Both are non-nil, return the minimum
	if *opLimit < *maxRows {
		return opLimit
	}
	return maxRows
}

// executeFind executes a find operation.
func executeFind(ctx context.Context, client *mongo.Client, database string, op *translator.Operation, maxRows *int64) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.Find()
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}
	// Compute effective limit: min(op.Limit, maxRows)
	effectiveLimit := computeEffectiveLimit(op.Limit, maxRows)
	if effectiveLimit != nil {
		opts.SetLimit(*effectiveLimit)
	}
	if op.Skip != nil {
		opts.SetSkip(*op.Skip)
	}
	if op.Projection != nil {
		opts.SetProjection(op.Projection)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Max != nil {
		opts.SetMax(op.Max)
	}
	if op.Min != nil {
		opts.SetMin(op.Min)
	}

	// Apply maxTimeMS using context timeout.
	// Note: MongoDB Go driver v2 removed SetMaxTime() from options. The recommended
	// replacement is context.WithTimeout(). This is a client-side timeout (includes
	// network latency), unlike mongosh's maxTimeMS which is server-side only.
	if op.MaxTimeMS != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*op.MaxTimeMS)*time.Millisecond)
		defer cancel()
	}

	cursor, err := collection.Find(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("find failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []bson.D
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}
		docs = append(docs, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	// Convert []bson.D to []any
	values := make([]any, len(docs))
	for i, doc := range docs {
		values[i] = doc
	}

	return &Result{
		Operation: types.OpFind,
		Value:     values,
	}, nil
}

// executeFindOne executes a findOne operation.
func executeFindOne(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.FindOne()
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}
	if op.Skip != nil {
		opts.SetSkip(*op.Skip)
	}
	if op.Projection != nil {
		opts.SetProjection(op.Projection)
	}
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Max != nil {
		opts.SetMax(op.Max)
	}
	if op.Min != nil {
		opts.SetMin(op.Min)
	}

	// Apply maxTimeMS using context timeout (see comment in executeFind for details).
	if op.MaxTimeMS != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*op.MaxTimeMS)*time.Millisecond)
		defer cancel()
	}

	var doc bson.D
	err := collection.FindOne(ctx, filter, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &Result{
				Operation: types.OpFindOne,
				Value:     []any{},
			}, nil
		}
		return nil, fmt.Errorf("findOne failed: %w", err)
	}

	return &Result{
		Operation: types.OpFindOne,
		Value:     []any{doc},
	}, nil
}

// executeAggregate executes an aggregation pipeline.
func executeAggregate(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	pipeline := op.Pipeline
	if pipeline == nil {
		pipeline = bson.A{}
	}

	opts := options.Aggregate()
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}

	// Apply maxTimeMS using context timeout (see comment in executeFind for details).
	if op.MaxTimeMS != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*op.MaxTimeMS)*time.Millisecond)
		defer cancel()
	}

	cursor, err := collection.Aggregate(ctx, pipeline, opts)
	if err != nil {
		return nil, fmt.Errorf("aggregate failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var values []any
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}
		values = append(values, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Operation: types.OpAggregate,
		Value:     values,
	}, nil
}

// executeGetIndexes executes a db.collection.getIndexes() command.
func executeGetIndexes(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	cursor, err := collection.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list indexes failed: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var values []any
	for cursor.Next(ctx) {
		var doc bson.D
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}
		values = append(values, doc)
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Operation: types.OpGetIndexes,
		Value:     values,
	}, nil
}

// executeCountDocuments executes a db.collection.countDocuments() command.
func executeCountDocuments(ctx context.Context, client *mongo.Client, database string, op *translator.Operation, maxRows *int64) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.Count()
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	// Compute effective limit: min(op.Limit, maxRows)
	effectiveLimit := computeEffectiveLimit(op.Limit, maxRows)
	if effectiveLimit != nil {
		opts.SetLimit(*effectiveLimit)
	}
	if op.Skip != nil {
		opts.SetSkip(*op.Skip)
	}

	// Apply maxTimeMS using context timeout (see comment in executeFind for details).
	if op.MaxTimeMS != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*op.MaxTimeMS)*time.Millisecond)
		defer cancel()
	}

	count, err := collection.CountDocuments(ctx, filter, opts)
	if err != nil {
		return nil, fmt.Errorf("count documents failed: %w", err)
	}

	return &Result{
		Operation: types.OpCountDocuments,
		Value:     []any{count},
	}, nil
}

// executeEstimatedDocumentCount executes a db.collection.estimatedDocumentCount() command.
func executeEstimatedDocumentCount(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	// Apply maxTimeMS using context timeout (see comment in executeFind for details).
	if op.MaxTimeMS != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*op.MaxTimeMS)*time.Millisecond)
		defer cancel()
	}

	count, err := collection.EstimatedDocumentCount(ctx)
	if err != nil {
		return nil, fmt.Errorf("estimated document count failed: %w", err)
	}

	return &Result{
		Operation: types.OpEstimatedDocumentCount,
		Value:     []any{count},
	}, nil
}

// executeDistinct executes a db.collection.distinct() command.
func executeDistinct(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	// Apply maxTimeMS using context timeout (see comment in executeFind for details).
	if op.MaxTimeMS != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(*op.MaxTimeMS)*time.Millisecond)
		defer cancel()
	}

	result := collection.Distinct(ctx, op.DistinctField, filter)
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("distinct failed: %w", err)
	}

	var values []any
	if err := result.Decode(&values); err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}

	return &Result{
		Operation: types.OpDistinct,
		Value:     values,
	}, nil
}
