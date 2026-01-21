package executor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/bytebase/gomongo/internal/translator"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// executeFind executes a find operation.
func executeFind(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.Find()
	if op.Sort != nil {
		opts.SetSort(op.Sort)
	}
	if op.Limit != nil {
		opts.SetLimit(*op.Limit)
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

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		// Marshal to Extended JSON (Relaxed)
		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
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

	var doc bson.M
	err := collection.FindOne(ctx, filter, opts).Decode(&doc)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return &Result{
				Rows:     nil,
				RowCount: 0,
			}, nil
		}
		return nil, fmt.Errorf("findOne failed: %w", err)
	}

	jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal failed: %w", err)
	}

	return &Result{
		Rows:     []string{string(jsonBytes)},
		RowCount: 1,
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

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
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

	var rows []string
	for cursor.Next(ctx) {
		var doc bson.M
		if err := cursor.Decode(&doc); err != nil {
			return nil, fmt.Errorf("decode failed: %w", err)
		}

		jsonBytes, err := bson.MarshalExtJSONIndent(doc, false, false, "", "  ")
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	if err := cursor.Err(); err != nil {
		return nil, fmt.Errorf("cursor error: %w", err)
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// executeCountDocuments executes a db.collection.countDocuments() command.
func executeCountDocuments(ctx context.Context, client *mongo.Client, database string, op *translator.Operation) (*Result, error) {
	collection := client.Database(database).Collection(op.Collection)

	filter := op.Filter
	if filter == nil {
		filter = bson.D{}
	}

	opts := options.Count()
	if op.Hint != nil {
		opts.SetHint(op.Hint)
	}
	if op.Limit != nil {
		opts.SetLimit(*op.Limit)
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
		Rows:     []string{fmt.Sprintf("%d", count)},
		RowCount: 1,
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
		Rows:     []string{fmt.Sprintf("%d", count)},
		RowCount: 1,
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

	var rows []string
	for _, val := range values {
		jsonBytes, err := marshalValue(val)
		if err != nil {
			return nil, fmt.Errorf("marshal failed: %w", err)
		}
		rows = append(rows, string(jsonBytes))
	}

	return &Result{
		Rows:     rows,
		RowCount: len(rows),
	}, nil
}

// marshalValue marshals a value to JSON.
// bson.MarshalExtJSONIndent only works for documents/arrays at top level,
// so we use encoding/json for primitive values (strings, numbers, booleans).
func marshalValue(val any) ([]byte, error) {
	switch v := val.(type) {
	case bson.M, bson.D, map[string]any:
		return bson.MarshalExtJSONIndent(v, false, false, "", "  ")
	case bson.A, []any:
		return bson.MarshalExtJSONIndent(v, false, false, "", "  ")
	default:
		return json.Marshal(v)
	}
}
