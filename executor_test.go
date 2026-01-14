package gomongo_test

import (
	"context"
	"testing"
	"time"

	"github.com/bytebase/gomongo"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func setupTestContainer(t *testing.T) (*mongo.Client, func()) {
	ctx := context.Background()

	mongodbContainer, err := mongodb.Run(ctx, "mongo:7")
	require.NoError(t, err)

	connectionString, err := mongodbContainer.ConnectionString(ctx)
	require.NoError(t, err)

	client, err := mongo.Connect(options.Client().ApplyURI(connectionString))
	require.NoError(t, err)

	cleanup := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = client.Disconnect(ctx)
		_ = mongodbContainer.Terminate(ctx)
	}

	return client, cleanup
}

func TestFindEmptyCollection(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	result, err := gc.Execute(ctx, "testdb", "db.users.find()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 0, result.RowCount)
	require.Empty(t, result.Rows)
}

func TestFindWithDocuments(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Insert test documents
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30},
		bson.M{"name": "bob", "age": 25},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)
	result, err := gc.Execute(ctx, "testdb", "db.users.find()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount)
	require.Len(t, result.Rows, 2)

	// Verify JSON format
	for _, row := range result.Rows {
		require.Contains(t, row, "name")
		require.Contains(t, row, "age")
		require.Contains(t, row, "_id")
	}
}

func TestFindWithEmptyFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("items")
	_, err := collection.InsertOne(ctx, bson.M{"item": "test"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)
	result, err := gc.Execute(ctx, "testdb", "db.items.find({})")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
}

func TestParseError(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	_, err := gc.Execute(ctx, "testdb", "db.users.find({ name: })")
	require.Error(t, err)

	var parseErr *gomongo.ParseError
	require.ErrorAs(t, err, &parseErr)
}

func TestUnsupportedOperation(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	_, err := gc.Execute(ctx, "testdb", "db.users.findOne()")
	require.Error(t, err)

	var unsupportedErr *gomongo.UnsupportedOperationError
	require.ErrorAs(t, err, &unsupportedErr)
	require.Equal(t, "findOne", unsupportedErr.Operation)
}

func TestCollectionAccessPatterns(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Insert a document
	collection := client.Database("testdb").Collection("my-collection")
	_, err := collection.InsertOne(ctx, bson.M{"data": "test"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name      string
		statement string
	}{
		{"dot access", "db.users.find()"},
		{"bracket double quote", `db["my-collection"].find()`},
		{"bracket single quote", `db['my-collection'].find()`},
		{"getCollection", `db.getCollection("my-collection").find()`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "testdb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}
