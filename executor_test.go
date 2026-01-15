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

func TestFindWithFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30, "active": true},
		bson.M{"name": "bob", "age": 25, "active": false},
		bson.M{"name": "carol", "age": 35, "active": true},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name          string
		statement     string
		expectedCount int
		checkResult   func(t *testing.T, rows []string)
	}{
		{
			name:          "filter by string",
			statement:     `db.users.find({ name: "alice" })`,
			expectedCount: 1,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"alice"`)
			},
		},
		{
			name:          "filter by number",
			statement:     `db.users.find({ age: 25 })`,
			expectedCount: 1,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"bob"`)
			},
		},
		{
			name:          "filter by boolean",
			statement:     `db.users.find({ active: true })`,
			expectedCount: 2,
		},
		{
			name:          "filter with $gt operator",
			statement:     `db.users.find({ age: { $gt: 28 } })`,
			expectedCount: 2,
		},
		{
			name:          "filter with $lte operator",
			statement:     `db.users.find({ age: { $lte: 25 } })`,
			expectedCount: 1,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"bob"`)
			},
		},
		{
			name:          "filter with multiple conditions",
			statement:     `db.users.find({ active: true, age: { $gte: 30 } })`,
			expectedCount: 2,
		},
		{
			name:          "filter with $in operator",
			statement:     `db.users.find({ name: { $in: ["alice", "bob"] } })`,
			expectedCount: 2,
		},
		{
			name:          "filter with no matches",
			statement:     `db.users.find({ name: "nobody" })`,
			expectedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "testdb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.expectedCount, result.RowCount)
			if tc.checkResult != nil && result.RowCount > 0 {
				tc.checkResult(t, result.Rows)
			}
		})
	}
}

func TestFindWithCursorModifications(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("items")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "apple", "price": 1, "category": "fruit"},
		bson.M{"name": "banana", "price": 2, "category": "fruit"},
		bson.M{"name": "carrot", "price": 3, "category": "vegetable"},
		bson.M{"name": "date", "price": 4, "category": "fruit"},
		bson.M{"name": "eggplant", "price": 5, "category": "vegetable"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name          string
		statement     string
		expectedCount int
		checkResult   func(t *testing.T, rows []string)
	}{
		{
			name:          "sort ascending",
			statement:     `db.items.find().sort({ price: 1 })`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"apple"`)
				require.Contains(t, rows[4], `"eggplant"`)
			},
		},
		{
			name:          "sort descending",
			statement:     `db.items.find().sort({ price: -1 })`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"eggplant"`)
				require.Contains(t, rows[4], `"apple"`)
			},
		},
		{
			name:          "limit",
			statement:     `db.items.find().limit(2)`,
			expectedCount: 2,
		},
		{
			name:          "skip",
			statement:     `db.items.find().sort({ price: 1 }).skip(2)`,
			expectedCount: 3,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"carrot"`)
			},
		},
		{
			name:          "sort and limit",
			statement:     `db.items.find().sort({ price: -1 }).limit(3)`,
			expectedCount: 3,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"eggplant"`)
				require.Contains(t, rows[2], `"carrot"`)
			},
		},
		{
			name:          "skip and limit",
			statement:     `db.items.find().sort({ price: 1 }).skip(1).limit(2)`,
			expectedCount: 2,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"banana"`)
				require.Contains(t, rows[1], `"carrot"`)
			},
		},
		{
			name:          "projection include",
			statement:     `db.items.find().projection({ name: 1 })`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"name"`)
				require.Contains(t, rows[0], `"_id"`)
				require.NotContains(t, rows[0], `"price"`)
				require.NotContains(t, rows[0], `"category"`)
			},
		},
		{
			name:          "projection exclude",
			statement:     `db.items.find().projection({ _id: 0, category: 0 })`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"name"`)
				require.Contains(t, rows[0], `"price"`)
				require.NotContains(t, rows[0], `"_id"`)
				require.NotContains(t, rows[0], `"category"`)
			},
		},
		{
			name:          "filter with sort and limit",
			statement:     `db.items.find({ category: "fruit" }).sort({ price: -1 }).limit(2)`,
			expectedCount: 2,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"date"`)
				require.Contains(t, rows[1], `"banana"`)
			},
		},
		{
			name:          "all modifiers combined",
			statement:     `db.items.find({ category: "fruit" }).sort({ price: 1 }).skip(1).limit(2).projection({ name: 1, price: 1, _id: 0 })`,
			expectedCount: 2,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"banana"`)
				require.Contains(t, rows[1], `"date"`)
				require.NotContains(t, rows[0], `"_id"`)
				require.NotContains(t, rows[0], `"category"`)
			},
		},
		{
			name:          "method chain order: limit before sort",
			statement:     `db.items.find().limit(3).sort({ price: -1 })`,
			expectedCount: 3,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"eggplant"`)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "testdb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, tc.expectedCount, result.RowCount)
			if tc.checkResult != nil && result.RowCount > 0 {
				tc.checkResult(t, result.Rows)
			}
		})
	}
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

func TestShowDatabases(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a database by inserting a document
	_, err := client.Database("mydb").Collection("test").InsertOne(ctx, bson.M{"x": 1})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name      string
		statement string
	}{
		{"show dbs", "show dbs"},
		{"show databases", "show databases"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "mydb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.GreaterOrEqual(t, result.RowCount, 1)

			// Check that mydb is in the result
			found := false
			for _, row := range result.Rows {
				if row == "mydb" {
					found = true
					break
				}
			}
			require.True(t, found, "expected 'mydb' in database list, got: %v", result.Rows)
		})
	}
}

func TestShowCollections(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database("testdb").Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)
	_, err = client.Database("testdb").Collection("orders").InsertOne(ctx, bson.M{"item": "book"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, "testdb", "show collections")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount)

	// Check that both collections are in the result
	collectionSet := make(map[string]bool)
	for _, row := range result.Rows {
		collectionSet[row] = true
	}
	require.True(t, collectionSet["users"], "expected 'users' collection")
	require.True(t, collectionSet["orders"], "expected 'orders' collection")
}

func TestGetCollectionNames(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database("testdb").Collection("products").InsertOne(ctx, bson.M{"name": "widget"})
	require.NoError(t, err)
	_, err = client.Database("testdb").Collection("categories").InsertOne(ctx, bson.M{"name": "electronics"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, "testdb", "db.getCollectionNames()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount)

	// Check that both collections are in the result
	collectionSet := make(map[string]bool)
	for _, row := range result.Rows {
		collectionSet[row] = true
	}
	require.True(t, collectionSet["products"], "expected 'products' collection")
	require.True(t, collectionSet["categories"], "expected 'categories' collection")
}
