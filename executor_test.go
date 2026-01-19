package gomongo_test

import (
	"context"
	"strings"
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

func TestFindOneEmptyCollection(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	result, err := gc.Execute(ctx, "testdb", "db.users.findOne()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 0, result.RowCount)
	require.Empty(t, result.Rows)
}

func TestFindOneWithDocuments(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30},
		bson.M{"name": "bob", "age": 25},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)
	result, err := gc.Execute(ctx, "testdb", "db.users.findOne()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Len(t, result.Rows, 1)
	require.Contains(t, result.Rows[0], "name")
	require.Contains(t, result.Rows[0], "age")
	require.Contains(t, result.Rows[0], "_id")
}

func TestFindOneWithFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30},
		bson.M{"name": "bob", "age": 25},
		bson.M{"name": "carol", "age": 35},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name        string
		statement   string
		expectMatch bool
		checkResult func(t *testing.T, row string)
	}{
		{
			name:        "filter by string",
			statement:   `db.users.findOne({ name: "bob" })`,
			expectMatch: true,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"bob"`)
				require.Contains(t, row, `"age": 25`)
			},
		},
		{
			name:        "filter by number",
			statement:   `db.users.findOne({ age: 35 })`,
			expectMatch: true,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"carol"`)
			},
		},
		{
			name:        "filter with no match",
			statement:   `db.users.findOne({ name: "nobody" })`,
			expectMatch: false,
		},
		{
			name:        "filter with $gt operator",
			statement:   `db.users.findOne({ age: { $gt: 30 } })`,
			expectMatch: true,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"carol"`)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "testdb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
			if tc.expectMatch {
				require.Equal(t, 1, result.RowCount)
				if tc.checkResult != nil {
					tc.checkResult(t, result.Rows[0])
				}
			} else {
				require.Equal(t, 0, result.RowCount)
			}
		})
	}
}

func TestFindOneWithOptions(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("items")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "apple", "price": 1},
		bson.M{"name": "banana", "price": 2},
		bson.M{"name": "carrot", "price": 3},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name        string
		statement   string
		checkResult func(t *testing.T, row string)
	}{
		{
			name:      "sort ascending - returns first",
			statement: `db.items.findOne().sort({ price: 1 })`,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"apple"`)
			},
		},
		{
			name:      "sort descending - returns first",
			statement: `db.items.findOne().sort({ price: -1 })`,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"carrot"`)
			},
		},
		{
			name:      "skip",
			statement: `db.items.findOne().sort({ price: 1 }).skip(1)`,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"banana"`)
			},
		},
		{
			name:      "projection include",
			statement: `db.items.findOne().projection({ name: 1, _id: 0 })`,
			checkResult: func(t *testing.T, row string) {
				require.Contains(t, row, `"name"`)
				require.NotContains(t, row, `"_id"`)
				require.NotContains(t, row, `"price"`)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "testdb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, 1, result.RowCount)
			tc.checkResult(t, result.Rows[0])
		})
	}
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

	_, err := gc.Execute(ctx, "testdb", "db.users.insertOne({ name: 'test' })")
	require.Error(t, err)

	var unsupportedErr *gomongo.UnsupportedOperationError
	require.ErrorAs(t, err, &unsupportedErr)
	require.Equal(t, "insertOne", unsupportedErr.Operation)
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

func TestAggregateBasic(t *testing.T) {
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
			name:          "empty pipeline",
			statement:     `db.items.aggregate([])`,
			expectedCount: 5,
		},
		{
			name:          "empty pipeline no args",
			statement:     `db.items.aggregate()`,
			expectedCount: 5,
		},
		{
			name:          "$match stage",
			statement:     `db.items.aggregate([{ $match: { category: "fruit" } }])`,
			expectedCount: 3,
		},
		{
			name:          "$sort ascending",
			statement:     `db.items.aggregate([{ $sort: { price: 1 } }])`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"apple"`)
				require.Contains(t, rows[4], `"eggplant"`)
			},
		},
		{
			name:          "$sort descending",
			statement:     `db.items.aggregate([{ $sort: { price: -1 } }])`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"eggplant"`)
				require.Contains(t, rows[4], `"apple"`)
			},
		},
		{
			name:          "$limit stage",
			statement:     `db.items.aggregate([{ $limit: 2 }])`,
			expectedCount: 2,
		},
		{
			name:          "$skip stage",
			statement:     `db.items.aggregate([{ $sort: { price: 1 } }, { $skip: 3 }])`,
			expectedCount: 2,
		},
		{
			name:          "$project include",
			statement:     `db.items.aggregate([{ $project: { name: 1, _id: 0 } }])`,
			expectedCount: 5,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"name"`)
				require.NotContains(t, rows[0], `"_id"`)
				require.NotContains(t, rows[0], `"price"`)
			},
		},
		{
			name:          "$count stage",
			statement:     `db.items.aggregate([{ $count: "total" }])`,
			expectedCount: 1,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"total": 5`)
			},
		},
		{
			name:          "multi-stage: match and sort",
			statement:     `db.items.aggregate([{ $match: { category: "fruit" } }, { $sort: { price: -1 } }])`,
			expectedCount: 3,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"date"`)
			},
		},
		{
			name:          "multi-stage: match, sort, limit",
			statement:     `db.items.aggregate([{ $match: { category: "fruit" } }, { $sort: { price: 1 } }, { $limit: 2 }])`,
			expectedCount: 2,
			checkResult: func(t *testing.T, rows []string) {
				require.Contains(t, rows[0], `"apple"`)
				require.Contains(t, rows[1], `"banana"`)
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

func TestAggregateGroup(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("sales")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"item": "apple", "quantity": 10, "price": 1.5},
		bson.M{"item": "banana", "quantity": 5, "price": 2.0},
		bson.M{"item": "apple", "quantity": 8, "price": 1.5},
		bson.M{"item": "banana", "quantity": 3, "price": 2.0},
		bson.M{"item": "carrot", "quantity": 15, "price": 0.5},
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
			name:          "$group by field",
			statement:     `db.sales.aggregate([{ $group: { _id: "$item" } }])`,
			expectedCount: 3,
		},
		{
			name:          "$group with $sum",
			statement:     `db.sales.aggregate([{ $group: { _id: "$item", totalQuantity: { $sum: "$quantity" } } }])`,
			expectedCount: 3,
		},
		{
			name:          "$group with $avg",
			statement:     `db.sales.aggregate([{ $group: { _id: "$item", avgQuantity: { $avg: "$quantity" } } }])`,
			expectedCount: 3,
		},
		{
			name: "$group with multiple accumulators",
			statement: `db.sales.aggregate([
				{ $group: {
					_id: "$item",
					totalQuantity: { $sum: "$quantity" },
					avgQuantity: { $avg: "$quantity" },
					count: { $sum: 1 }
				}}
			])`,
			expectedCount: 3,
		},
		{
			name: "$group then $sort",
			statement: `db.sales.aggregate([
				{ $group: { _id: "$item", total: { $sum: "$quantity" } } },
				{ $sort: { total: -1 } }
			])`,
			expectedCount: 3,
			checkResult: func(t *testing.T, rows []string) {
				// apple has 18 total, should be first
				require.Contains(t, rows[0], `"apple"`)
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

func TestAggregateCollectionAccess(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("my-items")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "test1"},
		bson.M{"name": "test2"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	tests := []struct {
		name      string
		statement string
	}{
		{"dot notation", `db.users.aggregate([])`},
		{"bracket notation", `db["my-items"].aggregate([{ $limit: 1 }])`},
		{"getCollection", `db.getCollection("my-items").aggregate([{ $limit: 1 }])`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := gc.Execute(ctx, "testdb", tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}

// TestAggregateFilteredSubset tests the "Filtered Subset" example from MongoDB docs
// https://www.mongodb.com/docs/manual/tutorial/aggregation-examples/filtered-subset/
func TestAggregateFilteredSubset(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("persons")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{
			"person_id":   "6392529400",
			"firstname":   "Elise",
			"lastname":    "Smith",
			"dateofbirth": time.Date(1972, 1, 13, 9, 32, 7, 0, time.UTC),
			"vocation":    "ENGINEER",
			"address":     bson.M{"number": 5625, "street": "Tipa Circle", "city": "Wojzinmoj"},
		},
		bson.M{
			"person_id":   "1723338115",
			"firstname":   "Olive",
			"lastname":    "Ranieri",
			"dateofbirth": time.Date(1985, 5, 12, 23, 14, 30, 0, time.UTC),
			"gender":      "FEMALE",
			"vocation":    "ENGINEER",
			"address":     bson.M{"number": 9303, "street": "Mele Circle", "city": "Tobihbo"},
		},
		bson.M{
			"person_id":   "8732762874",
			"firstname":   "Toni",
			"lastname":    "Jones",
			"dateofbirth": time.Date(1991, 11, 23, 16, 53, 56, 0, time.UTC),
			"vocation":    "POLITICIAN",
			"address":     bson.M{"number": 1, "street": "High Street", "city": "Upper Abbeywoodington"},
		},
		bson.M{
			"person_id":   "7363629563",
			"firstname":   "Bert",
			"lastname":    "Gooding",
			"dateofbirth": time.Date(1941, 4, 7, 22, 11, 52, 0, time.UTC),
			"vocation":    "FLORIST",
			"address":     bson.M{"number": 13, "street": "Upper Bold Road", "city": "Redringtonville"},
		},
		bson.M{
			"person_id":   "1029648329",
			"firstname":   "Sophie",
			"lastname":    "Celements",
			"dateofbirth": time.Date(1959, 7, 6, 17, 35, 45, 0, time.UTC),
			"vocation":    "ENGINEER",
			"address":     bson.M{"number": 5, "street": "Innings Close", "city": "Basilbridge"},
		},
		bson.M{
			"person_id":   "7363626383",
			"firstname":   "Carl",
			"lastname":    "Simmons",
			"dateofbirth": time.Date(1998, 12, 26, 13, 13, 55, 0, time.UTC),
			"vocation":    "ENGINEER",
			"address":     bson.M{"number": 187, "street": "Hillside Road", "city": "Kenningford"},
		},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Find 3 youngest engineers
	statement := `db.persons.aggregate([
		{ $match: { vocation: "ENGINEER" } },
		{ $sort: { dateofbirth: -1 } },
		{ $limit: 3 },
		{ $unset: ["_id", "vocation", "address"] }
	])`

	result, err := gc.Execute(ctx, "testdb", statement)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.RowCount)

	// Carl (1998) should be first (youngest)
	require.Contains(t, result.Rows[0], `"Carl"`)
	// Olive (1985) should be second
	require.Contains(t, result.Rows[1], `"Olive"`)
	// Elise (1972) should be third
	require.Contains(t, result.Rows[2], `"Elise"`)

	// Verify _id, vocation, and address are excluded
	require.NotContains(t, result.Rows[0], `"_id"`)
	require.NotContains(t, result.Rows[0], `"vocation"`)
	require.NotContains(t, result.Rows[0], `"address"`)
}

// TestAggregateGroupAndTotal tests the "Group and Total" example from MongoDB docs
// https://www.mongodb.com/docs/manual/tutorial/aggregation-examples/group-and-total/
func TestAggregateGroupAndTotal(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("orders")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{
			"customer_id": "elise_smith@myemail.com",
			"orderdate":   time.Date(2020, 5, 30, 8, 35, 52, 0, time.UTC),
			"value":       231.43,
		},
		bson.M{
			"customer_id": "elise_smith@myemail.com",
			"orderdate":   time.Date(2020, 1, 13, 9, 32, 7, 0, time.UTC),
			"value":       99.99,
		},
		bson.M{
			"customer_id": "oranieri@warmmail.com",
			"orderdate":   time.Date(2020, 1, 1, 8, 25, 37, 0, time.UTC),
			"value":       63.13,
		},
		bson.M{
			"customer_id": "tj@wheresmyemail.com",
			"orderdate":   time.Date(2019, 5, 28, 19, 13, 32, 0, time.UTC),
			"value":       2.01,
		},
		bson.M{
			"customer_id": "tj@wheresmyemail.com",
			"orderdate":   time.Date(2020, 11, 23, 22, 56, 53, 0, time.UTC),
			"value":       187.99,
		},
		bson.M{
			"customer_id": "tj@wheresmyemail.com",
			"orderdate":   time.Date(2020, 8, 18, 23, 4, 48, 0, time.UTC),
			"value":       4.59,
		},
		bson.M{
			"customer_id": "elise_smith@myemail.com",
			"orderdate":   time.Date(2020, 12, 26, 8, 55, 46, 0, time.UTC),
			"value":       48.50,
		},
		bson.M{
			"customer_id": "tj@wheresmyemail.com",
			"orderdate":   time.Date(2021, 2, 28, 7, 49, 32, 0, time.UTC),
			"value":       1024.89,
		},
		bson.M{
			"customer_id": "elise_smith@myemail.com",
			"orderdate":   time.Date(2020, 10, 3, 13, 49, 44, 0, time.UTC),
			"value":       102.24,
		},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Group orders by customer for year 2020
	statement := `db.orders.aggregate([
		{ $match: {
			orderdate: {
				$gte: ISODate("2020-01-01T00:00:00Z"),
				$lt: ISODate("2021-01-01T00:00:00Z")
			}
		}},
		{ $sort: { orderdate: 1 } },
		{ $group: {
			_id: "$customer_id",
			first_purchase_date: { $first: "$orderdate" },
			total_value: { $sum: "$value" },
			total_orders: { $sum: 1 }
		}},
		{ $sort: { first_purchase_date: 1 } },
		{ $set: { customer_id: "$_id" } },
		{ $unset: ["_id"] }
	])`

	result, err := gc.Execute(ctx, "testdb", statement)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.RowCount)

	// oranieri should be first (earliest order in 2020: Jan 1)
	require.Contains(t, result.Rows[0], `"oranieri@warmmail.com"`)

	// Verify structure
	require.Contains(t, result.Rows[0], `"customer_id"`)
	require.Contains(t, result.Rows[0], `"total_value"`)
	require.Contains(t, result.Rows[0], `"total_orders"`)
	require.NotContains(t, result.Rows[0], `"_id"`)
}

// TestAggregateUnwindArrays tests the "Unpack Arrays" example from MongoDB docs
// https://www.mongodb.com/docs/manual/tutorial/aggregation-examples/unpack-arrays/
func TestAggregateUnwindArrays(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	collection := client.Database("testdb").Collection("orders")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{
			"order_id": 6363763262239,
			"products": []bson.M{
				{"prod_id": "abc12345", "name": "Asus Laptop", "price": 431.43},
				{"prod_id": "def45678", "name": "Karcher Hose Set", "price": 22.13},
			},
		},
		bson.M{
			"order_id": 1197372932325,
			"products": []bson.M{
				{"prod_id": "abc12345", "name": "Asus Laptop", "price": 429.99},
			},
		},
		bson.M{
			"order_id": 9812343774839,
			"products": []bson.M{
				{"prod_id": "pqr88223", "name": "Morphy Richards Food Mixer", "price": 431.43},
				{"prod_id": "def45678", "name": "Karcher Hose Set", "price": 21.78},
			},
		},
		bson.M{
			"order_id": 4433997244387,
			"products": []bson.M{
				{"prod_id": "def45678", "name": "Karcher Hose Set", "price": 23.43},
				{"prod_id": "jkl77336", "name": "Picky Pencil Sharpener", "price": 0.67},
				{"prod_id": "xyz11228", "name": "Russell Hobbs Chrome Kettle", "price": 15.76},
			},
		},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Unpack products, filter by price > 15, group by product
	statement := `db.orders.aggregate([
		{ $unwind: { path: "$products" } },
		{ $match: { "products.price": { $gt: 15 } } },
		{ $group: {
			_id: "$products.prod_id",
			product: { $first: "$products.name" },
			total_value: { $sum: "$products.price" },
			quantity: { $sum: 1 }
		}},
		{ $set: { product_id: "$_id" } },
		{ $unset: ["_id"] }
	])`

	result, err := gc.Execute(ctx, "testdb", statement)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Should have: abc12345 (2x), def45678 (3x but all > 15), pqr88223 (1x), xyz11228 (1x)
	require.Equal(t, 4, result.RowCount)

	// Verify structure
	require.Contains(t, result.Rows[0], `"product_id"`)
	require.Contains(t, result.Rows[0], `"product"`)
	require.Contains(t, result.Rows[0], `"total_value"`)
	require.Contains(t, result.Rows[0], `"quantity"`)
}

// TestAggregateOneToOneJoin tests the "One-to-One Join" example from MongoDB docs
// https://www.mongodb.com/docs/manual/tutorial/aggregation-examples/one-to-one-join/
func TestAggregateOneToOneJoin(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create products collection
	productsCollection := client.Database("testdb").Collection("products")
	_, err := productsCollection.InsertMany(ctx, []any{
		bson.M{
			"id":          "a1b2c3d4",
			"name":        "Asus Laptop",
			"category":    "ELECTRONICS",
			"description": "Good value laptop for students",
		},
		bson.M{
			"id":          "z9y8x7w6",
			"name":        "The Day Of The Triffids",
			"category":    "BOOKS",
			"description": "Classic post-apocalyptic novel",
		},
		bson.M{
			"id":          "ff11gg22hh33",
			"name":        "Morphy Richards Food Mixer",
			"category":    "KITCHENWARE",
			"description": "Luxury mixer turning good cakes into great",
		},
		bson.M{
			"id":          "pqr678st",
			"name":        "Karcher Hose Set",
			"category":    "GARDEN",
			"description": "Hose + nozzles + winder for tidy storage",
		},
	})
	require.NoError(t, err)

	// Create orders collection
	ordersCollection := client.Database("testdb").Collection("orders")
	_, err = ordersCollection.InsertMany(ctx, []any{
		bson.M{
			"customer_id": "elise_smith@myemail.com",
			"orderdate":   time.Date(2020, 5, 30, 8, 35, 52, 0, time.UTC),
			"product_id":  "a1b2c3d4",
			"value":       431.43,
		},
		bson.M{
			"customer_id": "tj@wheresmyemail.com",
			"orderdate":   time.Date(2019, 5, 28, 19, 13, 32, 0, time.UTC),
			"product_id":  "z9y8x7w6",
			"value":       5.01,
		},
		bson.M{
			"customer_id": "oranieri@warmmail.com",
			"orderdate":   time.Date(2020, 1, 1, 8, 25, 37, 0, time.UTC),
			"product_id":  "ff11gg22hh33",
			"value":       63.13,
		},
		bson.M{
			"customer_id": "jjones@tepidmail.com",
			"orderdate":   time.Date(2020, 12, 26, 8, 55, 46, 0, time.UTC),
			"product_id":  "a1b2c3d4",
			"value":       429.65,
		},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Join orders to products
	statement := `db.orders.aggregate([
		{ $match: {
			orderdate: {
				$gte: ISODate("2020-01-01T00:00:00Z"),
				$lt: ISODate("2021-01-01T00:00:00Z")
			}
		}},
		{ $lookup: {
			from: "products",
			localField: "product_id",
			foreignField: "id",
			as: "product_mapping"
		}},
		{ $set: { product_mapping: { $first: "$product_mapping" } } },
		{ $set: {
			product_name: "$product_mapping.name",
			product_category: "$product_mapping.category"
		}},
		{ $unset: ["_id", "product_id", "product_mapping"] }
	])`

	result, err := gc.Execute(ctx, "testdb", statement)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.RowCount) // Only 2020 orders: elise, oranieri, jjones

	// Verify joined fields exist
	require.Contains(t, result.Rows[0], `"product_name"`)
	require.Contains(t, result.Rows[0], `"product_category"`)
	require.NotContains(t, result.Rows[0], `"_id"`)
	require.NotContains(t, result.Rows[0], `"product_mapping"`)
}

// TestAggregateMultiFieldJoin tests the "Multi-Field Join" example from MongoDB docs
// https://www.mongodb.com/docs/manual/tutorial/aggregation-examples/multi-field-join/
func TestAggregateMultiFieldJoin(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create products collection
	productsCollection := client.Database("testdb").Collection("products")
	_, err := productsCollection.InsertMany(ctx, []any{
		bson.M{
			"name":        "Asus Laptop",
			"variation":   "Ultra HD",
			"category":    "ELECTRONICS",
			"description": "Great for watching movies",
		},
		bson.M{
			"name":        "Asus Laptop",
			"variation":   "Normal Display",
			"category":    "ELECTRONICS",
			"description": "Good value laptop for students",
		},
		bson.M{
			"name":        "The Day Of The Triffids",
			"variation":   "1st Edition",
			"category":    "BOOKS",
			"description": "Classic post-apocalyptic novel",
		},
		bson.M{
			"name":        "The Day Of The Triffids",
			"variation":   "2nd Edition",
			"category":    "BOOKS",
			"description": "Classic post-apocalyptic novel",
		},
		bson.M{
			"name":        "Morphy Richards Food Mixer",
			"variation":   "Deluxe",
			"category":    "KITCHENWARE",
			"description": "Luxury mixer turning good cakes into great",
		},
		bson.M{
			"name":        "Karcher Hose Set",
			"variation":   "Full Monty",
			"category":    "GARDEN",
			"description": "Hose + nozzles + winder for tidy storage",
		},
	})
	require.NoError(t, err)

	// Create orders collection
	ordersCollection := client.Database("testdb").Collection("orders")
	_, err = ordersCollection.InsertMany(ctx, []any{
		bson.M{
			"customer_id":       "elise_smith@myemail.com",
			"orderdate":         time.Date(2020, 5, 30, 8, 35, 52, 0, time.UTC),
			"product_name":      "Asus Laptop",
			"product_variation": "Normal Display",
			"value":             431.43,
		},
		bson.M{
			"customer_id":       "tj@wheresmyemail.com",
			"orderdate":         time.Date(2019, 5, 28, 19, 13, 32, 0, time.UTC),
			"product_name":      "The Day Of The Triffids",
			"product_variation": "2nd Edition",
			"value":             5.01,
		},
		bson.M{
			"customer_id":       "oranieri@warmmail.com",
			"orderdate":         time.Date(2020, 1, 1, 8, 25, 37, 0, time.UTC),
			"product_name":      "Morphy Richards Food Mixer",
			"product_variation": "Deluxe",
			"value":             63.13,
		},
		bson.M{
			"customer_id":       "jjones@tepidmail.com",
			"orderdate":         time.Date(2020, 12, 26, 8, 55, 46, 0, time.UTC),
			"product_name":      "Asus Laptop",
			"product_variation": "Normal Display",
			"value":             429.65,
		},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Multi-field join using $lookup with let and pipeline
	statement := `db.products.aggregate([
		{ $lookup: {
			from: "orders",
			let: { prdname: "$name", prdvartn: "$variation" },
			pipeline: [
				{ $match: {
					$expr: {
						$and: [
							{ $eq: ["$product_name", "$$prdname"] },
							{ $eq: ["$product_variation", "$$prdvartn"] }
						]
					}
				}},
				{ $match: {
					orderdate: {
						$gte: ISODate("2020-01-01T00:00:00Z"),
						$lt: ISODate("2021-01-01T00:00:00Z")
					}
				}},
				{ $unset: ["_id", "product_name", "product_variation"] }
			],
			as: "orders"
		}},
		{ $match: { orders: { $ne: [] } } },
		{ $unset: ["_id"] }
	])`

	result, err := gc.Execute(ctx, "testdb", statement)
	require.NoError(t, err)
	require.NotNil(t, result)
	// Should have: Asus Laptop Normal Display (2 orders), Morphy Richards (1 order)
	require.Equal(t, 2, result.RowCount)

	// Verify structure
	require.Contains(t, result.Rows[0], `"orders"`)
	require.Contains(t, result.Rows[0], `"name"`)
	require.Contains(t, result.Rows[0], `"variation"`)
	require.NotContains(t, result.Rows[0], `"_id"`)
}

func TestGetCollectionInfos(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database("testdb").Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)
	_, err = client.Database("testdb").Collection("orders").InsertOne(ctx, bson.M{"item": "book"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test without filter - should return all collections
	result, err := gc.Execute(ctx, "testdb", "db.getCollectionInfos()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount)

	// Verify that results contain collection info structure
	for _, row := range result.Rows {
		require.Contains(t, row, `"name"`)
		require.Contains(t, row, `"type"`)
	}
}

func TestGetCollectionInfosWithFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database("testdb").Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)
	_, err = client.Database("testdb").Collection("orders").InsertOne(ctx, bson.M{"item": "book"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with filter - should return only matching collection
	result, err := gc.Execute(ctx, "testdb", `db.getCollectionInfos({ name: "users" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)

	// Verify that the returned collection is "users"
	require.Contains(t, result.Rows[0], `"name": "users"`)
	require.Contains(t, result.Rows[0], `"type": "collection"`)
}

func TestGetCollectionInfosEmptyResult(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection
	_, err := client.Database("testdb").Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with filter that matches no collections
	result, err := gc.Execute(ctx, "testdb", `db.getCollectionInfos({ name: "nonexistent" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 0, result.RowCount)
	require.Empty(t, result.Rows)
}

func TestGetIndexes(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with a document (this creates the default _id index)
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertOne(ctx, bson.M{"name": "alice", "email": "alice@example.com"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test getIndexes - should return at least the _id index
	result, err := gc.Execute(ctx, "testdb", "db.users.getIndexes()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.GreaterOrEqual(t, result.RowCount, 1)

	// Verify the _id index exists
	found := false
	for _, row := range result.Rows {
		if strings.Contains(row, `"name": "_id_"`) {
			found = true
			break
		}
	}
	require.True(t, found, "expected _id_ index")
}

func TestGetIndexesWithCustomIndex(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection and add a custom index
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertOne(ctx, bson.M{"name": "alice", "email": "alice@example.com"})
	require.NoError(t, err)

	// Create an index on the email field
	_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "email", Value: 1}},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, "testdb", "db.users.getIndexes()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount) // _id index + email index

	// Verify both indexes exist
	hasIdIndex := false
	hasEmailIndex := false
	for _, row := range result.Rows {
		if strings.Contains(row, `"name": "_id_"`) {
			hasIdIndex = true
		}
		if strings.Contains(row, `"email"`) {
			hasEmailIndex = true
		}
	}
	require.True(t, hasIdIndex, "expected _id_ index")
	require.True(t, hasEmailIndex, "expected email index")
}

func TestGetIndexesBracketNotation(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with hyphenated name
	collection := client.Database("testdb").Collection("user-logs")
	_, err := collection.InsertOne(ctx, bson.M{"message": "test"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with bracket notation
	result, err := gc.Execute(ctx, "testdb", `db["user-logs"].getIndexes()`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.GreaterOrEqual(t, result.RowCount, 1)

	// Verify the _id index exists
	require.Contains(t, result.Rows[0], `"name": "_id_"`)
}

func TestCountDocuments(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30},
		bson.M{"name": "bob", "age": 25},
		bson.M{"name": "charlie", "age": 35},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test countDocuments without filter
	result, err := gc.Execute(ctx, "testdb", "db.users.countDocuments()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Equal(t, "3", result.Rows[0])
}

func TestCountDocumentsWithFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30, "status": "active"},
		bson.M{"name": "bob", "age": 25, "status": "inactive"},
		bson.M{"name": "charlie", "age": 35, "status": "active"},
		bson.M{"name": "diana", "age": 28, "status": "active"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test countDocuments with filter
	result, err := gc.Execute(ctx, "testdb", `db.users.countDocuments({ status: "active" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Equal(t, "3", result.Rows[0])

	// Test with comparison operator
	result, err = gc.Execute(ctx, "testdb", `db.users.countDocuments({ age: { $gte: 30 } })`)
	require.NoError(t, err)
	require.Equal(t, "2", result.Rows[0])
}

func TestCountDocumentsEmptyCollection(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	// Test countDocuments on empty/non-existent collection
	result, err := gc.Execute(ctx, "testdb", "db.users.countDocuments()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Equal(t, "0", result.Rows[0])
}

func TestCountDocumentsWithEmptyFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("items")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"item": "a"},
		bson.M{"item": "b"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test countDocuments with empty filter {}
	result, err := gc.Execute(ctx, "testdb", "db.items.countDocuments({})")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "2", result.Rows[0])
}

func TestCountDocumentsWithOptions(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "age": 30},
		bson.M{"name": "bob", "age": 25},
		bson.M{"name": "charlie", "age": 35},
		bson.M{"name": "diana", "age": 28},
		bson.M{"name": "eve", "age": 32},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with limit option
	result, err := gc.Execute(ctx, "testdb", `db.users.countDocuments({}, { limit: 3 })`)
	require.NoError(t, err)
	require.Equal(t, "3", result.Rows[0])

	// Test with skip option
	result, err = gc.Execute(ctx, "testdb", `db.users.countDocuments({}, { skip: 2 })`)
	require.NoError(t, err)
	require.Equal(t, "3", result.Rows[0])

	// Test with both limit and skip
	result, err = gc.Execute(ctx, "testdb", `db.users.countDocuments({}, { skip: 1, limit: 2 })`)
	require.NoError(t, err)
	require.Equal(t, "2", result.Rows[0])
}

func TestCountDocumentsWithHint(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents and an index
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "status": "active"},
		bson.M{"name": "bob", "status": "inactive"},
		bson.M{"name": "charlie", "status": "active"},
	})
	require.NoError(t, err)

	// Create an index on status
	_, err = collection.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.D{{Key: "status", Value: 1}},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with hint using index name
	result, err := gc.Execute(ctx, "testdb", `db.users.countDocuments({ status: "active" }, { hint: "status_1" })`)
	require.NoError(t, err)
	require.Equal(t, "2", result.Rows[0])

	// Test with hint using index specification document
	result, err = gc.Execute(ctx, "testdb", `db.users.countDocuments({ status: "active" }, { hint: { status: 1 } })`)
	require.NoError(t, err)
	require.Equal(t, "2", result.Rows[0])
}

func TestEstimatedDocumentCount(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice"},
		bson.M{"name": "bob"},
		bson.M{"name": "charlie"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test estimatedDocumentCount
	result, err := gc.Execute(ctx, "testdb", "db.users.estimatedDocumentCount()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Equal(t, "3", result.Rows[0])
}

func TestEstimatedDocumentCountEmptyCollection(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	// Test estimatedDocumentCount on empty/non-existent collection
	result, err := gc.Execute(ctx, "testdb", "db.users.estimatedDocumentCount()")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Equal(t, "0", result.Rows[0])
}

func TestEstimatedDocumentCountWithEmptyOptions(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("items")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"item": "a"},
		bson.M{"item": "b"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test estimatedDocumentCount with empty options {}
	result, err := gc.Execute(ctx, "testdb", "db.items.estimatedDocumentCount({})")
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, "2", result.Rows[0])
}

func TestDistinct(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("users")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"name": "alice", "status": "active"},
		bson.M{"name": "bob", "status": "inactive"},
		bson.M{"name": "charlie", "status": "active"},
		bson.M{"name": "diana", "status": "active"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test distinct on status field
	result, err := gc.Execute(ctx, "testdb", `db.users.distinct("status")`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount)

	// Verify both values are present
	values := make(map[string]bool)
	for _, row := range result.Rows {
		values[row] = true
	}
	require.True(t, values[`"active"`] || values[`"inactive"`])
}

func TestDistinctWithFilter(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with documents
	collection := client.Database("testdb").Collection("products")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"category": "electronics", "brand": "Apple", "price": 999},
		bson.M{"category": "electronics", "brand": "Samsung", "price": 799},
		bson.M{"category": "electronics", "brand": "Apple", "price": 1299},
		bson.M{"category": "clothing", "brand": "Nike", "price": 99},
		bson.M{"category": "clothing", "brand": "Adidas", "price": 89},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test distinct with filter
	result, err := gc.Execute(ctx, "testdb", `db.products.distinct("brand", { category: "electronics" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 2, result.RowCount)

	// Verify only electronics brands are returned
	values := make(map[string]bool)
	for _, row := range result.Rows {
		values[row] = true
	}
	require.True(t, values[`"Apple"`])
	require.True(t, values[`"Samsung"`])
	require.False(t, values[`"Nike"`])
	require.False(t, values[`"Adidas"`])
}

func TestDistinctEmptyCollection(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	// Test distinct on empty/non-existent collection
	result, err := gc.Execute(ctx, "testdb", `db.users.distinct("status")`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 0, result.RowCount)
	require.Empty(t, result.Rows)
}

func TestDistinctBracketNotation(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with hyphenated name
	collection := client.Database("testdb").Collection("user-logs")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"level": "info"},
		bson.M{"level": "warn"},
		bson.M{"level": "error"},
		bson.M{"level": "info"},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with bracket notation
	result, err := gc.Execute(ctx, "testdb", `db["user-logs"].distinct("level")`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.RowCount)
}

func TestDistinctNumericValues(t *testing.T) {
	client, cleanup := setupTestContainer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a collection with numeric values
	collection := client.Database("testdb").Collection("scores")
	_, err := collection.InsertMany(ctx, []any{
		bson.M{"score": 100},
		bson.M{"score": 85},
		bson.M{"score": 100},
		bson.M{"score": 90},
		bson.M{"score": 85},
	})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test distinct on numeric field
	result, err := gc.Execute(ctx, "testdb", `db.scores.distinct("score")`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 3, result.RowCount) // 100, 85, 90
}
