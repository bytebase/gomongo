package gomongo_test

import (
	"context"
	"slices"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestShowDatabases(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_show_dbs"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create a database by inserting a document
	_, err := client.Database(dbName).Collection("test").InsertOne(ctx, bson.M{"x": 1})
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
			result, err := gc.Execute(ctx, dbName, tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
			require.GreaterOrEqual(t, result.RowCount, 1)

			// Check that dbName is in the result
			require.True(t, slices.Contains(result.Rows, dbName), "expected '%s' in database list, got: %v", dbName, result.Rows)
		})
	}
}

func TestShowCollections(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_show_colls"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)
	_, err = client.Database(dbName).Collection("orders").InsertOne(ctx, bson.M{"item": "book"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, "show collections")
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
	client := testutil.GetClient(t)
	dbName := "testdb_get_coll_names"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database(dbName).Collection("products").InsertOne(ctx, bson.M{"name": "widget"})
	require.NoError(t, err)
	_, err = client.Database(dbName).Collection("categories").InsertOne(ctx, bson.M{"name": "electronics"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, "db.getCollectionNames()")
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

func TestGetCollectionInfos(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_get_coll_infos"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)
	_, err = client.Database(dbName).Collection("orders").InsertOne(ctx, bson.M{"item": "book"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test without filter - should return all collections
	result, err := gc.Execute(ctx, dbName, "db.getCollectionInfos()")
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
	client := testutil.GetClient(t)
	dbName := "testdb_coll_infos_filter"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create collections by inserting documents
	_, err := client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)
	_, err = client.Database(dbName).Collection("orders").InsertOne(ctx, bson.M{"item": "book"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with filter - should return only matching collection
	result, err := gc.Execute(ctx, dbName, `db.getCollectionInfos({ name: "users" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)

	// Verify that the returned collection is "users"
	require.Contains(t, result.Rows[0], `"name": "users"`)
	require.Contains(t, result.Rows[0], `"type": "collection"`)
}

func TestGetCollectionInfosEmptyResult(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_coll_infos_empty"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create a collection
	_, err := client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	// Test with filter that matches no collections
	result, err := gc.Execute(ctx, dbName, `db.getCollectionInfos({ name: "nonexistent" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 0, result.RowCount)
	require.Empty(t, result.Rows)
}

func TestGetCollectionInfosNameOnly(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_coll_infos_nameonly"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create a collection
	_, err := client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "test"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.getCollectionInfos({}, { nameOnly: true })`)
	require.NoError(t, err)
	require.GreaterOrEqual(t, result.RowCount, 1)

	// With nameOnly: true, the result should contain "name" field
	require.Contains(t, result.Rows[0], `"name"`)
}

func TestGetCollectionInfosAuthorizedCollections(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_coll_infos_auth"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Create a collection
	_, err := client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "test"})
	require.NoError(t, err)

	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.getCollectionInfos({}, { authorizedCollections: true })`)
	require.NoError(t, err)
	require.GreaterOrEqual(t, result.RowCount, 1)
}

func TestGetCollectionInfosUnsupportedOption(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_coll_infos_unsup"
	defer testutil.CleanupDatabase(t, client, dbName)

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	_, err := gc.Execute(ctx, dbName, `db.getCollectionInfos({}, { unknownOption: true })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "getCollectionInfos()", optErr.Method)
}

func TestGetCollectionInfosTooManyArgs(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_coll_infos_args"
	defer testutil.CleanupDatabase(t, client, dbName)

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	_, err := gc.Execute(ctx, dbName, `db.getCollectionInfos({}, {}, {})`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "takes at most 2 arguments")
}

func TestCollectionAccessPatterns(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_coll_access"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	// Insert a document
	collection := client.Database(dbName).Collection("my-collection")
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
			result, err := gc.Execute(ctx, dbName, tc.statement)
			require.NoError(t, err)
			require.NotNil(t, result)
		})
	}
}
