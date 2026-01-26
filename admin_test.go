package gomongo_test

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCreateIndex(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idx_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection first
		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create an index on the 'name' field
		result, err := gc.Execute(ctx, dbName, `db.users.createIndex({ name: 1 })`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "name")
	})
}

func TestCreateIndexWithOptions(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idx_opts_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection first
		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"email": "alice@example.com"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create a unique index with a custom name
		result, err := gc.Execute(ctx, dbName, `db.users.createIndex({ email: 1 }, { name: "email_unique_idx" })`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, result.RowCount)
		require.Equal(t, "email_unique_idx", result.Rows[0])
	})
}

func TestDropIndex(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_drop_idx_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection with an index
		collection := db.Client.Database(dbName).Collection("users")
		_, err := collection.InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create an index first
		_, err = gc.Execute(ctx, dbName, `db.users.createIndex({ name: 1 }, { name: "name_idx" })`)
		require.NoError(t, err)

		// Drop the index by name
		result, err := gc.Execute(ctx, dbName, `db.users.dropIndex("name_idx")`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.Rows[0], `"ok": 1`)
	})
}

func TestDropIndexes(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_drop_idxs_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection with indexes
		collection := db.Client.Database(dbName).Collection("users")
		_, err := collection.InsertOne(ctx, bson.M{"name": "alice", "email": "alice@example.com"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create some indexes
		_, err = gc.Execute(ctx, dbName, `db.users.createIndex({ name: 1 })`)
		require.NoError(t, err)
		_, err = gc.Execute(ctx, dbName, `db.users.createIndex({ email: 1 })`)
		require.NoError(t, err)

		// Drop all indexes
		result, err := gc.Execute(ctx, dbName, `db.users.dropIndexes()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Contains(t, result.Rows[0], `"ok": 1`)

		// Verify only _id index remains
		idxResult, err := gc.Execute(ctx, dbName, `db.users.getIndexes()`)
		require.NoError(t, err)
		require.Equal(t, 1, idxResult.RowCount) // Only _id index
	})
}

func TestDropCollection(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_drop_coll_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection
		_, err := db.Client.Database(dbName).Collection("tobedeleted").InsertOne(ctx, bson.M{"x": 1})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Verify collection exists
		result, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)

		// Drop the collection
		result, err = gc.Execute(ctx, dbName, `db.tobedeleted.drop()`)
		require.NoError(t, err)
		require.Equal(t, "true", result.Rows[0])

		// Verify collection is gone
		result, err = gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 0, result.RowCount)
	})
}

func TestCreateCollection(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_coll_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Create a new collection
		result, err := gc.Execute(ctx, dbName, `db.createCollection("newcollection")`)
		require.NoError(t, err)
		require.Contains(t, result.Rows[0], `"ok": 1`)

		// Verify collection exists
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, collResult.RowCount)
		require.Equal(t, "newcollection", collResult.Rows[0])
	})
}

func TestDropDatabase(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_drop_db_%s", db.Name)
		// No defer cleanup since we're dropping the database

		ctx := context.Background()

		// Create a database by inserting a document
		_, err := db.Client.Database(dbName).Collection("test").InsertOne(ctx, bson.M{"x": 1})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Verify database exists
		result, err := gc.Execute(ctx, dbName, `show dbs`)
		require.NoError(t, err)
		require.True(t, slices.Contains(result.Rows, dbName), "database should exist before drop")

		// Drop the database
		result, err = gc.Execute(ctx, dbName, `db.dropDatabase()`)
		require.NoError(t, err)
		require.Contains(t, result.Rows[0], `"ok": 1`)
		require.Contains(t, result.Rows[0], fmt.Sprintf(`"dropped": "%s"`, dbName))
	})
}

func TestRenameCollection(t *testing.T) {
	testutil.RunOnMongoDBOnly(t, func(t *testing.T, db testutil.TestDB) {
		// renameCollection requires admin privileges and may not work on all DB types
		dbName := fmt.Sprintf("testdb_rename_coll_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection with documents
		_, err := db.Client.Database(dbName).Collection("oldname").InsertOne(ctx, bson.M{"x": 1})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Rename the collection
		result, err := gc.Execute(ctx, dbName, `db.oldname.renameCollection("newname")`)
		require.NoError(t, err)
		require.Contains(t, result.Rows[0], `"ok": 1`)

		// Verify old collection is gone and new one exists
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, collResult.RowCount)
		require.Equal(t, "newname", collResult.Rows[0])

		// Verify data is preserved
		findResult, err := gc.Execute(ctx, dbName, `db.newname.find()`)
		require.NoError(t, err)
		require.Equal(t, 1, findResult.RowCount)
		require.Contains(t, findResult.Rows[0], `"x": 1`)
	})
}

func TestRenameCollectionWithDropTarget(t *testing.T) {
	testutil.RunOnMongoDBOnly(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_rename_drop_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create source collection
		_, err := db.Client.Database(dbName).Collection("source").InsertOne(ctx, bson.M{"x": 1})
		require.NoError(t, err)

		// Create target collection that will be dropped
		_, err = db.Client.Database(dbName).Collection("target").InsertOne(ctx, bson.M{"y": 2})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Rename with dropTarget = true
		result, err := gc.Execute(ctx, dbName, `db.source.renameCollection("target", true)`)
		require.NoError(t, err)
		require.Contains(t, result.Rows[0], `"ok": 1`)

		// Verify only target exists with source data
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, collResult.RowCount)
		require.Equal(t, "target", collResult.Rows[0])

		// Verify it has source data, not old target data
		findResult, err := gc.Execute(ctx, dbName, `db.target.find()`)
		require.NoError(t, err)
		require.Equal(t, 1, findResult.RowCount)
		require.Contains(t, findResult.Rows[0], `"x": 1`)
	})
}
