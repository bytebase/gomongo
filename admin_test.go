package gomongo_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// containsCollectionName checks if the values contain the given collection name.
// Values can be strings (from show collections) or bson.D documents.
func containsCollectionName(values []any, name string) bool {
	for _, v := range values {
		// show collections returns strings
		if s, ok := v.(string); ok && s == name {
			return true
		}
		// getCollectionInfos returns bson.D
		if doc, ok := v.(bson.D); ok {
			for _, elem := range doc {
				if elem.Key == "name" && elem.Value == name {
					return true
				}
			}
		}
	}
	return false
}

// containsDatabaseName checks if the values contain the given database name.
// Values are strings from show dbs.
func containsDatabaseName(values []any, name string) bool {
	for _, v := range values {
		// show dbs returns strings
		if s, ok := v.(string); ok && s == name {
			return true
		}
	}
	return false
}

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
		require.Equal(t, 1, len(result.Value))
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, "name")
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
		require.Equal(t, 1, len(result.Value))
		// The returned value is the index name as string
		indexName, ok := result.Value[0].(string)
		require.True(t, ok)
		require.Equal(t, "email_unique_idx", indexName)
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
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)
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
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)

		// Verify only _id index remains
		idxResult, err := gc.Execute(ctx, dbName, `db.users.getIndexes()`)
		require.NoError(t, err)
		require.Equal(t, 1, len(idxResult.Value)) // Only _id index
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
		require.Equal(t, 1, len(result.Value))

		// Drop the collection
		result, err = gc.Execute(ctx, dbName, `db.tobedeleted.drop()`)
		require.NoError(t, err)
		// The returned value is a boolean
		dropped, ok := result.Value[0].(bool)
		require.True(t, ok)
		require.True(t, dropped)

		// Verify collection is gone
		result, err = gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 0, len(result.Value))
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
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)

		// Verify collection exists
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, len(collResult.Value))
		require.True(t, containsCollectionName(collResult.Value, "newcollection"), "expected 'newcollection' in result")
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
		require.True(t, containsDatabaseName(result.Value, dbName), "database should exist before drop")

		// Drop the database
		result, err = gc.Execute(ctx, dbName, `db.dropDatabase()`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)
		require.Contains(t, row, fmt.Sprintf(`"dropped": "%s"`, dbName))
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
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)

		// Verify old collection is gone and new one exists
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, len(collResult.Value))
		require.True(t, containsCollectionName(collResult.Value, "newname"), "expected 'newname' in result")

		// Verify data is preserved
		findResult, err := gc.Execute(ctx, dbName, `db.newname.find()`)
		require.NoError(t, err)
		require.Equal(t, 1, len(findResult.Value))
		findRow := valueToJSON(findResult.Value[0])
		require.Contains(t, findRow, `"x": 1`)
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
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)

		// Verify only target exists with source data
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, len(collResult.Value))
		require.True(t, containsCollectionName(collResult.Value, "target"), "expected 'target' in result")

		// Verify it has source data, not old target data
		findResult, err := gc.Execute(ctx, dbName, `db.target.find()`)
		require.NoError(t, err)
		require.Equal(t, 1, len(findResult.Value))
		findRow := valueToJSON(findResult.Value[0])
		require.Contains(t, findRow, `"x": 1`)
	})
}

func TestCreateCollectionWithOptions(t *testing.T) {
	// Capped collections are not supported on DocumentDB
	testutil.RunOnMongoDBOnly(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_coll_opts_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Create a capped collection
		result, err := gc.Execute(ctx, dbName, `db.createCollection("cappedcoll", { capped: true, size: 1048576 })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)

		// Verify collection exists
		collResult, err := gc.Execute(ctx, dbName, `show collections`)
		require.NoError(t, err)
		require.Equal(t, 1, len(collResult.Value))
		require.True(t, containsCollectionName(collResult.Value, "cappedcoll"), "expected 'cappedcoll' in result")
	})
}

func TestCreateCollectionWithMaxDocuments(t *testing.T) {
	// Capped collections are not supported on DocumentDB
	testutil.RunOnMongoDBOnly(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_coll_max_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Create a capped collection with max documents
		result, err := gc.Execute(ctx, dbName, `db.createCollection("cappedmax", { capped: true, size: 1048576, max: 100 })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)
	})
}

func TestDropIndexesWithArray(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_drop_idxs_arr_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create a collection with indexes
		collection := db.Client.Database(dbName).Collection("users")
		_, err := collection.InsertOne(ctx, bson.M{"name": "alice", "email": "alice@example.com", "age": 30})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create indexes with explicit names
		_, err = gc.Execute(ctx, dbName, `db.users.createIndex({ name: 1 }, { name: "name_idx" })`)
		require.NoError(t, err)
		_, err = gc.Execute(ctx, dbName, `db.users.createIndex({ email: 1 }, { name: "email_idx" })`)
		require.NoError(t, err)
		_, err = gc.Execute(ctx, dbName, `db.users.createIndex({ age: 1 }, { name: "age_idx" })`)
		require.NoError(t, err)

		// Drop two indexes using an array
		result, err := gc.Execute(ctx, dbName, `db.users.dropIndexes(["name_idx", "email_idx"])`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok": 1`)

		// Verify only _id and age_idx remain
		idxResult, err := gc.Execute(ctx, dbName, `db.users.getIndexes()`)
		require.NoError(t, err)
		require.Equal(t, 2, len(idxResult.Value)) // _id + age_idx
	})
}

func TestCreateIndexWithUniqueOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idx_unique_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"email": "alice@example.com"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create a unique index
		result, err := gc.Execute(ctx, dbName, `db.users.createIndex({ email: 1 }, { unique: true, name: "email_unique" })`)
		require.NoError(t, err)
		indexName, ok := result.Value[0].(string)
		require.True(t, ok)
		require.Equal(t, "email_unique", indexName)

		// Try to insert a duplicate - should fail
		_, err = gc.Execute(ctx, dbName, `db.users.insertOne({ email: "alice@example.com" })`)
		require.Error(t, err)
		// Different databases may use different error messages
		errStr := err.Error()
		require.True(t, strings.Contains(errStr, "duplicate key") || strings.Contains(errStr, "Duplicate key"),
			"expected duplicate key error, got: %s", errStr)
	})
}

func TestCreateIndexWithSparseOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idx_sparse_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create a sparse index
		result, err := gc.Execute(ctx, dbName, `db.users.createIndex({ email: 1 }, { sparse: true, name: "email_sparse" })`)
		require.NoError(t, err)
		indexName, ok := result.Value[0].(string)
		require.True(t, ok)
		require.Equal(t, "email_sparse", indexName)

		// Documents without the indexed field should still be insertable
		_, err = gc.Execute(ctx, dbName, `db.users.insertOne({ name: "bob" })`)
		require.NoError(t, err)
	})
}

func TestCreateIndexWithTTLOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idx_ttl_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("sessions").InsertOne(ctx, bson.M{"createdAt": bson.M{"$date": "2024-01-01T00:00:00Z"}})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create a TTL index
		result, err := gc.Execute(ctx, dbName, `db.sessions.createIndex({ createdAt: 1 }, { expireAfterSeconds: 3600, name: "session_ttl" })`)
		require.NoError(t, err)
		indexName, ok := result.Value[0].(string)
		require.True(t, ok)
		require.Equal(t, "session_ttl", indexName)
	})
}

func TestCreateIndexWithBackgroundOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idx_bg_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// Create an index with background option (deprecated but should be accepted)
		result, err := gc.Execute(ctx, dbName, `db.users.createIndex({ name: 1 }, { background: true })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, "name")
	})
}
