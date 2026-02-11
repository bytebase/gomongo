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

func TestCreateIndexes(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_create_idxs_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice", "email": "alice@example.com", "age": 30})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.createIndexes([{ key: { name: 1 }, name: "name_idx" }, { key: { email: 1 }, name: "email_idx" }])`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 2, len(result.Value))

		// Values should be index name strings
		name0, ok := result.Value[0].(string)
		require.True(t, ok)
		require.Equal(t, "name_idx", name0)
		name1, ok := result.Value[1].(string)
		require.True(t, ok)
		require.Equal(t, "email_idx", name1)

		// Verify indexes exist
		idxResult, err := gc.Execute(ctx, dbName, `db.users.getIndexes()`)
		require.NoError(t, err)
		require.Equal(t, 3, len(idxResult.Value)) // _id + name_idx + email_idx
	})
}

func TestDbStats(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_db_stats_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Create some data
		_, err := db.Client.Database(dbName).Collection("test").InsertOne(ctx, bson.M{"x": 1})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.stats()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"db"`)
		require.Contains(t, row, `"collections"`)
	})
}

func TestCollectionStats(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_coll_stats_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.stats()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ns"`)
		require.Contains(t, row, `"count"`)
	})
}

func TestServerStatus(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_server_status_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.serverStatus()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok"`)
	})
}

func TestServerBuildInfo(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_build_info_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.serverBuildInfo()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"version"`)
	})
}

func TestDbVersion(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_db_version_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.version()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		version, ok := result.Value[0].(string)
		require.True(t, ok)
		require.NotEmpty(t, version)
		// Version should look like a semver (e.g., "4.4.0", "8.0.0")
		require.True(t, strings.Contains(version, "."), "version should contain dots: %s", version)
	})
}

func TestHostInfo(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_host_info_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.hostInfo()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok"`)
	})
}

func TestListCommands(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_list_cmds_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.listCommands()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok"`)
		require.Contains(t, row, `"commands"`)
	})
}

func TestDataSize(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_data_size_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.dataSize()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))
		// dataSize returns a numeric value (int32 or int64)
		require.NotNil(t, result.Value[0])
	})
}

func TestStorageSize(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_storage_size_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.storageSize()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))
		require.NotNil(t, result.Value[0])
	})
}

func TestTotalIndexSize(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_total_idx_size_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.totalIndexSize()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))
		require.NotNil(t, result.Value[0])
	})
}

func TestTotalSize(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_total_size_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.totalSize()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))
		// totalSize is int64 (storageSize + totalIndexSize)
		_, ok := result.Value[0].(int64)
		require.True(t, ok, "expected int64, got %T", result.Value[0])
	})
}

func TestIsCapped(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_is_capped_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		// Regular collection should not be capped
		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.isCapped()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		capped, ok := result.Value[0].(bool)
		require.True(t, ok)
		require.False(t, capped)
	})
}

func TestIsCappedTrue(t *testing.T) {
	testutil.RunOnMongoDBOnly(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_is_capped_true_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Create a capped collection
		_, err := gc.Execute(ctx, dbName, `db.createCollection("capped_coll", { capped: true, size: 1048576 })`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.capped_coll.isCapped()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		capped, ok := result.Value[0].(bool)
		require.True(t, ok)
		require.True(t, capped)
	})
}

func TestValidate(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_validate_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.validate()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"ok"`)
		require.Contains(t, row, `"ns"`)
	})
}

func TestLatencyStats(t *testing.T) {
	// latencyStats uses $collStats aggregation which may not be available on all platforms
	testutil.RunOnMongoDBOnly(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_latency_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		_, err := db.Client.Database(dbName).Collection("users").InsertOne(ctx, bson.M{"name": "alice"})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.latencyStats()`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.GreaterOrEqual(t, len(result.Value), 1)

		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"latencyStats"`)
	})
}
