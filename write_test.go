package gomongo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestInsertOneBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_one_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert a document
		result, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"acknowledged": true`)
		require.Contains(t, row, `"insertedId"`)

		// Verify document was inserted
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.find({ name: "alice" })`)
		require.NoError(t, err)
		require.Equal(t, 1, len(verifyResult.Value))
		verifyRow := valueToJSON(verifyResult.Value[0])
		require.Contains(t, verifyRow, `"alice"`)
		require.Contains(t, verifyRow, `"age": 30`)
	})
}

func TestInsertOneWithObjectId(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_one_oid_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert with explicit ObjectId
		result, err := gc.Execute(ctx, dbName, `db.users.insertOne({ _id: ObjectId("507f1f77bcf86cd799439011"), name: "bob" })`)
		require.NoError(t, err)
		require.NotNil(t, result)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"507f1f77bcf86cd799439011"`)

		// Verify
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ _id: ObjectId("507f1f77bcf86cd799439011") })`)
		require.NoError(t, err)
		require.Equal(t, 1, len(verifyResult.Value))
		verifyRow := valueToJSON(verifyResult.Value[0])
		require.Contains(t, verifyRow, `"bob"`)
	})
}

func TestInsertOneWithNestedDocument(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_one_nested_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.insertOne({
		name: "carol",
		address: { city: "NYC", zip: "10001" },
		tags: ["admin", "user"]
	})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify nested structure
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "carol" })`)
		require.NoError(t, err)
		verifyRow := valueToJSON(verifyResult.Value[0])
		require.Contains(t, verifyRow, `"city": "NYC"`)
		require.Contains(t, verifyRow, `"admin"`)
	})
}

func TestInsertOneMissingDocument(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_one_missing_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Note: When insertOne() is called without arguments, the parser may not
		// recognize it as InsertOneMethod (grammar limitation). The error message
		// varies based on parser behavior - it may be "unsupported operation" or
		// "requires a document". Either way, it should be an error.
		_, err := gc.Execute(ctx, dbName, `db.users.insertOne()`)
		require.Error(t, err)
	})
}

func TestInsertOneInvalidDocument(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_one_invalid_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertOne("not a document")`)
		require.Error(t, err)
		require.Contains(t, err.Error(), "must be an object")
	})
}

func TestInsertManyBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_many_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", age: 30 },
		{ name: "bob", age: 25 },
		{ name: "carol", age: 35 }
	])`)
		require.NoError(t, err)
		require.NotNil(t, result)
		require.Equal(t, 1, len(result.Value))
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"acknowledged": true`)
		require.Contains(t, row, `"insertedIds"`)

		// Verify all documents were inserted
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
		require.NoError(t, err)
		count, ok := verifyResult.Value[0].(int64)
		require.True(t, ok)
		require.Equal(t, int64(3), count)
	})
}

func TestInsertManyEmpty(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_many_empty_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([])`)
		require.Error(t, err) // MongoDB doesn't allow empty array
	})
}

func TestUpdateOneBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_one_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert test data
		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
		require.NoError(t, err)

		// Update
		result, err := gc.Execute(ctx, dbName, `db.users.updateOne({ name: "alice" }, { $set: { age: 31 } })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"acknowledged": true`)
		require.Contains(t, row, `"matchedCount": 1`)
		require.Contains(t, row, `"modifiedCount": 1`)

		// Verify
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "alice" })`)
		require.NoError(t, err)
		verifyRow := valueToJSON(verifyResult.Value[0])
		require.Contains(t, verifyRow, `"age": 31`)
	})
}

func TestUpdateOneNoMatch(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_one_no_match_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.updateOne({ name: "nobody" }, { $set: { age: 99 } })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"matchedCount": 0`)
		require.Contains(t, row, `"modifiedCount": 0`)
	})
}

func TestUpdateOneUpsert(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_one_upsert_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.updateOne(
		{ name: "newuser" },
		{ $set: { age: 25 } },
		{ upsert: true }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"upsertedId"`)

		// Verify upserted document
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "newuser" })`)
		require.NoError(t, err)
		require.Equal(t, 1, len(verifyResult.Value))
	})
}

func TestUpdateManyBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_many_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert test data
		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", status: "active" },
		{ name: "bob", status: "active" },
		{ name: "carol", status: "inactive" }
	])`)
		require.NoError(t, err)

		// Update all active users
		result, err := gc.Execute(ctx, dbName, `db.users.updateMany(
		{ status: "active" },
		{ $set: { verified: true } }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"matchedCount": 2`)
		require.Contains(t, row, `"modifiedCount": 2`)
	})
}

func TestUpdateManyNoMatch(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_many_no_match_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.updateMany({ status: "nonexistent" }, { $set: { verified: true } })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"matchedCount": 0`)
		require.Contains(t, row, `"modifiedCount": 0`)
	})
}

func TestUpdateManyUpsert(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_many_upsert_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.updateMany(
		{ status: "pending" },
		{ $set: { verified: false } },
		{ upsert: true }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"upsertedId"`)

		// Verify upserted document
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ status: "pending" })`)
		require.NoError(t, err)
		require.Equal(t, 1, len(verifyResult.Value))
	})
}

func TestReplaceOneBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_replace_one_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert test data
		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30, city: "NYC" })`)
		require.NoError(t, err)

		// Replace entire document
		result, err := gc.Execute(ctx, dbName, `db.users.replaceOne(
		{ name: "alice" },
		{ name: "alice", age: 31, country: "USA" }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"matchedCount": 1`)
		require.Contains(t, row, `"modifiedCount": 1`)

		// Verify - city should be gone, country should exist
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "alice" })`)
		require.NoError(t, err)
		verifyRow := valueToJSON(verifyResult.Value[0])
		require.Contains(t, verifyRow, `"country": "USA"`)
		require.NotContains(t, verifyRow, `"city"`)
	})
}

func TestReplaceOneUpsert(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_replace_one_upsert_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.replaceOne(
		{ name: "newuser" },
		{ name: "newuser", age: 25 },
		{ upsert: true }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"upsertedId"`)
	})
}

func TestDeleteOneBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_delete_one_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert test data
		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice" },
		{ name: "bob" },
		{ name: "carol" }
	])`)
		require.NoError(t, err)

		// Delete one
		result, err := gc.Execute(ctx, dbName, `db.users.deleteOne({ name: "bob" })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"acknowledged": true`)
		require.Contains(t, row, `"deletedCount": 1`)

		// Verify
		countResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
		require.NoError(t, err)
		count, ok := countResult.Value[0].(int64)
		require.True(t, ok)
		require.Equal(t, int64(2), count)
	})
}

func TestDeleteOneNoMatch(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_delete_one_no_match_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.deleteOne({ name: "nobody" })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"deletedCount": 0`)
	})
}

func TestDeleteManyBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_delete_many_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert test data
		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", status: "inactive" },
		{ name: "bob", status: "inactive" },
		{ name: "carol", status: "active" }
	])`)
		require.NoError(t, err)

		// Delete all inactive
		result, err := gc.Execute(ctx, dbName, `db.users.deleteMany({ status: "inactive" })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"deletedCount": 2`)

		// Verify only carol remains
		countResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
		require.NoError(t, err)
		count, ok := countResult.Value[0].(int64)
		require.True(t, ok)
		require.Equal(t, int64(1), count)
	})
}

func TestDeleteManyAll(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_delete_many_all_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert test data
		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice" },
		{ name: "bob" }
	])`)
		require.NoError(t, err)

		// Delete all with empty filter
		result, err := gc.Execute(ctx, dbName, `db.users.deleteMany({})`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"deletedCount": 2`)
	})
}

func TestFindOneAndUpdateBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_update_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
		require.NoError(t, err)

		// Returns document BEFORE update by default
		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndUpdate(
		{ name: "alice" },
		{ $set: { age: 31 } }
	)`)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Value))
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"age": 30`)
	})
}

func TestFindOneAndUpdateReturnAfter(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_update_after_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndUpdate(
		{ name: "alice" },
		{ $set: { age: 31 } },
		{ returnDocument: "after" }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"age": 31`)
	})
}

func TestFindOneAndUpdateNoMatch(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_update_no_match_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndUpdate(
		{ name: "nobody" },
		{ $set: { age: 99 } }
	)`)
		require.NoError(t, err)
		// No document found returns empty slice
		require.Equal(t, 0, len(result.Value))
	})
}

func TestFindOneAndReplaceBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_replace_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30, city: "NYC" })`)
		require.NoError(t, err)

		// Returns document BEFORE replacement
		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndReplace(
		{ name: "alice" },
		{ name: "alice", age: 31, country: "USA" }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"city": "NYC"`)
	})
}

func TestFindOneAndReplaceReturnAfter(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_replace_after_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndReplace(
		{ name: "alice" },
		{ name: "alice", age: 31 },
		{ returnDocument: "after" }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"age": 31`)
	})
}

func TestFindOneAndDeleteBasic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_delete_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", age: 30 },
		{ name: "bob", age: 25 }
	])`)
		require.NoError(t, err)

		// Returns the deleted document
		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndDelete({ name: "alice" })`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"alice"`)
		require.Contains(t, row, `"age": 30`)

		// Verify alice is deleted
		countResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
		require.NoError(t, err)
		count, ok := countResult.Value[0].(int64)
		require.True(t, ok)
		require.Equal(t, int64(1), count)
	})
}

func TestFindOneAndDeleteNoMatch(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_delete_no_match_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndDelete({ name: "nobody" })`)
		require.NoError(t, err)
		// No document found returns empty slice
		require.Equal(t, 0, len(result.Value))
	})
}

func TestFindOneAndDeleteWithSort(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_find_one_and_delete_sort_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", score: 10 },
		{ name: "alice", score: 20 }
	])`)
		require.NoError(t, err)

		// Delete the alice with lowest score
		result, err := gc.Execute(ctx, dbName, `db.users.findOneAndDelete(
		{ name: "alice" },
		{ sort: { score: 1 } }
	)`)
		require.NoError(t, err)
		row := valueToJSON(result.Value[0])
		require.Contains(t, row, `"score": 10`)

		// Verify only score=20 remains
		verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "alice" })`)
		require.NoError(t, err)
		verifyRow := valueToJSON(verifyResult.Value[0])
		require.Contains(t, verifyRow, `"score": 20`)
	})
}
