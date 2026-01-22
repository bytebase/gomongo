package gomongo_test

import (
	"context"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestInsertOneBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_one"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	// Insert a document
	result, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Contains(t, result.Rows[0], `"acknowledged": true`)
	require.Contains(t, result.Rows[0], `"insertedId"`)

	// Verify document was inserted
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.find({ name: "alice" })`)
	require.NoError(t, err)
	require.Equal(t, 1, verifyResult.RowCount)
	require.Contains(t, verifyResult.Rows[0], `"alice"`)
	require.Contains(t, verifyResult.Rows[0], `"age": 30`)
}

func TestInsertOneWithObjectId(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_one_oid"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	// Insert with explicit ObjectId
	result, err := gc.Execute(ctx, dbName, `db.users.insertOne({ _id: ObjectId("507f1f77bcf86cd799439011"), name: "bob" })`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Contains(t, result.Rows[0], `"507f1f77bcf86cd799439011"`)

	// Verify
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ _id: ObjectId("507f1f77bcf86cd799439011") })`)
	require.NoError(t, err)
	require.Equal(t, 1, verifyResult.RowCount)
	require.Contains(t, verifyResult.Rows[0], `"bob"`)
}

func TestInsertOneWithNestedDocument(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_one_nested"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

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
	require.Contains(t, verifyResult.Rows[0], `"city": "NYC"`)
	require.Contains(t, verifyResult.Rows[0], `"admin"`)
}

func TestInsertOneMissingDocument(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_one_missing"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	// Note: When insertOne() is called without arguments, the parser may not
	// recognize it as InsertOneMethod (grammar limitation). The error message
	// varies based on parser behavior - it may be "unsupported operation" or
	// "requires a document". Either way, it should be an error.
	_, err := gc.Execute(ctx, dbName, `db.users.insertOne()`)
	require.Error(t, err)
}

func TestInsertOneInvalidDocument(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_one_invalid"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertOne("not a document")`)
	require.Error(t, err)
	require.Contains(t, err.Error(), "must be an object")
}

func TestInsertManyBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_many"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", age: 30 },
		{ name: "bob", age: 25 },
		{ name: "carol", age: 35 }
	])`)
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, 1, result.RowCount)
	require.Contains(t, result.Rows[0], `"acknowledged": true`)
	require.Contains(t, result.Rows[0], `"insertedIds"`)

	// Verify all documents were inserted
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
	require.NoError(t, err)
	require.Equal(t, "3", verifyResult.Rows[0])
}

func TestInsertManyEmpty(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_insert_many_empty"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertMany([])`)
	require.Error(t, err) // MongoDB doesn't allow empty array
}

func TestUpdateOneBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_update_one"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	// Insert test data
	_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
	require.NoError(t, err)

	// Update
	result, err := gc.Execute(ctx, dbName, `db.users.updateOne({ name: "alice" }, { $set: { age: 31 } })`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"acknowledged": true`)
	require.Contains(t, result.Rows[0], `"matchedCount": 1`)
	require.Contains(t, result.Rows[0], `"modifiedCount": 1`)

	// Verify
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "alice" })`)
	require.NoError(t, err)
	require.Contains(t, verifyResult.Rows[0], `"age": 31`)
}

func TestUpdateOneNoMatch(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_update_one_no_match"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.updateOne({ name: "nobody" }, { $set: { age: 99 } })`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"matchedCount": 0`)
	require.Contains(t, result.Rows[0], `"modifiedCount": 0`)
}

func TestUpdateOneUpsert(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_update_one_upsert"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.updateOne(
		{ name: "newuser" },
		{ $set: { age: 25 } },
		{ upsert: true }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"upsertedId"`)

	// Verify upserted document
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "newuser" })`)
	require.NoError(t, err)
	require.Equal(t, 1, verifyResult.RowCount)
}

func TestUpdateManyBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_update_many"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

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
	require.Contains(t, result.Rows[0], `"matchedCount": 2`)
	require.Contains(t, result.Rows[0], `"modifiedCount": 2`)
}

func TestUpdateManyNoMatch(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_update_many_no_match"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.updateMany({ status: "nonexistent" }, { $set: { verified: true } })`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"matchedCount": 0`)
	require.Contains(t, result.Rows[0], `"modifiedCount": 0`)
}

func TestUpdateManyUpsert(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_update_many_upsert"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.updateMany(
		{ status: "pending" },
		{ $set: { verified: false } },
		{ upsert: true }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"upsertedId"`)

	// Verify upserted document
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ status: "pending" })`)
	require.NoError(t, err)
	require.Equal(t, 1, verifyResult.RowCount)
}

func TestReplaceOneBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_replace_one"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	// Insert test data
	_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30, city: "NYC" })`)
	require.NoError(t, err)

	// Replace entire document
	result, err := gc.Execute(ctx, dbName, `db.users.replaceOne(
		{ name: "alice" },
		{ name: "alice", age: 31, country: "USA" }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"matchedCount": 1`)
	require.Contains(t, result.Rows[0], `"modifiedCount": 1`)

	// Verify - city should be gone, country should exist
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "alice" })`)
	require.NoError(t, err)
	require.Contains(t, verifyResult.Rows[0], `"country": "USA"`)
	require.NotContains(t, verifyResult.Rows[0], `"city"`)
}

func TestReplaceOneUpsert(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_replace_one_upsert"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.replaceOne(
		{ name: "newuser" },
		{ name: "newuser", age: 25 },
		{ upsert: true }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"upsertedId"`)
}

func TestDeleteOneBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_delete_one"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

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
	require.Contains(t, result.Rows[0], `"acknowledged": true`)
	require.Contains(t, result.Rows[0], `"deletedCount": 1`)

	// Verify
	countResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
	require.NoError(t, err)
	require.Equal(t, "2", countResult.Rows[0])
}

func TestDeleteOneNoMatch(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_delete_one_no_match"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.deleteOne({ name: "nobody" })`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"deletedCount": 0`)
}

func TestDeleteManyBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_delete_many"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

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
	require.Contains(t, result.Rows[0], `"deletedCount": 2`)

	// Verify only carol remains
	countResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
	require.NoError(t, err)
	require.Equal(t, "1", countResult.Rows[0])
}

func TestDeleteManyAll(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_delete_many_all"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	// Insert test data
	_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice" },
		{ name: "bob" }
	])`)
	require.NoError(t, err)

	// Delete all with empty filter
	result, err := gc.Execute(ctx, dbName, `db.users.deleteMany({})`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"deletedCount": 2`)
}

func TestFindOneAndUpdateBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_update"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
	require.NoError(t, err)

	// Returns document BEFORE update by default
	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndUpdate(
		{ name: "alice" },
		{ $set: { age: 31 } }
	)`)
	require.NoError(t, err)
	require.Equal(t, 1, result.RowCount)
	require.Contains(t, result.Rows[0], `"age": 30`)
}

func TestFindOneAndUpdateReturnAfter(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_update_after"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
	require.NoError(t, err)

	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndUpdate(
		{ name: "alice" },
		{ $set: { age: 31 } },
		{ returnDocument: "after" }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"age": 31`)
}

func TestFindOneAndUpdateNoMatch(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_update_no_match"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndUpdate(
		{ name: "nobody" },
		{ $set: { age: 99 } }
	)`)
	require.NoError(t, err)
	require.Equal(t, "null", result.Rows[0])
}

func TestFindOneAndReplaceBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_replace"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30, city: "NYC" })`)
	require.NoError(t, err)

	// Returns document BEFORE replacement
	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndReplace(
		{ name: "alice" },
		{ name: "alice", age: 31, country: "USA" }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"city": "NYC"`)
}

func TestFindOneAndReplaceReturnAfter(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_replace_after"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertOne({ name: "alice", age: 30 })`)
	require.NoError(t, err)

	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndReplace(
		{ name: "alice" },
		{ name: "alice", age: 31 },
		{ returnDocument: "after" }
	)`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"age": 31`)
}

func TestFindOneAndDeleteBasic(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_delete"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.insertMany([
		{ name: "alice", age: 30 },
		{ name: "bob", age: 25 }
	])`)
	require.NoError(t, err)

	// Returns the deleted document
	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndDelete({ name: "alice" })`)
	require.NoError(t, err)
	require.Contains(t, result.Rows[0], `"alice"`)
	require.Contains(t, result.Rows[0], `"age": 30`)

	// Verify alice is deleted
	countResult, err := gc.Execute(ctx, dbName, `db.users.countDocuments()`)
	require.NoError(t, err)
	require.Equal(t, "1", countResult.Rows[0])
}

func TestFindOneAndDeleteNoMatch(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_delete_no_match"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

	result, err := gc.Execute(ctx, dbName, `db.users.findOneAndDelete({ name: "nobody" })`)
	require.NoError(t, err)
	require.Equal(t, "null", result.Rows[0])
}

func TestFindOneAndDeleteWithSort(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_find_one_and_delete_sort"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()
	gc := gomongo.NewClient(client)

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
	require.Contains(t, result.Rows[0], `"score": 10`)

	// Verify only score=20 remains
	verifyResult, err := gc.Execute(ctx, dbName, `db.users.findOne({ name: "alice" })`)
	require.NoError(t, err)
	require.Contains(t, verifyResult.Rows[0], `"score": 20`)
}
