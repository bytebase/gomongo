package gomongo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Tests for mongosh compatibility fixes (BYT-9080).

func TestPrettyNoOp(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_pretty_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		collection := db.Client.Database(dbName).Collection("users")
		_, err := collection.InsertMany(ctx, []any{
			bson.M{"name": "alice", "age": 30},
			bson.M{"name": "bob", "age": 25},
		})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// pretty() should be a no-op cursor method
		result, err := gc.Execute(ctx, dbName, `db.users.find().pretty()`)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Value))

		// pretty() chained after sort
		result, err = gc.Execute(ctx, dbName, `db.users.find().sort({name: 1}).pretty()`)
		require.NoError(t, err)
		require.Equal(t, 2, len(result.Value))
		rows := valuesToStrings(result.Value)
		require.Contains(t, rows[0], `"alice"`)

		// aggregate().pretty()
		result, err = gc.Execute(ctx, dbName, `db.users.aggregate([{$match: {name: "alice"}}]).pretty()`)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Value))
	})
}

func TestDeprecatedInsert(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_insert_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// insert() with a single document should behave like insertOne()
		result, err := gc.Execute(ctx, dbName, `db.users.insert({name: "alice", age: 30})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the document was inserted
		result, err = gc.Execute(ctx, dbName, `db.users.find({name: "alice"})`)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Value))

		// insert() with an array should behave like insertMany()
		result, err = gc.Execute(ctx, dbName, `db.users.insert([{name: "bob"}, {name: "charlie"}])`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify all documents
		result, err = gc.Execute(ctx, dbName, `db.users.find()`)
		require.NoError(t, err)
		require.Equal(t, 3, len(result.Value))

		// insert() via getCollection
		result, err = gc.Execute(ctx, dbName, `db.getCollection("users").insert({name: "dave"})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		result, err = gc.Execute(ctx, dbName, `db.users.find()`)
		require.NoError(t, err)
		require.Equal(t, 4, len(result.Value))
	})
}

func TestDeprecatedCount(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_count_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		collection := db.Client.Database(dbName).Collection("items")
		_, err := collection.InsertMany(ctx, []any{
			bson.M{"name": "a", "status": "active"},
			bson.M{"name": "b", "status": "active"},
			bson.M{"name": "c", "status": "inactive"},
		})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// count() with no args should return total count
		result, err := gc.Execute(ctx, dbName, `db.items.count()`)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Value))
		require.Equal(t, int64(3), result.Value[0])

		// count({}) with empty filter
		result, err = gc.Execute(ctx, dbName, `db.items.count({})`)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Value))
		require.Equal(t, int64(3), result.Value[0])

		// count() with filter
		result, err = gc.Execute(ctx, dbName, `db.items.count({status: "active"})`)
		require.NoError(t, err)
		require.Equal(t, 1, len(result.Value))
		require.Equal(t, int64(2), result.Value[0])
	})
}

func TestDeprecatedUpdate(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_update_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		collection := db.Client.Database(dbName).Collection("users")
		_, err := collection.InsertMany(ctx, []any{
			bson.M{"name": "alice", "age": 30},
			bson.M{"name": "bob", "age": 25},
		})
		require.NoError(t, err)

		gc := gomongo.NewClient(db.Client)

		// update() defaults to updateOne behavior
		result, err := gc.Execute(ctx, dbName, `db.users.update({name: "alice"}, {$set: {age: 31}})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify the update
		findResult, err := gc.Execute(ctx, dbName, `db.users.findOne({name: "alice"})`)
		require.NoError(t, err)
		require.Equal(t, 1, len(findResult.Value))
		require.Contains(t, valueToJSON(findResult.Value[0]), `31`)

		// update() with multi: true should behave like updateMany
		_, err = collection.InsertOne(ctx, bson.M{"name": "alice", "age": 20})
		require.NoError(t, err)

		result, err = gc.Execute(ctx, dbName, `db.users.update({name: "alice"}, {$set: {age: 99}}, {multi: true})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Verify both alices were updated
		findResult, err = gc.Execute(ctx, dbName, `db.users.find({name: "alice"})`)
		require.NoError(t, err)
		require.Equal(t, 2, len(findResult.Value))
		for _, v := range findResult.Value {
			require.Contains(t, valueToJSON(v), `99`)
		}

		// update() via getCollection
		result, err = gc.Execute(ctx, dbName, `db.getCollection("users").update({name: "bob"}, {$set: {age: 26}})`)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}

func TestInt32StringArg(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_int32str_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Int32("0") should parse the string as int32
		result, err := gc.Execute(ctx, dbName, `db.items.insertOne({val: Int32("123")})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		findResult, err := gc.Execute(ctx, dbName, `db.items.find()`)
		require.NoError(t, err)
		require.Equal(t, 1, len(findResult.Value))
		json := valueToJSON(findResult.Value[0])
		require.Contains(t, json, `"val"`)
		require.Contains(t, json, `123`)

		// NumberInt("456") should also work
		result, err = gc.Execute(ctx, dbName, `db.items.insertOne({val: NumberInt("456")})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// Long("789") already supported in grammar, verify it works end-to-end
		result, err = gc.Execute(ctx, dbName, `db.items.insertOne({val: Long("789")})`)
		require.NoError(t, err)
		require.NotNil(t, result)

		// NumberLong("1774250313") — from real user report
		result, err = gc.Execute(ctx, dbName, `db.items.insertOne({val: NumberLong("1774250313")})`)
		require.NoError(t, err)
		require.NotNil(t, result)
	})
}
