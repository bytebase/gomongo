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
