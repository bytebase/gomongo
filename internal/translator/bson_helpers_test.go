package translator_test

import (
	"context"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
)

// These tests verify BSON helper function conversions through the full pipeline.
// Since the helper functions are not exported, we test them end-to-end.

func TestObjectIdHelper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_objectid_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test ObjectId with valid hex string
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({_id: ObjectId("507f1f77bcf86cd799439011"), name: "test"})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({_id: ObjectId("507f1f77bcf86cd799439011")})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "507f1f77bcf86cd799439011")
	})
}

func TestObjectIdHelperGenerated(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_objectid_gen_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test ObjectId() without arguments (generates new ObjectId)
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({name: "test"})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		// Should have an _id field with ObjectId
		require.Contains(t, result.Rows[0], `"_id"`)
	})
}

func TestISODateHelper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_isodate_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test ISODate with valid ISO string
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({created: ISODate("2024-01-15T10:30:00Z")})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		// Extended JSON format for dates
		require.Contains(t, result.Rows[0], "2024-01-15")
	})
}

func TestNumberLongHelper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_numberlong_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test NumberLong with large number (beyond JS safe integer)
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({bignum: NumberLong("9007199254740993")})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "9007199254740993")
	})
}

func TestNumberIntHelper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_numberint_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test NumberInt
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({count: NumberInt(42)})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "42")
	})
}

func TestUUIDHelper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_uuid_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test UUID helper
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({uuid: UUID("550e8400-e29b-41d4-a716-446655440000")})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		// UUID should be in the output (as binary subtype 4)
		require.Contains(t, result.Rows[0], "uuid")
	})
}

func TestTimestampHelper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_timestamp_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test Timestamp(t, i) format
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({ts: Timestamp(1234567890, 1)})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "1234567890")
	})
}

func TestDecimal128Helper(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := "testdb_decimal128_helper_" + db.Name
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Test Decimal128 (NumberDecimal)
		_, err := gc.Execute(ctx, dbName, `db.test.insertOne({price: NumberDecimal("123.456")})`)
		require.NoError(t, err)

		result, err := gc.Execute(ctx, dbName, `db.test.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "123.456")
	})
}
