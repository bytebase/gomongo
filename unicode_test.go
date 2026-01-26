package gomongo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestUnicodeInsertAndQuery(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_unicode_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert CJK document
		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({"name": "张三", "city": "北京"})`)
		require.NoError(t, err)

		// Query by unicode field value
		result, err := gc.Execute(ctx, dbName, `db.users.findOne({"name": "张三"})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "张三")
		require.Contains(t, result.Rows[0], "北京")
	})
}

func TestUnicodeArabic(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_arabic_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert Arabic document
		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({"name": "محمد", "city": "القاهرة"})`)
		require.NoError(t, err)

		// Query by Arabic field value
		result, err := gc.Execute(ctx, dbName, `db.users.findOne({"name": "محمد"})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "محمد")
	})
}

func TestUnicodeEmoji(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_emoji_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert document with emoji
		_, err := gc.Execute(ctx, dbName, `db.users.insertOne({"name": "Test 🎉", "tags": ["🔥", "✨"]})`)
		require.NoError(t, err)

		// Query and verify emoji preserved
		result, err := gc.Execute(ctx, dbName, `db.users.findOne({})`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
		require.Contains(t, result.Rows[0], "🎉")
		require.Contains(t, result.Rows[0], "🔥")
	})
}

func TestUnicodeInCollectionName(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_unicode_coll_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert into unicode-named collection
		_, err := gc.Execute(ctx, dbName, `db["用户表"].insertOne({"name": "test"})`)
		require.NoError(t, err)

		// Query unicode-named collection
		result, err := gc.Execute(ctx, dbName, `db["用户表"].find()`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
	})
}

func TestUnicodeEmojiInCollectionName(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_emoji_coll_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Insert into emoji-named collection
		_, err := gc.Execute(ctx, dbName, `db["users🎉"].insertOne({"name": "test"})`)
		require.NoError(t, err)

		// Query emoji-named collection
		result, err := gc.Execute(ctx, dbName, `db["users🎉"].find()`)
		require.NoError(t, err)
		require.Equal(t, 1, result.RowCount)
	})
}

func TestUnicodeRoundTrip(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_roundtrip_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()
		gc := gomongo.NewClient(db.Client)

		// Load unicode fixture
		docs, err := testutil.LoadFixtureAsAny("unicode_samples.json")
		require.NoError(t, err)

		// Insert all unicode samples using driver directly
		collection := db.Client.Database(dbName).Collection("samples")
		_, err = collection.InsertMany(ctx, docs)
		require.NoError(t, err)

		// Query each and verify round-trip integrity
		result, err := gc.Execute(ctx, dbName, `db.samples.find()`)
		require.NoError(t, err)
		require.Equal(t, len(docs), result.RowCount)

		// Spot check specific unicode values
		allRows := ""
		for _, row := range result.Rows {
			allRows += row
		}
		require.Contains(t, allRows, "张三")   // Chinese
		require.Contains(t, allRows, "田中太郎") // Japanese
		require.Contains(t, allRows, "김철수")  // Korean
		require.Contains(t, allRows, "محمد") // Arabic
		require.Contains(t, allRows, "🎉")    // Emoji
	})
}
