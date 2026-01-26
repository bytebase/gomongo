package gomongo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParseError(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_parse_error_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		_, err := gc.Execute(ctx, dbName, "db.users.find({ name: })")
		require.Error(t, err)

		var parseErr *gomongo.ParseError
		require.ErrorAs(t, err, &parseErr)
	})
}

func TestPlannedOperation(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_planned_op_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// createIndexes is a planned M3 operation - should return PlannedOperationError
		// (createIndex is now implemented, so we use createIndexes instead)
		_, err := gc.Execute(ctx, dbName, "db.users.createIndexes([{ key: { name: 1 } }])")
		require.Error(t, err)

		var plannedErr *gomongo.PlannedOperationError
		require.ErrorAs(t, err, &plannedErr)
		require.Equal(t, "createIndexes()", plannedErr.Operation)
	})
}

func TestUnsupportedOperation(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_unsup_op_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// createSearchIndex is NOT in the registry - should return UnsupportedOperationError
		_, err := gc.Execute(ctx, dbName, `db.movies.createSearchIndex({ name: "default", definition: { mappings: { dynamic: true } } })`)
		require.Error(t, err)

		var unsupportedErr *gomongo.UnsupportedOperationError
		require.ErrorAs(t, err, &unsupportedErr)
		require.Equal(t, "createSearchIndex()", unsupportedErr.Operation)
	})
}

func TestUnsupportedOptionError(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_unsup_opt_err_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		gc := gomongo.NewClient(db.Client)

		// find() with unsupported option 'collation'
		_, err := gc.Execute(ctx, dbName, `db.users.find({}, {}, { collation: { locale: "en" } })`)
		var optErr *gomongo.UnsupportedOptionError
		require.ErrorAs(t, err, &optErr)
		require.Equal(t, "find()", optErr.Method)
		require.Equal(t, "collation", optErr.Option)
	})
}

func TestMethodRegistryStats(t *testing.T) {
	total := gomongo.MethodRegistryStats()

	// Registry should contain 15 planned methods after M3 high-ROI implementations
	// M3 high-ROI methods implemented (removed from registry):
	//   - createIndex, dropIndex, dropIndexes (index management: 3)
	//   - drop, createCollection, dropDatabase, renameCollection (collection management: 4)
	// M3 remaining planned methods: 15 (originally 22)
	require.Equal(t, 15, total, "expected 15 planned methods in registry (M3 remaining)")

	// Log stats for visibility
	t.Logf("Method Registry Stats: total=%d planned methods", total)
}

func TestFindOneUnsupportedOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_findone_unsup_opt_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.findOne({}, {}, { collation: { locale: "en" } })`)
		var optErr *gomongo.UnsupportedOptionError
		require.ErrorAs(t, err, &optErr)
		require.Equal(t, "findOne()", optErr.Method)
		require.Equal(t, "collation", optErr.Option)
	})
}

func TestAggregateUnsupportedOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_agg_unsup_opt_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.aggregate([], { allowDiskUse: true })`)
		var optErr *gomongo.UnsupportedOptionError
		require.ErrorAs(t, err, &optErr)
		require.Equal(t, "aggregate()", optErr.Method)
		require.Equal(t, "allowDiskUse", optErr.Option)
	})
}

func TestCountDocumentsUnsupportedOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_count_unsup_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.countDocuments({}, { collation: { locale: "en" } })`)
		var optErr *gomongo.UnsupportedOptionError
		require.ErrorAs(t, err, &optErr)
		require.Equal(t, "countDocuments()", optErr.Method)
	})
}

func TestDistinctUnsupportedOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_distinct_unsup_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.distinct("city", {}, { collation: { locale: "en" } })`)
		var optErr *gomongo.UnsupportedOptionError
		require.ErrorAs(t, err, &optErr)
		require.Equal(t, "distinct()", optErr.Method)
	})
}

func TestEstimatedDocumentCountUnsupportedOption(t *testing.T) {
	testutil.RunOnAllDBs(t, func(t *testing.T, db testutil.TestDB) {
		dbName := fmt.Sprintf("testdb_est_count_unsup_%s", db.Name)
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		ctx := context.Background()

		gc := gomongo.NewClient(db.Client)

		_, err := gc.Execute(ctx, dbName, `db.users.estimatedDocumentCount({ comment: "test" })`)
		var optErr *gomongo.UnsupportedOptionError
		require.ErrorAs(t, err, &optErr)
		require.Equal(t, "estimatedDocumentCount()", optErr.Method)
		require.Equal(t, "comment", optErr.Option)
	})
}
