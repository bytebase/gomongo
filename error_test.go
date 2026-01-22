package gomongo_test

import (
	"context"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestParseError(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_parse_error"
	defer testutil.CleanupDatabase(t, client, dbName)

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	_, err := gc.Execute(ctx, dbName, "db.users.find({ name: })")
	require.Error(t, err)

	var parseErr *gomongo.ParseError
	require.ErrorAs(t, err, &parseErr)
}

func TestPlannedOperation(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_planned_op"
	defer testutil.CleanupDatabase(t, client, dbName)

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	// createIndex is a planned M3 operation - should return PlannedOperationError
	_, err := gc.Execute(ctx, dbName, "db.users.createIndex({ name: 1 })")
	require.Error(t, err)

	var plannedErr *gomongo.PlannedOperationError
	require.ErrorAs(t, err, &plannedErr)
	require.Equal(t, "createIndex()", plannedErr.Operation)
}

func TestUnsupportedOperation(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_unsup_op"
	defer testutil.CleanupDatabase(t, client, dbName)

	gc := gomongo.NewClient(client)
	ctx := context.Background()

	// createSearchIndex is NOT in the registry - should return UnsupportedOperationError
	_, err := gc.Execute(ctx, dbName, `db.movies.createSearchIndex({ name: "default", definition: { mappings: { dynamic: true } } })`)
	require.Error(t, err)

	var unsupportedErr *gomongo.UnsupportedOperationError
	require.ErrorAs(t, err, &unsupportedErr)
	require.Equal(t, "createSearchIndex()", unsupportedErr.Operation)
}

func TestUnsupportedOptionError(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_unsup_opt_err"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	// find() with unsupported option 'collation'
	_, err := gc.Execute(ctx, dbName, `db.users.find({}, {}, { collation: { locale: "en" } })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "find()", optErr.Method)
	require.Equal(t, "collation", optErr.Option)
}

func TestMethodRegistryStats(t *testing.T) {
	total := gomongo.MethodRegistryStats()

	// Registry should contain M3 (22) planned methods
	// M2 write operations have been implemented and removed from the registry
	require.Equal(t, 22, total, "expected 22 planned methods in registry (M3: 22)")

	// Log stats for visibility
	t.Logf("Method Registry Stats: total=%d planned methods", total)
}

func TestFindOneUnsupportedOption(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_findone_unsup_opt"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.findOne({}, {}, { collation: { locale: "en" } })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "findOne()", optErr.Method)
	require.Equal(t, "collation", optErr.Option)
}

func TestAggregateUnsupportedOption(t *testing.T) {
	client := testutil.GetClient(t)
	dbName := "testdb_agg_unsup_opt"
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.aggregate([], { allowDiskUse: true })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "aggregate()", optErr.Method)
	require.Equal(t, "allowDiskUse", optErr.Option)
}

func TestCountDocumentsUnsupportedOption(t *testing.T) {
	dbName := "testdb_count_unsup"
	client := testutil.GetClient(t)
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.countDocuments({}, { collation: { locale: "en" } })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "countDocuments()", optErr.Method)
}

func TestDistinctUnsupportedOption(t *testing.T) {
	dbName := "testdb_distinct_unsup"
	client := testutil.GetClient(t)
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.distinct("city", {}, { collation: { locale: "en" } })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "distinct()", optErr.Method)
}

func TestEstimatedDocumentCountUnsupportedOption(t *testing.T) {
	dbName := "testdb_est_count_unsup"
	client := testutil.GetClient(t)
	defer testutil.CleanupDatabase(t, client, dbName)

	ctx := context.Background()

	gc := gomongo.NewClient(client)

	_, err := gc.Execute(ctx, dbName, `db.users.estimatedDocumentCount({ comment: "test" })`)
	var optErr *gomongo.UnsupportedOptionError
	require.ErrorAs(t, err, &optErr)
	require.Equal(t, "estimatedDocumentCount()", optErr.Method)
	require.Equal(t, "comment", optErr.Option)
}
