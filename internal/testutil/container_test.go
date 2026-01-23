package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/stretchr/testify/require"
)

func TestMultiContainer(t *testing.T) {
	dbs := GetAllClients(t)
	require.Equal(t, 3, len(dbs)) // All three databases must be available: documentdb, mongo4, mongo8
	for _, db := range dbs {
		require.NotEmpty(t, db.Name)
		require.NotNil(t, db.Client)
	}
}

func TestRunOnAllDBsHelper(t *testing.T) {
	RunOnAllDBs(t, func(t *testing.T, db TestDB) {
		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()
		dbName := fmt.Sprintf("test_%s_helper", db.Name)
		defer CleanupDatabase(t, db.Client, dbName)

		result, err := gc.Execute(ctx, dbName, "db.test.find()")
		require.NoError(t, err)
		require.Equal(t, 0, result.RowCount)
	})
}

func TestLoadFixture(t *testing.T) {
	docs, err := LoadFixture("users.json")
	require.NoError(t, err)
	require.Len(t, docs, 5)
	require.Equal(t, "user1", docs[0]["_id"])
}
