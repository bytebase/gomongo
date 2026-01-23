package gomongo_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/bytebase/gomongo"
	"github.com/bytebase/gomongo/internal/testutil"
)

func FuzzFindFilter(f *testing.F) {
	// Seed corpus with valid filters
	f.Add(`{}`)
	f.Add(`{"name": "test"}`)
	f.Add(`{"age": 25}`)
	f.Add(`{"age": {"$gt": 10}}`)
	f.Add(`{"$and": [{"a": 1}, {"b": 2}]}`)
	f.Add(`{"name": {"$regex": "^test"}}`)

	f.Fuzz(func(t *testing.T, filter string) {
		// Get first available client (don't iterate all for fuzz)
		dbs := testutil.GetAllClients(t)
		if len(dbs) == 0 {
			t.Skip("no database available")
		}
		db := dbs[0]

		dbName := "fuzz_test"
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		// Should not panic - errors are OK
		query := fmt.Sprintf(`db.test.find(%s)`, filter)
		_, _ = gc.Execute(ctx, dbName, query)
	})
}

func FuzzInsertDocument(f *testing.F) {
	// Seed corpus with valid documents
	f.Add(`{"name": "test"}`)
	f.Add(`{"a": 1, "b": "two", "c": true}`)
	f.Add(`{"nested": {"deep": {"value": 1}}}`)
	f.Add(`{"arr": [1, 2, 3]}`)

	f.Fuzz(func(t *testing.T, doc string) {
		dbs := testutil.GetAllClients(t)
		if len(dbs) == 0 {
			t.Skip("no database available")
		}
		db := dbs[0]

		dbName := "fuzz_insert"
		defer testutil.CleanupDatabase(t, db.Client, dbName)

		gc := gomongo.NewClient(db.Client)
		ctx := context.Background()

		query := fmt.Sprintf(`db.test.insertOne(%s)`, doc)
		_, _ = gc.Execute(ctx, dbName, query)
	})
}
