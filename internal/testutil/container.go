package testutil

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

var (
	container     *mongodb.MongoDBContainer
	client        *mongo.Client
	containerOnce sync.Once
	containerErr  error
)

// GetClient returns a shared MongoDB client for testing.
// The container is started once and reused across all tests.
// Each test should use a unique database name to avoid interference.
func GetClient(t *testing.T) *mongo.Client {
	t.Helper()

	containerOnce.Do(func() {
		ctx := context.Background()

		container, containerErr = mongodb.Run(ctx, "mongo:7")
		if containerErr != nil {
			return
		}

		connectionString, err := container.ConnectionString(ctx)
		if err != nil {
			containerErr = err
			return
		}

		client, containerErr = mongo.Connect(options.Client().ApplyURI(connectionString))
	})

	if containerErr != nil {
		t.Fatalf("failed to setup test container: %v", containerErr)
	}

	return client
}

// CleanupDatabase drops the specified database after a test.
func CleanupDatabase(t *testing.T, client *mongo.Client, dbName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Database(dbName).Drop(ctx); err != nil {
		t.Logf("warning: failed to drop database %s: %v", dbName, err)
	}
}
