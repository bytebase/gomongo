package testutil

import (
	"context"
	"crypto/tls"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mongodb"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// TestDB represents a test database connection.
type TestDB struct {
	Name   string
	Client *mongo.Client
}

var (
	testDBs   []TestDB
	setupOnce sync.Once
	setupErr  error
)

// GetAllClients returns all test database clients.
// Containers are started once and reused across all tests.
func GetAllClients(t *testing.T) []TestDB {
	t.Helper()

	setupOnce.Do(func() {
		ctx := context.Background()
		testDBs, setupErr = setupContainers(ctx)
	})

	if setupErr != nil {
		t.Fatalf("failed to setup test containers: %v", setupErr)
	}

	return testDBs
}

// GetClient returns the first available MongoDB client (for backward compatibility).
func GetClient(t *testing.T) *mongo.Client {
	t.Helper()
	dbs := GetAllClients(t)
	if len(dbs) == 0 {
		t.Fatal("no test databases available")
	}
	return dbs[0].Client
}

func setupContainers(ctx context.Context) ([]TestDB, error) {
	var dbs []TestDB
	var mu sync.Mutex
	var wg sync.WaitGroup
	errCh := make(chan error, 3)

	// MongoDB 4.4 (Go driver v2 requires at least MongoDB 4.2)
	wg.Add(1)
	go func() {
		defer wg.Done()
		db, err := setupMongoDB(ctx, "mongo4", "mongo:4.4")
		if err != nil {
			errCh <- fmt.Errorf("mongo4: %w", err)
			return
		}
		mu.Lock()
		dbs = append(dbs, db)
		mu.Unlock()
	}()

	// MongoDB 8.0
	wg.Add(1)
	go func() {
		defer wg.Done()
		db, err := setupMongoDB(ctx, "mongo8", "mongo:8.0")
		if err != nil {
			errCh <- fmt.Errorf("mongo8: %w", err)
			return
		}
		mu.Lock()
		dbs = append(dbs, db)
		mu.Unlock()
	}()

	// DocumentDB
	wg.Add(1)
	go func() {
		defer wg.Done()
		db, err := setupDocumentDB(ctx)
		if err != nil {
			errCh <- fmt.Errorf("documentdb: %w", err)
			return
		}
		mu.Lock()
		dbs = append(dbs, db)
		mu.Unlock()
	}()

	wg.Wait()
	close(errCh)

	// Collect errors - fail if any container failed
	var errs []error
	for err := range errCh {
		errs = append(errs, err)
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("container setup failed: %v", errs)
	}

	return dbs, nil
}

func setupMongoDB(ctx context.Context, name, image string) (TestDB, error) {
	container, err := mongodb.Run(ctx, image)
	if err != nil {
		return TestDB{}, err
	}

	connStr, err := container.ConnectionString(ctx)
	if err != nil {
		return TestDB{}, err
	}

	client, err := mongo.Connect(options.Client().ApplyURI(connStr))
	if err != nil {
		return TestDB{}, err
	}

	return TestDB{Name: name, Client: client}, nil
}

func setupDocumentDB(ctx context.Context) (TestDB, error) {
	req := testcontainers.ContainerRequest{
		Image:        "ghcr.io/documentdb/documentdb/documentdb-local:latest",
		ExposedPorts: []string{"10260/tcp"},
		Cmd:          []string{"--username", "test", "--password", "testpass"},
		WaitingFor:   wait.ForLog("documentdb").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return TestDB{}, err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return TestDB{}, err
	}

	port, err := container.MappedPort(ctx, "10260")
	if err != nil {
		return TestDB{}, err
	}

	connStr := fmt.Sprintf("mongodb://test:testpass@%s:%s/?tls=true&tlsInsecure=true", host, port.Port())

	tlsConfig := &tls.Config{InsecureSkipVerify: true}
	client, err := mongo.Connect(
		options.Client().
			ApplyURI(connStr).
			SetTLSConfig(tlsConfig),
	)
	if err != nil {
		return TestDB{}, err
	}

	// Verify connection
	pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return TestDB{}, fmt.Errorf("ping failed: %w", err)
	}

	return TestDB{Name: "documentdb", Client: client}, nil
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

// RunOnAllDBs runs a test function against all available databases.
func RunOnAllDBs(t *testing.T, testFn func(t *testing.T, db TestDB)) {
	t.Helper()
	dbs := GetAllClients(t)
	for _, db := range dbs {
		t.Run(db.Name, func(t *testing.T) {
			testFn(t, db)
		})
	}
}
