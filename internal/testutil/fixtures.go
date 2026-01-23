package testutil

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// LoadFixture loads a JSON fixture file and returns it as a slice of bson.M.
func LoadFixture(filename string) ([]bson.M, error) {
	path := fixturePath(filename)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read fixture %s: %w", filename, err)
	}

	var docs []bson.M
	if err := json.Unmarshal(data, &docs); err != nil {
		return nil, fmt.Errorf("unmarshal fixture %s: %w", filename, err)
	}

	return docs, nil
}

// LoadFixtureAsAny loads a fixture and returns []any for InsertMany.
func LoadFixtureAsAny(filename string) ([]any, error) {
	docs, err := LoadFixture(filename)
	if err != nil {
		return nil, err
	}

	result := make([]any, len(docs))
	for i, doc := range docs {
		result[i] = doc
	}
	return result, nil
}

func fixturePath(filename string) string {
	_, currentFile, _, _ := runtime.Caller(0)
	projectRoot := filepath.Dir(filepath.Dir(filepath.Dir(currentFile)))
	return filepath.Join(projectRoot, "testdata", filename)
}
