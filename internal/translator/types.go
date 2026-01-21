package translator

import "go.mongodb.org/mongo-driver/v2/bson"

// OperationType represents the type of MongoDB operation.
type OperationType int

const (
	OpUnknown OperationType = iota
	OpFind
	OpFindOne
	OpAggregate
	OpShowDatabases
	OpShowCollections
	OpGetCollectionNames
	OpGetCollectionInfos
	OpGetIndexes
	OpCountDocuments
	OpEstimatedDocumentCount
	OpDistinct
)

// Operation represents a parsed MongoDB operation.
type Operation struct {
	OpType     OperationType
	Collection string
	Filter     bson.D
	// Read operation options (find, findOne)
	Sort       bson.D
	Limit      *int64
	Skip       *int64
	Projection bson.D
	// Index scan bounds and query options
	Hint      any    // string (index name) or document (index spec)
	Max       bson.D // upper bound for index scan
	Min       bson.D // lower bound for index scan
	MaxTimeMS *int64 // max execution time in milliseconds
	// Aggregation pipeline
	Pipeline bson.A
	// distinct field name
	DistinctField string
	// getCollectionInfos options
	NameOnly              *bool
	AuthorizedCollections *bool
}
