package translator

import (
	"github.com/bytebase/gomongo/types"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// Operation represents a parsed MongoDB operation.
type Operation struct {
	OpType     types.OperationType
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

	// M2: Write operation fields
	Document       bson.D   // insertOne document
	Documents      []bson.D // insertMany documents
	Update         any      // update document or pipeline (bson.D or bson.A)
	Replacement    bson.D   // replaceOne replacement document
	Upsert         *bool    // upsert option for update/replace operations
	ReturnDocument *string  // "before" or "after" for findOneAnd* operations

	// M2: Additional write operation options
	Ordered                  *bool  // insertMany ordered option
	Collation                bson.D // collation settings for string comparison
	ArrayFilters             bson.A // array element filters for update operations
	Let                      bson.D // variables for aggregation expressions
	BypassDocumentValidation *bool  // bypass schema validation
	Comment                  any    // comment for server logs/profiling
	WriteConcern             bson.D // write concern settings (w, j, wtimeout)

	// M3: Administrative operation fields
	IndexKeys   bson.D   // createIndex key specification
	IndexName   string   // dropIndex index name (or createIndex name option)
	IndexNames  []string // dropIndexes array of index names
	NewName     string   // renameCollection new collection name
	DropTarget  *bool    // renameCollection dropTarget option
	IndexUnique *bool    // createIndex unique option
	IndexSparse *bool    // createIndex sparse option
	IndexTTL    *int32   // createIndex expireAfterSeconds option

	// createCollection options
	Capped           *bool  // createCollection capped option
	CollectionSize   *int64 // createCollection size option (for capped)
	CollectionMax    *int64 // createCollection max option (for capped)
	ValidationLevel  string // createCollection validationLevel option
	ValidationAction string // createCollection validationAction option
	Validator        bson.D // createCollection validator option
}
