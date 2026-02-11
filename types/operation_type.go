// Package types provides shared types for the gomongo library.
package types

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
	// Write Operations
	OpInsertOne
	OpInsertMany
	OpUpdateOne
	OpUpdateMany
	OpReplaceOne
	OpDeleteOne
	OpDeleteMany
	OpFindOneAndUpdate
	OpFindOneAndReplace
	OpFindOneAndDelete
	// Administrative Operations
	OpCreateIndex
	OpCreateIndexes
	OpDropIndex
	OpDropIndexes
	OpDrop
	OpCreateCollection
	OpDropDatabase
	OpRenameCollection
	// Database Information
	OpDbStats
	OpCollectionStats
	OpServerStatus
	OpServerBuildInfo
	OpDbVersion
	OpHostInfo
	OpListCommands
	// Collection Information
	OpDataSize
	OpStorageSize
	OpTotalIndexSize
	OpTotalSize
	OpIsCapped
	OpValidate
	OpLatencyStats
)
