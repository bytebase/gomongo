package gomongo

// methodStatus represents the support status of a MongoDB method.
type methodStatus int

const (
	statusSupported methodStatus = iota
	statusDeprecated
	statusUnsupported
)

// methodContext distinguishes where a method can be called.
type methodContext int

const (
	contextCollection methodContext = iota
	contextCursor
	contextDatabase
	contextConnection  // Mongo(), connect(), connection methods
	contextReplication // rs.* methods
	contextSharding    // sh.* methods
	contextEncryption  // KeyVault, ClientEncryption methods
	contextBulk        // Bulk operation methods
	contextPlanCache   // PlanCache methods
	contextStream      // sp.* stream processing methods
	contextNative      // Native shell functions like cat(), load(), quit()
)

// methodInfo contains metadata about a MongoDB method.
type methodInfo struct {
	status      methodStatus
	context     methodContext
	alternative string // for deprecated methods: what to use instead
	hint        string // additional guidance for unsupported methods
}

// methodRegistry maps method names to their metadata.
// Key format: "methodName" for unique methods, or "context:methodName" for context-specific methods.
// When looking up, first try context-specific key, then fall back to generic key.
var methodRegistry = map[string]methodInfo{
	// ============================================================
	// DEPRECATED METHODS - These have alternatives users should use
	// ============================================================

	// Collection methods (deprecated)
	"collection:count": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "countDocuments() or estimatedDocumentCount()",
	},
	"collection:insert": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "insertOne() or insertMany()",
	},
	"collection:update": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "updateOne(), updateMany(), or replaceOne()",
	},
	"collection:remove": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "deleteOne() or deleteMany()",
	},
	"collection:save": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "insertOne() or replaceOne() with upsert option",
	},
	"collection:findAndModify": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "findOneAndUpdate(), findOneAndReplace(), or findOneAndDelete()",
	},
	"collection:ensureIndex": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "createIndex()",
	},
	"collection:reIndex": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "drop and recreate indexes, or use the reIndex database command",
	},
	"collection:dropIndex": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "dropIndexes()",
	},
	"collection:copyTo": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "aggregation with $out or $merge stage",
	},
	"collection:group": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "aggregate() with $group stage",
	},

	// Cursor methods (deprecated)
	"cursor:count": {
		status:      statusDeprecated,
		context:     contextCursor,
		alternative: "countDocuments() or estimatedDocumentCount()",
	},
	"cursor:forEach": {
		status:      statusDeprecated,
		context:     contextCursor,
		alternative: "for await...of syntax or toArray()",
	},
	"cursor:snapshot": {
		status:      statusDeprecated,
		context:     contextCursor,
		alternative: "hint() with _id index",
	},
	"cursor:maxScan": {
		status:      statusDeprecated,
		context:     contextCursor,
		alternative: "maxTimeMS()",
	},
	"cursor:addOption": {
		status:      statusDeprecated,
		context:     contextCursor,
		alternative: "specific cursor methods like noCursorTimeout(), tailable(), etc.",
	},

	// Database methods (deprecated)
	"database:addUser": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "createUser()",
	},
	"database:removeUser": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "dropUser()",
	},
	"database:eval": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "aggregation framework or application-side logic",
	},
	"database:copyDatabase": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "mongodump and mongorestore",
	},
	"database:cloneDatabase": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "mongodump and mongorestore",
	},
	"database:cloneCollection": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "aggregation with $out or $merge stage",
	},

	// ============================================================
	// UNSUPPORTED METHODS - Not yet implemented but recognized
	// ============================================================

	// Write operations (unsupported - read-only for now)
	"collection:insertOne": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:insertMany": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:updateOne": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:updateMany": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:deleteOne": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:deleteMany": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:replaceOne": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:findOneAndUpdate": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:findOneAndReplace": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:findOneAndDelete": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},
	"collection:bulkWrite": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "write operations are not supported yet",
	},

	// Index management (unsupported)
	"collection:createIndex": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "index management operations are not supported yet",
	},
	"collection:createIndexes": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "index management operations are not supported yet",
	},
	"collection:dropIndexes": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "index management operations are not supported yet",
	},

	// Collection management (unsupported)
	"collection:drop": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "collection management operations are not supported yet",
	},
	"collection:renameCollection": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "collection management operations are not supported yet",
	},

	// Collection stats (unsupported)
	"collection:stats": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},
	"collection:storageSize": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},
	"collection:totalIndexSize": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},
	"collection:totalSize": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},
	"collection:dataSize": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},
	"collection:isCapped": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},
	"collection:validate": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "validation operations are not supported yet",
	},
	"collection:latencyStats": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "stats operations are not supported yet",
	},

	// Cursor terminal methods (unsupported)
	"cursor:toArray": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "use find() or findOne() directly to get results",
	},
	"cursor:next": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor iteration is not supported; use find() to get all results",
	},
	"cursor:tryNext": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor iteration is not supported; use find() to get all results",
	},
	"cursor:hasNext": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor iteration is not supported; use find() to get all results",
	},
	"cursor:close": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor management is handled automatically",
	},
	"cursor:isClosed": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor management is handled automatically",
	},
	"cursor:isExhausted": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor management is handled automatically",
	},
	"cursor:objsLeftInBatch": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor batch management is not supported",
	},
	"cursor:itcount": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "use countDocuments() instead",
	},
	"cursor:size": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "use countDocuments() instead",
	},
	"cursor:explain": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "explain is not supported yet",
	},
	"cursor:pretty": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "output is already formatted as JSON",
	},
	"cursor:map": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "use aggregation $project stage for transformations",
	},

	// Cursor modifier methods (unsupported)
	"cursor:batchSize": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "batch size configuration is not supported yet",
	},
	"cursor:collation": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "collation is not supported yet",
	},
	"cursor:comment": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "query comments are not supported yet",
	},
	"cursor:hint": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "index hints are not supported yet for find operations",
	},
	"cursor:max": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "index bounds are not supported yet",
	},
	"cursor:min": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "index bounds are not supported yet",
	},
	"cursor:maxTimeMS": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "query timeout is not supported yet",
	},
	"cursor:maxAwaitTimeMS": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "await timeout is not supported yet",
	},
	"cursor:noCursorTimeout": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "cursor timeout configuration is not supported yet",
	},
	"cursor:readConcern": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "read concern is not supported yet",
	},
	"cursor:readPref": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "read preference is not supported yet",
	},
	"cursor:returnKey": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "returnKey is not supported yet",
	},
	"cursor:showRecordId": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "showRecordId is not supported yet",
	},
	"cursor:tailable": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "tailable cursors are not supported yet",
	},
	"cursor:allowDiskUse": {
		status:  statusUnsupported,
		context: contextCursor,
		hint:    "allowDiskUse is not supported yet",
	},

	// Database methods (unsupported)
	"database:createCollection": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "database management operations are not supported yet",
	},
	"database:dropDatabase": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "database management operations are not supported yet",
	},
	"database:stats": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "stats operations are not supported yet",
	},
	"database:serverStatus": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "server status is not supported yet",
	},
	"database:serverBuildInfo": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "server info is not supported yet",
	},
	"database:version": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "version info is not supported yet",
	},
	"database:hostInfo": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "host info is not supported yet",
	},
	"database:listCommands": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "command listing is not supported yet",
	},
	"database:runCommand": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "raw command execution is not supported yet",
	},
	"database:adminCommand": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "admin commands are not supported yet",
	},
	"database:getName": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "database info is not supported yet",
	},
	"database:getMongo": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "connection management is not supported yet",
	},
	"database:getSiblingDB": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "database switching is not supported yet",
	},

	// User management (unsupported)
	"database:createUser": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:dropUser": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:auth": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "authentication is handled at connection level",
	},

	// Atlas Search Index methods (unsupported)
	"collection:createSearchIndex": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "Atlas Search Index operations are not supported yet",
	},
	"collection:createSearchIndexes": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "Atlas Search Index operations are not supported yet",
	},
	"collection:dropSearchIndex": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "Atlas Search Index operations are not supported yet",
	},
	"collection:updateSearchIndex": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "Atlas Search Index operations are not supported yet",
	},
	"collection:getSearchIndexes": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "Atlas Search Index operations are not supported yet",
	},

	// Watch/Change Stream methods (unsupported)
	"collection:watch": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "change stream operations are not supported yet",
	},
	"database:watch": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "change stream operations are not supported yet",
	},

	// Bulk operations (unsupported)
	"collection:initializeOrderedBulkOp": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "bulk operations are not supported yet",
	},
	"collection:initializeUnorderedBulkOp": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "bulk operations are not supported yet",
	},

	// Plan cache methods (unsupported)
	"collection:getPlanCache": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "plan cache operations are not supported yet",
	},

	// Additional collection methods (unsupported)
	"collection:hideIndex": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "index management operations are not supported yet",
	},
	"collection:unhideIndex": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "index management operations are not supported yet",
	},
	"collection:compactStructuredEncryptionData": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "encryption operations are not supported yet",
	},

	// Additional database methods (unsupported)
	"database:currentOp": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "operation monitoring is not supported yet",
	},
	"database:killOp": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "operation management is not supported yet",
	},
	"database:fsyncLock": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "fsync operations are not supported yet",
	},
	"database:fsyncUnlock": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "fsync operations are not supported yet",
	},
	"database:setProfilingLevel": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "profiling operations are not supported yet",
	},
	"database:getProfilingStatus": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "profiling operations are not supported yet",
	},
	"database:setLogLevel": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "log management is not supported yet",
	},
	"database:getLogComponents": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "log management is not supported yet",
	},
	"database:rotateCertificates": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "certificate operations are not supported yet",
	},
	"database:shutdownServer": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "server management is not supported yet",
	},

	// User/Role management (unsupported)
	"database:getUser": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:getUsers": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:updateUser": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:changeUserPassword": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:dropAllUsers": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:grantRolesToUser": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:revokeRolesFromUser": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "user management operations are not supported yet",
	},
	"database:createRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:dropRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:getRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:getRoles": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:updateRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:dropAllRoles": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:grantPrivilegesToRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:revokePrivilegesFromRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:grantRolesToRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},
	"database:revokeRolesFromRole": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "role management operations are not supported yet",
	},

	// ============================================================
	// CONNECTION METHODS (17) - Mongo(), connect(), connection chain
	// ============================================================
	"connection:Mongo": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "connection is handled at client creation time",
	},
	"connection:connect": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "connection is handled at client creation time",
	},
	"connection:getDB": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "database is set at connection time",
	},
	"connection:getDBNames": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "use 'show dbs' instead",
	},
	"connection:getDBs": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "use 'show dbs' instead",
	},
	"connection:getReadPrefMode": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "read preference is set at connection time",
	},
	"connection:getReadPrefTagSet": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "read preference is set at connection time",
	},
	"connection:getURI": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "connection URI is set at client creation time",
	},
	"connection:getWriteConcern": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "write concern is set at connection time",
	},
	"connection:setCausalConsistency": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "session management is not supported",
	},
	"connection:setReadPref": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "read preference is set at connection time",
	},
	"connection:setWriteConcern": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "write concern is set at connection time",
	},
	"connection:startSession": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "session management is not supported",
	},
	"connection:watch": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "change streams are not supported",
	},
	"connection:Session": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "session management is not supported",
	},
	"connection:SessionOptions": {
		status:  statusUnsupported,
		context: contextConnection,
		hint:    "session management is not supported",
	},

	// ============================================================
	// REPLICATION METHODS (15) - rs.* methods
	// ============================================================
	"replication:add": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:addArb": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:conf": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:freeze": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:help": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "help system is not supported",
	},
	"replication:initiate": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:printReplicationInfo": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:printSecondaryReplicationInfo": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:reconfig": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:reconfigForPSASet": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:remove": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:status": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:stepDown": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},
	"replication:syncFrom": {
		status:  statusUnsupported,
		context: contextReplication,
		hint:    "replica set administration is not supported",
	},

	// ============================================================
	// SHARDING METHODS (49) - sh.* methods
	// ============================================================
	"sharding:abortMoveCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:abortReshardCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:abortUnshardCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:addShard": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:addShardTag": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:addShardToZone": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:addTagRange": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:balancerCollectionStatus": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:checkMetadataConsistency": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:commitReshardCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:disableAutoMerger": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:disableAutoSplit": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:disableBalancing": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:disableMigrations": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:enableAutoMerger": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:enableAutoSplit": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:enableBalancing": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:enableMigrations": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:enableSharding": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:getBalancerState": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:getShardedDataDistribution": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:help": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "help system is not supported",
	},
	"sharding:isBalancerRunning": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:isConfigShardEnabled": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:listShards": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:moveChunk": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:moveCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:moveRange": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:removeRangeFromZone": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:removeShardFromZone": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:removeShardTag": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:removeTagRange": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:reshardCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:setBalancerState": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:shardAndDistributeCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:shardCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:splitAt": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:splitFind": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:startAutoMerger": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:startBalancer": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:status": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:stopAutoMerger": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:stopBalancer": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:unshardCollection": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:updateZoneKeyRange": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:waitForBalancer": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:waitForBalancerOff": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},
	"sharding:waitForPingChange": {
		status:  statusUnsupported,
		context: contextSharding,
		hint:    "sharding administration is not supported",
	},

	// ============================================================
	// ENCRYPTION METHODS (18) - KeyVault, ClientEncryption
	// ============================================================
	"encryption:ClientEncryption.createEncryptedCollection": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:ClientEncryption.decrypt": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:ClientEncryption.encrypt": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:ClientEncryption.encryptExpression": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:getClientEncryption": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:getKeyVault": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.addKeyAlternateName": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.addKeyAltName": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.createKey": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.createDataKey": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.deleteKey": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.getKey": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.getKeyByAltName": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.getKeys": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.removeKeyAlternateName": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.removeKeyAltName": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},
	"encryption:KeyVault.rewrapManyDataKey": {
		status:  statusUnsupported,
		context: contextEncryption,
		hint:    "client-side encryption is not supported",
	},

	// ============================================================
	// BULK OPERATION METHODS (22)
	// ============================================================
	"bulk:Bulk": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.execute": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.arrayFilters": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.collation": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.delete": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.deleteOne": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.hint": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.remove": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.removeOne": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.replaceOne": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.update": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.updateOne": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.find.upsert": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.getOperations": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.insert": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.toJSON": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Bulk.toString": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:initializeOrderedBulkOp": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:initializeUnorderedBulkOp": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},
	"bulk:Mongo.bulkWrite": {
		status:  statusUnsupported,
		context: contextBulk,
		hint:    "use individual write commands instead",
	},

	// ============================================================
	// PLAN CACHE METHODS (6)
	// ============================================================
	"plancache:getPlanCache": {
		status:  statusUnsupported,
		context: contextPlanCache,
		hint:    "plan cache operations are not supported",
	},
	"plancache:PlanCache.clear": {
		status:  statusUnsupported,
		context: contextPlanCache,
		hint:    "plan cache operations are not supported",
	},
	"plancache:PlanCache.clearPlansByQuery": {
		status:  statusUnsupported,
		context: contextPlanCache,
		hint:    "plan cache operations are not supported",
	},
	"plancache:PlanCache.help": {
		status:  statusUnsupported,
		context: contextPlanCache,
		hint:    "help system is not supported",
	},
	"plancache:PlanCache.list": {
		status:  statusUnsupported,
		context: contextPlanCache,
		hint:    "plan cache operations are not supported",
	},

	// ============================================================
	// STREAM PROCESSING METHODS (10) - sp.* Atlas Stream Processing
	// ============================================================
	"stream:createStreamProcessor": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:listConnections": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:listStreamProcessors": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:process": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:processor.drop": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:processor.sample": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:processor.start": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:processor.stats": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},
	"stream:processor.stop": {
		status:  statusUnsupported,
		context: contextStream,
		hint:    "Atlas Stream Processing is not supported",
	},

	// ============================================================
	// NATIVE SHELL METHODS (18) - top-level functions
	// ============================================================
	"native:_isWindows": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:_rand": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:cat": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:getHostName": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:getMemInfo": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:hostname": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:isInteractive": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:listFiles": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:load": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:ls": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:md5sumFile": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:mkdir": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:quit": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:removeFile": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "file system operations are not supported",
	},
	"native:sleep": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "shell-specific function is not supported",
	},
	"native:version": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "use db.version() instead",
	},
	"native:passwordPrompt": {
		status:  statusUnsupported,
		context: contextNative,
		hint:    "interactive shell feature is not supported",
	},

	// ============================================================
	// ADDITIONAL DATABASE METHODS from milestone doc
	// ============================================================
	"database:aggregate": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "database-level aggregation is not supported yet",
	},
	"database:commandHelp": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "help system is not supported",
	},
	"database:createView": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "view creation is not supported yet",
	},
	"database:getCollection": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "use db.collectionName syntax directly",
	},
	"database:getReplicationInfo": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "replication information is not supported",
	},
	"database:hello": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "cluster state is not supported",
	},
	"database:help": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "help system is not supported",
	},
	"database:logout": {
		status:      statusDeprecated,
		context:     contextDatabase,
		alternative: "close connection and reconnect",
	},
	"database:printCollectionStats": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "use db.collection.stats() instead",
	},
	"database:printReplicationInfo": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "replication information is not supported",
	},
	"database:printSecondaryReplicationInfo": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "replication information is not supported",
	},
	"database:printShardingStatus": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "sharding information is not supported",
	},
	"database:serverCmdLineOpts": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "server configuration is not supported",
	},
	"database:checkMetadataConsistency": {
		status:  statusUnsupported,
		context: contextDatabase,
		hint:    "cluster administration is not supported",
	},

	// ============================================================
	// ADDITIONAL COLLECTION METHODS from milestone doc
	// ============================================================
	"collection:analyzeShardKey": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "sharding features are not supported",
	},
	"collection:configureQueryAnalyzer": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "sharding features are not supported",
	},
	"collection:explain": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "explain is not supported yet",
	},
	"collection:getShardDistribution": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "sharding features are not supported",
	},
	"collection:getShardVersion": {
		status:  statusUnsupported,
		context: contextCollection,
		hint:    "sharding features are not supported",
	},
	"collection:mapReduce": {
		status:      statusDeprecated,
		context:     contextCollection,
		alternative: "aggregate() with $group and other stages",
	},
}

// MethodRegistryStats returns statistics about the method registry.
func MethodRegistryStats() (total, deprecated, unsupported int) {
	for _, info := range methodRegistry {
		total++
		switch info.status {
		case statusDeprecated:
			deprecated++
		case statusUnsupported:
			unsupported++
		}
	}
	return total, deprecated, unsupported
}

// lookupMethod looks up a method in the registry.
// It first tries the context-specific key, then falls back to a generic lookup.
func lookupMethod(ctx methodContext, methodName string) (methodInfo, bool) {
	var contextPrefix string
	switch ctx {
	case contextCollection:
		contextPrefix = "collection:"
	case contextCursor:
		contextPrefix = "cursor:"
	case contextDatabase:
		contextPrefix = "database:"
	case contextConnection:
		contextPrefix = "connection:"
	case contextReplication:
		contextPrefix = "replication:"
	case contextSharding:
		contextPrefix = "sharding:"
	case contextEncryption:
		contextPrefix = "encryption:"
	case contextBulk:
		contextPrefix = "bulk:"
	case contextPlanCache:
		contextPrefix = "plancache:"
	case contextStream:
		contextPrefix = "stream:"
	case contextNative:
		contextPrefix = "native:"
	}

	// Try context-specific lookup first
	if info, ok := methodRegistry[contextPrefix+methodName]; ok {
		return info, true
	}

	// Fall back to generic lookup (for methods that are the same across contexts)
	if info, ok := methodRegistry[methodName]; ok {
		return info, true
	}

	return methodInfo{}, false
}
