package gomongo

// methodStatus represents the support status of a MongoDB method.
type methodStatus int

const (
	// statusPlanned means the method is planned for implementation (M2/M3).
	// When encountered, the caller should fallback to mongosh.
	statusPlanned methodStatus = iota
)

// methodInfo contains metadata about a MongoDB method.
type methodInfo struct {
	status methodStatus
}

// methodRegistry contains only methods we plan to implement (M2, M3).
// If a method is NOT in this registry, it's unsupported (throw error, no fallback).
// If a method IS in this registry, it's planned (fallback to mongosh).
var methodRegistry = map[string]methodInfo{
	// ============================================================
	// MILESTONE 2: Write Operations (10 methods)
	// ============================================================

	// Insert Commands (2)
	"collection:insertOne":  {status: statusPlanned},
	"collection:insertMany": {status: statusPlanned},

	// Update Commands (3)
	"collection:updateOne":  {status: statusPlanned},
	"collection:updateMany": {status: statusPlanned},
	"collection:replaceOne": {status: statusPlanned},

	// Delete Commands (2)
	"collection:deleteOne":  {status: statusPlanned},
	"collection:deleteMany": {status: statusPlanned},

	// Atomic Find-and-Modify Commands (3)
	"collection:findOneAndUpdate":  {status: statusPlanned},
	"collection:findOneAndReplace": {status: statusPlanned},
	"collection:findOneAndDelete":  {status: statusPlanned},

	// ============================================================
	// MILESTONE 3: Administrative Operations (22 methods)
	// ============================================================

	// Index Management (4)
	"collection:createIndex":   {status: statusPlanned},
	"collection:createIndexes": {status: statusPlanned},
	"collection:dropIndex":     {status: statusPlanned},
	"collection:dropIndexes":   {status: statusPlanned},

	// Collection Management (4)
	"database:createCollection":       {status: statusPlanned},
	"collection:drop":                 {status: statusPlanned},
	"collection:renameCollection":     {status: statusPlanned},
	"database:dropDatabase":           {status: statusPlanned},

	// Database Information (7)
	"database:stats":           {status: statusPlanned},
	"collection:stats":         {status: statusPlanned},
	"database:serverStatus":    {status: statusPlanned},
	"database:serverBuildInfo": {status: statusPlanned},
	"database:version":         {status: statusPlanned},
	"database:hostInfo":        {status: statusPlanned},
	"database:listCommands":    {status: statusPlanned},

	// Collection Information (7)
	"collection:dataSize":       {status: statusPlanned},
	"collection:storageSize":    {status: statusPlanned},
	"collection:totalIndexSize": {status: statusPlanned},
	"collection:totalSize":      {status: statusPlanned},
	"collection:isCapped":       {status: statusPlanned},
	"collection:validate":       {status: statusPlanned},
	"collection:latencyStats":   {status: statusPlanned},
}

// IsPlannedMethod checks if a method is in the registry (planned for implementation).
// Returns true if the method should fallback to mongosh.
// Returns false if the method is unsupported (throw error).
func IsPlannedMethod(context, methodName string) bool {
	key := context + ":" + methodName
	_, ok := methodRegistry[key]
	return ok
}

// MethodRegistryStats returns statistics about the method registry.
func MethodRegistryStats() int {
	return len(methodRegistry)
}
