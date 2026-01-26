package translator

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
// Note: Methods that are now implemented have been removed from this registry.
var methodRegistry = map[string]methodInfo{
	// ============================================================
	// MILESTONE 3: Administrative Operations (remaining planned)
	// ============================================================

	// Index Management (1 remaining - createIndexes has lower ROI)
	"collection:createIndexes": {status: statusPlanned},

	// Database Information (7) - lower ROI, keep as planned
	"database:stats":           {status: statusPlanned},
	"collection:stats":         {status: statusPlanned},
	"database:serverStatus":    {status: statusPlanned},
	"database:serverBuildInfo": {status: statusPlanned},
	"database:version":         {status: statusPlanned},
	"database:hostInfo":        {status: statusPlanned},
	"database:listCommands":    {status: statusPlanned},

	// Collection Information (7) - lower ROI, keep as planned
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
