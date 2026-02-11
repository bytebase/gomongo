package translator

// methodStatus represents the support status of a MongoDB method.
type methodStatus int

const (
	// statusPlanned means the method is planned for implementation.
	// When encountered, the caller should fallback to mongosh.
	statusPlanned methodStatus = iota
)

// methodInfo contains metadata about a MongoDB method.
type methodInfo struct {
	status methodStatus
}

// methodRegistry contains only methods we plan to implement.
// If a method is NOT in this registry, it's unsupported (throw error, no fallback).
// If a method IS in this registry, it's planned (fallback to mongosh).
// Note: All M1-M3 methods have been implemented and removed from this registry.
var methodRegistry = map[string]methodInfo{}

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
