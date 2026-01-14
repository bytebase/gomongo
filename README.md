# gomongo

Go library for parsing and executing MongoDB shell syntax using the native MongoDB driver.

## Overview

gomongo parses MongoDB shell commands (e.g., `db.users.find()`) and executes them using the Go MongoDB driver, eliminating the need for external mongosh CLI.

## Status

**MVP v0.1.0** - Basic functionality implemented:

| Feature | Status |
|---------|--------|
| `find()` with filter | Supported |
| `findOne()` | Not yet supported |
| Cursor modifiers (sort, limit, skip, projection) | Parsed but ignored |
| Helper functions (ObjectId, ISODate, UUID, etc.) | Supported |
| Shell commands (show dbs, show collections) | Not yet supported |
| Collection access (dot, bracket, getCollection) | Supported |

## Installation

```bash
go get github.com/bytebase/gomongo
```

## Usage

```go
package main

import (
    "context"
    "fmt"

    "github.com/bytebase/gomongo"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
    // Connect to MongoDB
    client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        panic(err)
    }
    defer client.Disconnect(context.Background())

    // Create gomongo client
    gc := gomongo.NewClient(client)

    // Execute MongoDB shell command
    ctx := context.Background()
    result, err := gc.Execute(ctx, "mydb", `db.users.find()`)
    if err != nil {
        panic(err)
    }

    // Print results (Extended JSON format)
    for _, row := range result.Rows {
        fmt.Println(row)
    }
}
```

## Supported Operations (MVP)

| Category | Operation | Status |
|----------|-----------|--------|
| **Read** | `find()` | Supported (with filter) |
| | `findOne()` | Not yet supported |
| **Collection Access** | dot notation | Supported (`db.users`) |
| | bracket notation | Supported (`db["user-logs"]`) |
| | getCollection | Supported (`db.getCollection("users")`) |

## Supported Filter Syntax

```javascript
// Simple equality
db.users.find({ name: "alice" })

// Comparison operators
db.users.find({ age: { $gt: 25 } })
db.users.find({ age: { $lte: 30 } })

// Multiple conditions
db.users.find({ active: true, age: { $gte: 18 } })

// Array operators
db.users.find({ tags: { $in: ["admin", "user"] } })
```

## Supported Helper Functions

| Helper | Example | BSON Type |
|--------|---------|-----------|
| `ObjectId()` | `ObjectId("507f1f77bcf86cd799439011")` | ObjectID |
| `ISODate()` | `ISODate("2024-01-01T00:00:00Z")` | DateTime |
| `new Date()` | `new Date("2024-01-01")` | DateTime |
| `UUID()` | `UUID("550e8400-e29b-41d4-a716-446655440000")` | Binary (subtype 4) |
| `Long()` / `NumberLong()` | `Long(123)` | int64 |
| `Int32()` / `NumberInt()` | `Int32(123)` | int32 |
| `Double()` | `Double(1.5)` | float64 |
| `Decimal128()` | `Decimal128("123.45")` | Decimal128 |
| `Timestamp()` | `Timestamp(1627811580, 1)` | Timestamp |
| `/pattern/flags` | `/^test/i` | Regex |
| `RegExp()` | `RegExp("pattern", "i")` | Regex |

## Output Format

Results are returned in Extended JSON (Relaxed) format:

```json
{
  "_id": {"$oid": "507f1f77bcf86cd799439011"},
  "name": "Alice",
  "age": 30,
  "createdAt": {"$date": "2024-01-01T00:00:00Z"}
}
```

## Roadmap

Future versions will add:
- `findOne()` support
- Cursor modifiers (sort, limit, skip, projection)
- Shell commands (show dbs, show collections)

## License

Apache License 2.0
