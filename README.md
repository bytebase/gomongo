# gomongo

Go library for parsing and executing MongoDB shell syntax using the native MongoDB driver.

## Overview

gomongo parses MongoDB shell commands (e.g., `db.users.find()`) and executes them using the Go MongoDB driver, eliminating the need for external mongosh CLI.

## Status

**MVP v0.1.0** - Basic functionality implemented:

| Feature | Status |
|---------|--------|
| `find()` without filter | Supported |
| `find()` with filter | Parsed but filter ignored (returns all documents) |
| `findOne()` | Not yet supported |
| Cursor modifiers (sort, limit, skip, projection) | Parsed but ignored |
| Helper functions (ObjectId, ISODate, etc.) | Not yet supported |
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
| **Read** | `find()` | Supported (no filter) |
| | `findOne()` | Not yet supported |
| **Collection Access** | dot notation | Supported (`db.users`) |
| | bracket notation | Supported (`db["user-logs"]`) |
| | getCollection | Supported (`db.getCollection("users")`) |

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
- Filter support for `find()` and `findOne()`
- Cursor modifiers (sort, limit, skip, projection)
- Helper functions (ObjectId, ISODate, UUID, NumberLong, etc.)
- Shell commands (show dbs, show collections)

## License

Apache License 2.0
