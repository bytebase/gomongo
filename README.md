# gomongo

Go library for parsing and executing MongoDB shell syntax using the native MongoDB driver.

## Overview

gomongo parses MongoDB shell commands (e.g., `db.users.find({ age: { $gt: 25 } })`) and executes them using the Go MongoDB driver, eliminating the need for external mongosh CLI.

## Features

- Parse MongoDB shell syntax using ANTLR-based parser
- Execute queries via native Go MongoDB driver
- Support for common read operations (`find`, `findOne`)
- Support for cursor modifiers (`.sort()`, `.limit()`, `.skip()`, `.projection()`)
- Support for helper functions (`ObjectId()`, `ISODate()`, `UUID()`, etc.)
- Output results in Extended JSON (Relaxed) format

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
    result, err := gc.Execute(ctx, "mydb", `db.users.find({ age: { $gt: 25 } }).sort({ name: 1 }).limit(10)`)
    if err != nil {
        panic(err)
    }

    // Print results (Extended JSON format)
    for _, row := range result.Rows {
        fmt.Println(row)
    }
}
```

## Supported Operations

| Category | Operation | Example |
|----------|-----------|---------|
| **Read** | `find()` | `db.users.find({ age: { $gt: 25 } })` |
| | `findOne()` | `db.users.findOne({ _id: ObjectId('...') })` |
| **Cursor Modifiers** | `.sort()` | `.sort({ age: -1 })` |
| | `.limit()` | `.limit(10)` |
| | `.skip()` | `.skip(20)` |
| | `.projection()` | `.projection({ name: 1, age: 1 })` |
| **Utility** | `show dbs` | `show dbs` |
| | `show collections` | `show collections` |
| **Collection Access** | dot notation | `db.users` |
| | bracket notation | `db["user-logs"]` |
| | getCollection | `db.getCollection("users")` |

## Helper Functions

| Helper | Syntax | Example |
|--------|--------|---------|
| `ObjectId()` | `ObjectId("hex")` | `ObjectId("507f1f77bcf86cd799439011")` |
| `ISODate()` | `ISODate("iso-string")` | `ISODate("2024-01-01T00:00:00Z")` |
| `UUID()` | `UUID("uuid-string")` | `UUID("550e8400-e29b-41d4-a716-446655440000")` |
| `NumberLong()` | `NumberLong(n)` | `NumberLong(123)` |
| `NumberInt()` | `NumberInt(n)` | `NumberInt(42)` |
| `NumberDecimal()` | `NumberDecimal("n")` | `NumberDecimal("123.45")` |

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

## License

Apache License 2.0
