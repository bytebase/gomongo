# gomongo

Go library for parsing and executing MongoDB shell syntax using the native MongoDB driver.

## Overview

gomongo parses MongoDB shell commands (e.g., `db.users.find()`) and executes them using the Go MongoDB driver, eliminating the need for external mongosh CLI.

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
    "log"

    "github.com/bytebase/gomongo"
    "go.mongodb.org/mongo-driver/v2/mongo"
    "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
    ctx := context.Background()

    // Connect to MongoDB
    client, err := mongo.Connect(options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        log.Fatal(err)
    }
    defer client.Disconnect(ctx)

    // Create gomongo client
    gc := gomongo.NewClient(client)

    // Execute MongoDB shell commands
    result, err := gc.Execute(ctx, "mydb", `db.users.find({ age: { $gt: 25 } })`)
    if err != nil {
        log.Fatal(err)
    }

    // Print results (Extended JSON format)
    for _, row := range result.Rows {
        fmt.Println(row)
    }
}
```

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

## Command Reference

### Milestone 1: Read Operations + Utility + Aggregation (Current)

#### Utility Commands

| Command | Syntax | Status |
|---------|--------|--------|
| show dbs | `show dbs` | Supported |
| show databases | `show databases` | Supported |
| show collections | `show collections` | Supported |
| db.getCollectionNames() | `db.getCollectionNames()` | Supported |
| db.getCollectionInfos() | `db.getCollectionInfos()` | Supported |

#### Read Commands

| Command | Syntax | Status | Notes |
|---------|--------|--------|-------|
| db.collection.find() | `find(query, projection)` | Supported | options deferred |
| db.collection.findOne() | `findOne(query, projection)` | Supported | |
| db.collection.countDocuments() | `countDocuments(filter)` | Supported | options deferred |
| db.collection.estimatedDocumentCount() | `estimatedDocumentCount()` | Supported | options deferred |
| db.collection.distinct() | `distinct(field, query)` | Supported | options deferred |
| db.collection.getIndexes() | `getIndexes()` | Supported | |

#### Cursor Modifiers

| Method | Syntax | Status |
|--------|--------|--------|
| cursor.limit() | `limit(number)` | Supported |
| cursor.skip() | `skip(number)` | Supported |
| cursor.sort() | `sort(document)` | Supported |
| cursor.count() | `count()` | Deprecated - use countDocuments() |

#### Aggregation

| Command | Syntax | Status | Notes |
|---------|--------|--------|-------|
| db.collection.aggregate() | `aggregate(pipeline)` | Supported | options deferred |

#### Object Constructors

| Constructor | Supported Syntax | Unsupported Syntax |
|-------------|------------------|-------------------|
| ObjectId() | `ObjectId()`, `ObjectId("hex")` | `new ObjectId()` |
| ISODate() | `ISODate()`, `ISODate("string")` | `new ISODate()` |
| Date() | `Date()`, `Date("string")`, `Date(timestamp)` | `new Date()` |
| UUID() | `UUID("hex")` | `new UUID()` |
| NumberInt() | `NumberInt(value)` | `new NumberInt()` |
| NumberLong() | `NumberLong(value)` | `new NumberLong()` |
| NumberDecimal() | `NumberDecimal("value")` | `new NumberDecimal()` |
| Timestamp() | `Timestamp(t, i)` | `new Timestamp()` |
| BinData() | `BinData(subtype, base64)` | |
| RegExp() | `RegExp("pattern", "flags")`, `/pattern/flags` | |

### Milestone 2: Write Operations (Planned)

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.insertOne() | `insertOne(document)` | Not yet supported |
| db.collection.insertMany() | `insertMany(documents)` | Not yet supported |
| db.collection.updateOne() | `updateOne(filter, update)` | Not yet supported |
| db.collection.updateMany() | `updateMany(filter, update)` | Not yet supported |
| db.collection.deleteOne() | `deleteOne(filter)` | Not yet supported |
| db.collection.deleteMany() | `deleteMany(filter)` | Not yet supported |
| db.collection.replaceOne() | `replaceOne(filter, replacement)` | Not yet supported |
| db.collection.findOneAndUpdate() | `findOneAndUpdate(filter, update)` | Not yet supported |
| db.collection.findOneAndReplace() | `findOneAndReplace(filter, replacement)` | Not yet supported |
| db.collection.findOneAndDelete() | `findOneAndDelete(filter)` | Not yet supported |

### Milestone 3: Administrative Operations (Planned)

#### Index Management

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.createIndex() | `createIndex(keys)` | Not yet supported |
| db.collection.createIndexes() | `createIndexes(indexSpecs)` | Not yet supported |
| db.collection.dropIndex() | `dropIndex(index)` | Not yet supported |
| db.collection.dropIndexes() | `dropIndexes()` | Not yet supported |

#### Collection Management

| Command | Syntax | Status |
|---------|--------|--------|
| db.createCollection() | `db.createCollection(name)` | Not yet supported |
| db.collection.drop() | `drop()` | Not yet supported |
| db.collection.renameCollection() | `renameCollection(newName)` | Not yet supported |
| db.dropDatabase() | `db.dropDatabase()` | Not yet supported |

#### Database Information

| Command | Syntax | Status |
|---------|--------|--------|
| db.stats() | `db.stats()` | Not yet supported |
| db.collection.stats() | `stats()` | Not yet supported |
| db.serverStatus() | `db.serverStatus()` | Not yet supported |
| db.serverBuildInfo() | `db.serverBuildInfo()` | Not yet supported |
| db.version() | `db.version()` | Not yet supported |
| db.hostInfo() | `db.hostInfo()` | Not yet supported |
| db.listCommands() | `db.listCommands()` | Not yet supported |

### Not Planned

The following categories are recognized but not planned for support:

| Category | Reason |
|----------|--------|
| Database switching (`use <db>`, `db.getSiblingDB()`) | Database is set at connection time |
| Interactive cursor methods (`hasNext()`, `next()`, `toArray()`) | Not an interactive shell |
| JavaScript execution (`forEach()`, `map()`) | No JavaScript engine |
| Replication (`rs.*`) | Cluster administration |
| Sharding (`sh.*`) | Cluster administration |
| User/Role management | Security administration |
| Client-side encryption | Security feature |
| Atlas Stream Processing (`sp.*`) | Atlas-specific |
| Native shell functions (`cat()`, `load()`, `quit()`) | Shell-specific |

For deprecated methods (e.g., `db.collection.insert()`, `db.collection.update()`), gomongo returns actionable error messages directing users to modern alternatives.

## Design Principles

1. **No database switching** - Database is set at connection time only
2. **Not an interactive shell** - No cursor iteration, REPL-style commands, or stateful operations
3. **Syntax translator, not validator** - Arguments pass directly to the Go driver; the server validates
4. **Single syntax for constructors** - Use `ObjectId()`, not `new ObjectId()`
5. **Clear error messages** - Actionable guidance for unsupported or deprecated syntax
