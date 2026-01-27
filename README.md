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
    "go.mongodb.org/mongo-driver/v2/bson"
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

    // Result.Value contains []any - type depends on operation
    // For find(), each element is bson.D
    for _, val := range result.Value {
        doc, ok := val.(bson.D)
        if !ok {
            log.Printf("unexpected type %T\n", val)
            continue
        }
        fmt.Printf("%+v\n", doc)
    }

    // For countDocuments(), single element is int64
    countResult, err := gc.Execute(ctx, "mydb", `db.users.countDocuments({})`)
    if err != nil {
        log.Fatal(err)
    }
    count, ok := countResult.Value[0].(int64)
    if !ok {
        log.Fatalf("unexpected type %T\n", countResult.Value[0])
    }
    fmt.Printf("Count: %d\n", count)
}
```

## Execute Options

The `Execute` method accepts optional configuration:

### WithMaxRows

Limit the maximum number of rows returned by `find()` and `countDocuments()` operations. This is useful to prevent excessive memory usage or network traffic from unbounded queries.

```go
// Cap results at 1000 rows
result, err := gc.Execute(ctx, "mydb", `db.users.find()`, gomongo.WithMaxRows(1000))
```

**Behavior:**
- If the query includes `.limit(N)`, the effective limit is `min(N, maxRows)`
- Query limit 50 + MaxRows 1000 → returns up to 50 rows
- Query limit 5000 + MaxRows 1000 → returns up to 1000 rows
- `aggregate()` operations are not affected (use `$limit` stage instead)

## Output Format

Results are returned as native Go types in `Result.Value` (a `[]any` slice). Use `Result.Operation` to determine the expected type:

| Operation | Value Type |
|-----------|-----------|
| `OpFind`, `OpAggregate`, `OpGetIndexes`, `OpGetCollectionInfos` | Each element is `bson.D` |
| `OpFindOne`, `OpFindOneAnd*` | 0 or 1 element of `bson.D` |
| `OpCountDocuments`, `OpEstimatedDocumentCount` | Single `int64` |
| `OpDistinct` | Elements are the distinct values |
| `OpShowDatabases`, `OpShowCollections`, `OpGetCollectionNames` | Each element is `string` |
| `OpInsert*`, `OpUpdate*`, `OpReplace*`, `OpDelete*` | Single `bson.D` with result |
| `OpCreateIndex` | Single `string` (index name) |
| `OpDropIndex`, `OpDropIndexes`, `OpCreateCollection`, `OpDropDatabase`, `OpRenameCollection` | Single `bson.D` with `{ok: 1}` |
| `OpDrop` | Single `bool` (true) |

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

### Milestone 2: Write Operations (Current)

#### Insert Commands

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.insertOne() | `insertOne(document, options)` | Supported |
| db.collection.insertMany() | `insertMany(documents, options)` | Supported |

#### Update Commands

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.updateOne() | `updateOne(filter, update, options)` | Supported |
| db.collection.updateMany() | `updateMany(filter, update, options)` | Supported |
| db.collection.replaceOne() | `replaceOne(filter, replacement, options)` | Supported |

#### Delete Commands

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.deleteOne() | `deleteOne(filter, options)` | Supported |
| db.collection.deleteMany() | `deleteMany(filter, options)` | Supported |

#### Atomic Find-and-Modify Commands

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.findOneAndUpdate() | `findOneAndUpdate(filter, update, options)` | Supported |
| db.collection.findOneAndReplace() | `findOneAndReplace(filter, replacement, options)` | Supported |
| db.collection.findOneAndDelete() | `findOneAndDelete(filter, options)` | Supported |

#### Write Operation Options

| Option | Applies To | Description |
|--------|-----------|-------------|
| `writeConcern` | All write ops | Write concern settings (`w`, `j`, `wtimeout`*) |
| `bypassDocumentValidation` | Insert, Update, Replace, FindOneAndUpdate/Replace | Skip schema validation |
| `comment` | All write ops | Comment for server logs |
| `ordered` | insertMany | Execute inserts sequentially (default: true) |
| `upsert` | Update, Replace, FindOneAndUpdate/Replace | Insert if no match found |
| `hint` | Update, Replace, Delete, FindOneAnd* | Force index usage |
| `collation` | Update, Replace, Delete, FindOneAnd* | String comparison rules |
| `arrayFilters` | updateOne, updateMany, findOneAndUpdate | Array element filtering |
| `let` | Update, Replace, Delete, FindOneAnd* | Variables for expressions |
| `sort` | updateOne, replaceOne, FindOneAnd* | Document selection order |
| `projection` | FindOneAnd* | Fields to return |
| `returnDocument` | FindOneAndUpdate/Replace | Return "before" or "after" |

*Note: `wtimeout` is parsed but ignored as it's not supported in MongoDB Go driver v2.

### Milestone 3: Administrative Operations

#### Index Management

| Command | Syntax | Status |
|---------|--------|--------|
| db.collection.createIndex() | `createIndex(keys, options)` | Supported |
| db.collection.createIndexes() | `createIndexes(indexSpecs)` | Not yet supported |
| db.collection.dropIndex() | `dropIndex(index)` | Supported |
| db.collection.dropIndexes() | `dropIndexes()` | Supported |

#### Collection Management

| Command | Syntax | Status |
|---------|--------|--------|
| db.createCollection() | `db.createCollection(name, options)` | Supported |
| db.collection.drop() | `drop()` | Supported |
| db.collection.renameCollection() | `renameCollection(newName, dropTarget)` | Supported |
| db.dropDatabase() | `db.dropDatabase()` | Supported |

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
