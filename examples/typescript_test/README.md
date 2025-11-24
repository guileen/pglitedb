# TypeScript Test for pglitedb

This is a TypeScript test suite that connects to pglitedb as if it were a PostgreSQL database.

## Prerequisites

1. Make sure pglitedb server is running on localhost:5432
2. Node.js and npm installed

## Setup

```bash
npm install
```

## Running Tests

First, make sure the pglitedb server is running:

```bash
# In the root directory of pglitedb
go run cmd/server/main.go
```

Then run the TypeScript tests:

```bash
npm test
```

## What the Tests Cover

1. Database connection
2. Table creation
3. Data insertion
4. Data selection with ORDER BY
5. Data selection with WHERE clause
6. Data updates
7. Data deletion
8. Transaction handling

The tests verify that pglitedb correctly implements PostgreSQL-compatible SQL operations.