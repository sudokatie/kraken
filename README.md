# Kraken

A distributed SQL database built from scratch in Rust.

## What This Is

Kraken implements database fundamentals from the ground up:
- Page-based storage engine with B-tree indexes
- SQL parser and query planner
- Volcano-style query execution
- Write-ahead logging for durability
- Raft consensus for replication

Educational project to understand how databases actually work.

## Features

- SQL support: SELECT, INSERT, UPDATE, DELETE, CREATE TABLE
- Data types: INTEGER, REAL, TEXT, BOOLEAN
- B-tree indexes for fast lookups
- MVCC for concurrent reads
- Raft replication for high availability
- WAL for crash recovery

## Quick Start

```bash
# Build
cargo build

# Start single-node server
cargo run -- server --data-dir ./data --port 5432

# Connect with client
cargo run -- client --host localhost --port 5432

# Check cluster status
cargo run -- status --host localhost --port 5432
```

## SQL Examples

```sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    email TEXT
);

INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com');

SELECT * FROM users WHERE id = 1;

UPDATE users SET email = 'alice@new.com' WHERE id = 1;

DELETE FROM users WHERE id = 1;
```

## Architecture

```
┌─────────────────────────────────────────┐
│              SQL Parser                 │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│            Query Planner                │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│          Execution Engine               │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│          Transaction Manager            │
│              (WAL + MVCC)               │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│           Storage Engine                │
│         (Pages + B-trees)               │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────▼───────────────────────┐
│          Raft Consensus                 │
│           (Replication)                 │
└─────────────────────────────────────────┘
```

## Requirements

- Rust 1.75+
- Linux, macOS, or Windows

## Project Status

v0.1.0 - In Development

## License

MIT

---

*Built by Katie to understand database internals.*
