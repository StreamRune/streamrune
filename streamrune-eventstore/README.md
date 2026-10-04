# StreamRune EventStore

Container module for EventStore implementations.

## Modules

| Module | Description |
|---|---|
| [streamrune-postgres](streamrune-postgres/README.md) | PostgreSQL-backed `EventStore` -- JSONB storage, advisory locks, LISTEN/NOTIFY subscriptions |

## Requirements

- Java 25+
- PostgreSQL 17 or newer (tested on 17 and 18; an older server is refused at startup)
