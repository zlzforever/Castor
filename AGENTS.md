# PROJECT KNOWLEDGE BASE

**Generated:** 2026-05-11

## OVERVIEW
Castor is a .NET 10 Worker Service that continuously syncs etcd key-value data to PostgreSQL via etcd watch API and full initial scan. Raw SQL via Dapper — no ORM.

## STRUCTURE
```
etcd_to_pg/
├── src/Castor/              # Sole project (Microsoft.NET.Sdk.Worker)
│   ├── Program.cs           # Entry: Host builder, DI, Serilog boot
│   ├── EtcdOptions.cs       # etcd connection POCO (Url, BatchSize, Prefix, auth)
│   ├── DatabaseOptions.cs   # DB POCO (Type, ConnectionString, TablePrefix)
│   ├── DatabaseFactory.cs   # Resolves IRepository by Type string
│   └── Services/
│       ├── SyncBackgroundService.cs  # Core: FullSync → WatchLoop
│       ├── PostgreSQLRepository.cs   # Dapper-backed Postgres impl
│       ├── IRepository.cs            # Storage abstraction
│       └── EtcdKvRecord.cs           # KV row model
├── Dockerfile               # Multi-stage: sdk:10.0 build → runtime:10.0
├── docker-entrypoint.sh     # exec "$@"
└── .github/workflows/       # CI: docker build + push on main push/PR
```

## WHERE TO LOOK
| Task | Location | Notes |
|------|----------|-------|
| Sync orchestration | `Services/SyncBackgroundService.cs:19-41` | FullSyncAsync (initial) → WatchLoopAsync |
| etcd full scan | `Services/SyncBackgroundService.cs:43-99` | RangeRequest with BatchSize pagination |
| etcd watch | `Services/SyncBackgroundService.cs:101-195` | WatchAsync with infinite Task.Delay |
| DB upsert/delete | `Services/PostgreSQLRepository.cs:116-164` | Transactional: upsert + delete + sync state |
| DB schema init | `Services/PostgreSQLRepository.cs:84-107` | CREATE TABLE IF NOT EXISTS + indexes |
| Config binding | `Program.cs:39-40` | IOptions<T> from "Etcd"/"Database" sections |
| DB provider switch | `DatabaseFactory.cs:22-26` | Type string → DI resolve (only "postgres" today) |

## CODE MAP

| Symbol | Type | Location | Role |
|--------|------|----------|------|
| `SyncBackgroundService` | class | Services/ | Hosted service; owns sync lifecycle |
| `PostgreSQLRepository` | class | Services/ | All DB DDL + DML |
| `IRepository` | interface | Services/ | DB abstraction; only 1 impl |
| `DatabaseFactory` | class | . | Provider pattern; switch on Type string |
| `EtcdKvRecord` | class | Services/ | DTO with Key/Value/Version/CreateRev/ModRev |
| `EtcdOptions` | class | . | etcd config; BatchSize defaults to 50 |
| `DatabaseOptions` | class | . | DB config; TablePrefix for multi-tenant |

## CONVENTIONS
- **File-scoped namespaces**: `namespace Castor;` (C# 10+)
- **Primary constructors**: `class Foo(IOptions<T> opts) { }` (C# 12)
- **Raw SQL via Dapper**: No EF Core/ORM. All SQL in constructor with C# raw string literals (`"""`)
- **`IOptions<T>` pattern**: Config sections bound by name, not attributes
- **`BackgroundService` base class**: Async hosted service lifetime
- **Worker SDK, not ASP.NET**: `Microsoft.NET.Sdk.Worker` — no HTTP stack

## ANTI-PATTERNS (THIS PROJECT)
- **No `using` on sync DB**: `UpsertBatch` and `SyncBatch` use `using var` which is fine for connection, but the methods themselves are `void` (sync) — they block the watch callback thread

## UNIQUE STYLES
- Table prefix injected at SQL-string-construction time (constructor)
- Sync state table uses single-row checkpoint (id=1)
- Watch uses `Task.Delay(Timeout.Infinite, ct)` as a "wait forever" pattern
- etcd keyspace traversal uses `\0` byte suffix to get next key after current range

## COMMANDS
```bash
dotnet build src/Castor
dotnet run --project src/Castor
docker build -t castor .
```

## NOTES
- `appsettings.Development.json` contains real credentials (password in plaintext) — excluded from Docker build via `rm -f` in Dockerfile
- No test project
- No linter or code style config (.editorconfig, .golangci.yml, etc.)
- Table schema has Chinese comments
