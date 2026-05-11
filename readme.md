# Castor

.NET 10 Worker Service — continuously syncs etcd key-value data into PostgreSQL.

## Quick Start

```bash
dotnet run --project src/Castor
```

## Configuration

All settings in `appsettings.json`:

```json
{
  "Etcd": {
    "Url": "http://localhost:2379",
    "BatchSize": 50,
    "Prefix": "/apisix/",
    "Username": "",
    "Password": ""
  },
  "Database": {
    "Type": "Postgres",
    "ConnectionString": "Host=localhost;Port=5432;Database=etcd_sync;Username=postgres;Password=;Pooling=true;",
    "TablePrefix": "apisix_"
  }
}
```

| Key | Default | Description |
|-----|---------|-------------|
| `Etcd.Url` | — | etcd server address |
| `Etcd.BatchSize` | `50` | Keys per page during full sync |
| `Etcd.Prefix` | (all keys) | Only sync keys with this prefix |
| `Etcd.Username` | — | etcd auth (optional) |
| `Etcd.Password` | — | etcd auth (optional) |
| `Database.Type` | — | DB provider (`Postgres` only) |
| `Database.ConnectionString` | — | Npgsql connection string |
| `Database.TablePrefix` | — | Prefix for DB table names |

## How It Works

1. **First run** — full scan of etcd keyspace, upsert everything into PostgreSQL
2. **Restart** — resumes from last synced revision via watch API, replays missed events
3. **Compaction** — if etcd has compacted away the old revision, falls back to full re-sync
4. **Ongoing** — long-lived watch on etcd prefix, upserts/deletes applied transactionally

State tracked in `{prefix}etcd_sync_state` (single-row checkpoint).

## Docker

```bash
docker build -t castor .
docker run -v ./appsettings.json:/app/appsettings.json castor
```

CI builds and pushes on push/PR to `main` (`.github/workflows/docker-image.yaml`).

## Logging

Serilog with console + file + OpenTelemetry sinks. Configured in `appsettings.json` under `Serilog` section.
