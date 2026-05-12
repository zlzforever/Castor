using System.Data.Common;
using System.Text.RegularExpressions;
using Dapper;
using Npgsql;
using Microsoft.Extensions.Options;

namespace Castor.Services;

public class PostgreSQLRepository : IRepository
{
    private readonly DatabaseOptions _options;
    private readonly ILogger<PostgreSQLRepository> _logger;
    private readonly string _createEtcdKvTable;
    private readonly string _createSyncStateTable;
    private readonly string _ensureSyncStateRow;
    private readonly string _updateSyncStateSql;
    private readonly string _upsertSql;
    private readonly string _deleteBatchSql;
    private readonly string _queryLastRevisionSql;
    private readonly string _createTempSyncKeysSql;
    private readonly string _insertTempSyncKeySql;
    private readonly string _deleteStaleKeysSql;

    public PostgreSQLRepository(IOptions<DatabaseOptions> options, ILogger<PostgreSQLRepository> logger)
    {
        _options = options.Value;
        var tablePrefix = _options.TablePrefix ?? string.Empty;
        _logger = logger;
        _createEtcdKvTable = $$"""
                               CREATE TABLE IF NOT EXISTS {{tablePrefix}}etcd_kv
                               (
                                   key        VARCHAR(1024) NOT NULL, -- etcd key，最长1024
                                   value      TEXT          NOT NULL,
                                   version    BIGINT        NOT NULL, -- etcd version
                                   create_rev BIGINT        NOT NULL, -- 创建 revision
                                   mod_rev    BIGINT        NOT NULL, -- 修改 revision
                                   updated_at TIMESTAMPTZ   NOT NULL DEFAULT NOW(),
                                   PRIMARY KEY (key)                  -- 保持你的主键
                               );
                               -- 优化1：mod_rev 索引增加 KEY 包含（仅索引扫描，性能爆炸）
                               CREATE INDEX IF NOT EXISTS idx_{{tablePrefix}}etcd_kv_mod_rev ON {{tablePrefix}}etcd_kv (mod_rev) INCLUDE (key);

                               -- 优化2：前缀索引保留，但必须用 varchar_pattern_ops（你之前用错了！）
                               CREATE INDEX IF NOT EXISTS idx_{{tablePrefix}}etcd_kv_key_prefix ON {{tablePrefix}}etcd_kv (key varchar_pattern_ops);

                               -- 优化3：
                               CREATE INDEX IF NOT EXISTS idx_{{tablePrefix}}etcd_kv_updated_at ON {{tablePrefix}}etcd_kv (updated_at);
                               """;
        _createSyncStateTable = $$"""
                                  CREATE TABLE IF NOT EXISTS {{tablePrefix}}etcd_sync_state (
                                      id              INT PRIMARY KEY,
                                      last_revision   BIGINT NOT NULL DEFAULT 0,
                                      last_sync_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
                                  );
                                  """;
        _ensureSyncStateRow = $$"""
                                INSERT INTO {{tablePrefix}}etcd_sync_state (id, last_revision, last_sync_at)
                                VALUES (1, 0, NOW())
                                ON CONFLICT (id) DO NOTHING;
                                """;
        _updateSyncStateSql = $$"""
                                UPDATE {{tablePrefix}}etcd_sync_state
                                SET last_revision = @LastRevision, last_sync_at  = NOW()
                                WHERE id = 1;
                                """;
        _upsertSql = $$"""
                       INSERT INTO {{tablePrefix}}etcd_kv (key, value, version, create_rev, mod_rev, updated_at)
                       VALUES (@Key, @Value, @Version, @CreateRevision, @ModRevision, NOW())
                       ON CONFLICT (key) DO UPDATE SET
                           value         = EXCLUDED.value,
                           version       = EXCLUDED.version,
                           create_rev    = EXCLUDED.create_rev,
                           mod_rev       = EXCLUDED.mod_rev,
                           updated_at    = NOW()
                       WHERE {{tablePrefix}}etcd_kv.mod_rev < EXCLUDED.mod_rev;
                       """;

        _deleteBatchSql = $$"""
                            DELETE FROM {{tablePrefix}}etcd_kv WHERE key = ANY(@Keys);
                            """;
        _queryLastRevisionSql = $$"""
                                  SELECT last_revision FROM {{tablePrefix}}etcd_sync_state WHERE id = 1;
                                  """;

        _createTempSyncKeysSql = """CREATE TEMP TABLE IF NOT EXISTS _sync_keys (key VARCHAR(1024) PRIMARY KEY);""";

        _insertTempSyncKeySql = """
                                    INSERT INTO _sync_keys (key)
                                    SELECT unnest(@Keys)
                                    ON CONFLICT DO NOTHING;
                                """;

        _deleteStaleKeysSql = $$"""
                                    DELETE FROM {{tablePrefix}}etcd_kv AS main
                                    WHERE NOT EXISTS (
                                        SELECT 1 FROM _sync_keys t WHERE t.key = main.key
                                    );
                                """;
    }

    public string HostLabel => ExtractHost(_options.ConnectionString);

    public async Task InitializeAsync(CancellationToken ct = default)
    {
        await using var conn = new NpgsqlConnection(_options.ConnectionString);
        if (conn.State != System.Data.ConnectionState.Open)
        {
            await conn.OpenAsync(ct);
        }

        await using var cmd = conn.CreateCommand();

        cmd.CommandText = _createEtcdKvTable;
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Ensured table: etcd_kv");

        cmd.CommandText = _createSyncStateTable;
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Ensured table: etcd_sync_state");

        cmd.CommandText = _ensureSyncStateRow;
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Ensured sync state checkpoint row");

        _logger.LogInformation("Database initialization complete");
    }

    public async Task<long> GetLastRevisionAsync(CancellationToken ct = default)
    {
        await using var conn = CreateConnection();
        var revision = await conn.QuerySingleOrDefaultAsync<long?>(_queryLastRevisionSql, ct);
        return revision.HasValue ? revision.Value <= 0 ? 0 : revision.Value : 0L;
    }

    public async Task UpsertBatchAsync(List<EtcdKvRecord> records, long lastRevision,
        CancellationToken stoppingToken = default)
    {
        if (records.Count == 0) return;

        await using var conn = CreateConnection();
        if (conn.State != System.Data.ConnectionState.Open)
        {
            await conn.OpenAsync(stoppingToken);
        }

        await using var tx = await conn.BeginTransactionAsync(stoppingToken);
        await conn.ExecuteAsync(_upsertSql, records, transaction: tx);
        await conn.ExecuteAsync(_updateSyncStateSql, new { LastRevision = lastRevision }, transaction: tx);
        await tx.CommitAsync(stoppingToken);

        _logger.LogDebug("Batch upserted {Count} keys", records.Count);
    }

    public async Task SyncBatchAsync(List<EtcdKvRecord> puts, List<string> deletes, long lastRevision,
        CancellationToken stoppingToken = default)
    {
        if (puts.Count == 0 && deletes.Count == 0)
        {
            return;
        }

        await using var conn = CreateConnection();
        if (conn.State != System.Data.ConnectionState.Open)
        {
            await conn.OpenAsync(stoppingToken);
        }

        await using var tx = await conn.BeginTransactionAsync(stoppingToken);

        if (puts.Count > 0)
        {
            await conn.ExecuteAsync(_upsertSql, puts, transaction: tx);
        }

        if (deletes.Count > 0)
        {
            await conn.ExecuteAsync(_deleteBatchSql, new { Keys = deletes }, transaction: tx);
        }

        await conn.ExecuteAsync(_updateSyncStateSql, new { LastRevision = lastRevision }, transaction: tx);
        await tx.CommitAsync(stoppingToken);

        _logger.LogDebug("Applied watch events: {Puts} upserts, {Deletes} deletes, {Rev}", puts.Count, deletes.Count,
            lastRevision);
    }

    public async Task BeginStaleKeyTrackingAsync(DbConnection conn, CancellationToken ct = default)
    {
        await using var cmd = conn.CreateCommand();
        cmd.CommandText = _createTempSyncKeysSql;
        await cmd.ExecuteNonQueryAsync(ct);
        _logger.LogDebug("Temp sync keys table created");
    }

    public async Task TrackSyncKeysAsync(DbConnection conn, string[] keys)
    {
        if (keys.Length == 0)
        {
            return;
        }

        await conn.ExecuteAsync(_insertTempSyncKeySql, new { Keys = keys });
    }

    public async Task EndStaleKeyTrackingAsync(DbConnection conn, CancellationToken ct = default)
    {
        var deleted = await conn.ExecuteAsync(_deleteStaleKeysSql);

        await using var cmd = conn.CreateCommand();
        cmd.CommandText = "DROP TABLE IF EXISTS _sync_keys";
        await cmd.ExecuteNonQueryAsync(ct);

        _logger.LogInformation("Stale key cleanup complete: {Count} keys deleted", deleted);
    }

    public DbConnection CreateConnection()
    {
        return new NpgsqlConnection(_options.ConnectionString);
    }

    private static string ExtractHost(string? connectionString)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            return string.Empty;
        }

        var m = Regex.Match(connectionString, "Host=([^;]+)", RegexOptions.IgnoreCase);
        return m.Success ? m.Groups[1].Value : "unknown";
    }
}