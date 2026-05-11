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
    }


    public string ConnectionString => _options.ConnectionString!;

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

    public void UpsertBatch(List<EtcdKvRecord> records, long lastRevision)
    {
        if (records.Count == 0) return;

        using var conn = CreateConnection();
        if (conn.State != System.Data.ConnectionState.Open)
        {
            conn.Open();
        }

        using var tx = conn.BeginTransaction();
        conn.Execute(_upsertSql, records, transaction: tx);
        conn.Execute(_updateSyncStateSql, new { LastRevision = lastRevision }, transaction: tx);
        tx.Commit();

        _logger.LogDebug("Batch upserted {Count} keys", records.Count);
    }

    public void SyncBatch(List<EtcdKvRecord> puts, List<string> deletes, long lastRevision)
    {
        if (puts.Count == 0 && deletes.Count == 0)
        {
            return;
        }

        using var conn = CreateConnection();
        if (conn.State != System.Data.ConnectionState.Open)
        {
            conn.Open();
        }

        using var tx = conn.BeginTransaction();

        if (puts.Count > 0)
        {
            conn.Execute(_upsertSql, puts, transaction: tx);
        }

        if (deletes.Count > 0)
        {
            conn.Execute(_deleteBatchSql, new { Keys = deletes }, transaction: tx);
        }

        conn.Execute(_updateSyncStateSql, new { LastRevision = lastRevision }, transaction: tx);
        tx.Commit();

        _logger.LogDebug("Applied watch events: {Puts} upserts, {Deletes} deletes, {Rev}", puts.Count, deletes.Count,
            lastRevision);
    }

    private NpgsqlConnection CreateConnection()
    {
        return new NpgsqlConnection(_options.ConnectionString);
    }
}