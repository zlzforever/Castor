using System.Data.Common;

namespace Castor.Services;

public interface IRepository
{
    string HostLabel { get; }
    Task InitializeAsync(CancellationToken stoppingToken = default);
    Task<long> GetLastRevisionAsync(CancellationToken stoppingToken = default);

    Task UpsertBatchAsync(List<EtcdKvRecord> records, long lastRevision,
        CancellationToken stoppingToken = default);

    Task SyncBatchAsync(List<EtcdKvRecord> puts, List<string> deletes, long lastRevision,
        CancellationToken stoppingToken = default);

    DbConnection CreateConnection();

    Task BeginStaleKeyTrackingAsync(DbConnection conn, CancellationToken ct = default);
    Task TrackSyncKeysAsync(DbConnection conn, string[] keys);
    Task EndStaleKeyTrackingAsync(DbConnection conn, CancellationToken ct = default);
}