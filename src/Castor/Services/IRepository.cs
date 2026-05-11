namespace Castor.Services;

public interface IRepository
{
    string ConnectionString { get; }
    Task InitializeAsync(CancellationToken stoppingToken = default);
    Task<long> GetLastRevisionAsync(CancellationToken stoppingToken = default);
    void UpsertBatch(List<EtcdKvRecord> records, long lastRevision);
    void SyncBatch(List<EtcdKvRecord> puts, List<string> deletes, long lastRevision);
}