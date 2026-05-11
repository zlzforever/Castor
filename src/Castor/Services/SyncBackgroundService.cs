using System.Text.RegularExpressions;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Castor.Services;

public class SyncBackgroundService(
    IOptions<EtcdOptions> etcd,
    DatabaseFactory databaseFactory,
    ILogger<SyncBackgroundService> logger)
    : BackgroundService
{
    private readonly EtcdOptions _etcdOptions = etcd.Value;

    private static readonly ByteString AllKeysStart = ByteString.CopyFrom(0);
    private static readonly ByteString RangeEndAll = ByteString.CopyFrom(0);

    private readonly ByteString _rangeStart = string.IsNullOrWhiteSpace(etcd.Value.Prefix)
        ? AllKeysStart
        : ByteString.CopyFromUtf8(etcd.Value.Prefix);

    private readonly ByteString _rangeEnd = string.IsNullOrWhiteSpace(etcd.Value.Prefix)
        ? RangeEndAll
        : GetPrefixRangeEnd(etcd.Value.Prefix);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var repository = databaseFactory.Create();
        logger.LogInformation("Starting sync — etcd={Url}, prefix={Prefix}, pg={Host}", _etcdOptions.Url,
            _etcdOptions.Prefix, ExtractHost(repository.ConnectionString));
        await repository.InitializeAsync(stoppingToken);

        var lastRev = await repository.GetLastRevisionAsync(stoppingToken);
        logger.LogInformation("Last synced revision: {Rev}", lastRev);

        var etcdClient = !string.IsNullOrEmpty(_etcdOptions.Username)
            ? new EtcdClient(_etcdOptions.Url, _etcdOptions.Username, _etcdOptions.Password)
            : new EtcdClient(_etcdOptions.Url);

        // 第一次启动，或在数据库中把 rev 设为 0
        if (lastRev == 0)
        {
            logger.LogInformation("No sync checkpoint found, running initial full sync");
            lastRev = await FullSyncAsync(etcdClient, repository, stoppingToken);
            logger.LogInformation("Initial full sync complete, revision {Rev}", lastRev);
        }

        await WatchLoopAsync(etcdClient, repository, lastRev, stoppingToken);
    }

    private async Task<long> FullSyncAsync(EtcdClient client, IRepository repository, CancellationToken ct)
    {
        var count = 0;
        var key = _rangeStart;
        var latestRev = 0L;
        while (!ct.IsCancellationRequested)
        {
            var request = new RangeRequest
            {
                Key = key,
                RangeEnd = _rangeEnd,
                Limit = _etcdOptions.BatchSize,
                SortTarget = RangeRequest.Types.SortTarget.Key,
                SortOrder = RangeRequest.Types.SortOrder.Ascend
            };

            var response = await client.GetAsync(request, cancellationToken: ct);
            latestRev = response.Header.Revision;

            if (response.Kvs.Count == 0)
            {
                break;
            }

            var batch = new List<EtcdKvRecord>(response.Kvs.Count);
            foreach (var kv in response.Kvs)
            {
                batch.Add(new EtcdKvRecord
                {
                    Key = kv.Key.ToStringUtf8(),
                    Value = kv.Value.ToStringUtf8(),
                    Version = kv.Version,
                    CreateRevision = kv.CreateRevision,
                    ModRevision = kv.ModRevision
                });
            }

            repository.UpsertBatch(batch, latestRev);
            count += batch.Count;

            var lastKey = response.Kvs[^1].Key;
            key = ByteString.CopyFromUtf8(lastKey.ToStringUtf8() + "\0");

            logger.LogDebug("Full sync batch: {Count} keys, rev {Rev}", batch.Count, latestRev);

            await Task.Delay(50, ct);
        }

        logger.LogInformation("Full sync complete: {Count} keys, rev {Rev}", count, latestRev);
        return latestRev;
    }

    private async Task WatchLoopAsync(EtcdClient etcdClient, IRepository repository, long startRev,
        CancellationToken stoppingToken)
    {
        var lastCheckpointRev = startRev;
        var processed = 0L;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var request = new WatchRequest
                {
                    CreateRequest = new WatchCreateRequest
                    {
                        Key = _rangeStart,
                        RangeEnd = _rangeEnd,
                        StartRevision = lastCheckpointRev,
                        ProgressNotify = false
                    }
                };

                void WatchResponseHandler(WatchResponse response)
                {
                    try
                    {
                        if (response.Events.Count == 0)
                        {
                            return;
                        }

                        var batch = new List<EtcdKvRecord>();
                        var deletes = new List<string>();
                        var maxEventRev = lastCheckpointRev;

                        foreach (var e in response.Events)
                        {
                            if (e?.Kv == null)
                            {
                                continue;
                            }

                            var eventKey = e.Kv.Key.ToStringUtf8();
                            if (eventKey == null)
                            {
                                continue;
                            }

                            if (e.Type == Mvccpb.Event.Types.EventType.Put)
                            {
                                batch.Add(new EtcdKvRecord
                                {
                                    Key = eventKey,
                                    Value = e.Kv.Value.ToStringUtf8(),
                                    Version = e.Kv.Version,
                                    CreateRevision = e.Kv.CreateRevision,
                                    ModRevision = e.Kv.ModRevision
                                });
                            }
                            else if (e.Type == Mvccpb.Event.Types.EventType.Delete)
                            {
                                deletes.Add(eventKey);
                            }

                            if (e.Kv?.ModRevision > maxEventRev)
                            {
                                maxEventRev = e.Kv.ModRevision;
                            }
                        }

                        if (maxEventRev > lastCheckpointRev)
                        {
                            lastCheckpointRev = maxEventRev;
                        }

                        repository.SyncBatch(batch, deletes, lastCheckpointRev);
                        processed = Interlocked.Add(ref processed, response.Events.Count);

                        logger.LogInformation("Applied {Puts} puts + {Deletes} deletes @ rev {Rev}, total processed {Total}",
                            batch.Count, deletes.Count, lastCheckpointRev, processed);
                    }
                    catch (Exception ex)
                    {
                        logger.LogError(ex, "Watch event handler failed");
                    }
                }

                await etcdClient.WatchAsync(request, WatchResponseHandler, cancellationToken: stoppingToken);
                logger.LogInformation("Watch established from revision {Rev}", lastCheckpointRev);

                // 一直等待到程序取消！！！
                await Task.Delay(Timeout.Infinite, stoppingToken);
            }
            catch (RpcException ex) when (IsCompactedError(ex))
            {
                logger.LogWarning("Revision {Rev} compacted by etcd, falling back to full sync", lastCheckpointRev);
                lastCheckpointRev = await FullSyncAsync(etcdClient, repository, stoppingToken);
                logger.LogInformation("Re-sync complete, restored to revision {Rev}", lastCheckpointRev);
                await Task.Delay(150, stoppingToken);
            }
            catch (Exception ex) when (!stoppingToken.IsCancellationRequested)
            {
                logger.LogError(ex, "Watch loop failed, will try again after 5 seconds");
                await Task.Delay(5000, stoppingToken);
            }
        }
    }

    private static bool IsCompactedError(RpcException ex)
    {
        return ex.StatusCode == StatusCode.OutOfRange
               || ex.Status.Detail.Contains("compacted", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>
    /// etcd prefix range end: increment the last byte to create an exclusive upper bound.
    /// For prefix "/apisix", returns "/apisiy", so range ["/apisix","/apisiy") catches all /apisix/*.
    /// </summary>
    private static ByteString GetPrefixRangeEnd(string prefix)
    {
        var bytes = System.Text.Encoding.UTF8.GetBytes(prefix);
        for (var i = bytes.Length - 1; i >= 0; i--)
        {
            if (bytes[i] < 0xff)
            {
                bytes[i]++;
                return ByteString.CopyFrom(bytes, 0, i + 1);
            }
        }

        return ByteString.CopyFrom(0); // all bytes 0xff: fallback to all keys
    }

    private static string ExtractHost(string connectionString)
    {
        var m = Regex.Match(connectionString, "Host=([^;]+)", RegexOptions.IgnoreCase);
        return m.Success ? m.Groups[1].Value : "unknown";
    }
}