namespace Castor.Services;

public class EtcdKvRecord
{
    public string Key { get; set; } = string.Empty;
    public string Value { get; set; } = string.Empty;
    public long Version { get; set; }
    public long CreateRevision { get; set; }
    public long ModRevision { get; set; }
}
