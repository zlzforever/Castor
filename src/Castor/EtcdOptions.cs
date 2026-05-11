using System.Diagnostics.CodeAnalysis;

namespace Castor;

[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
public class EtcdOptions
{
    public const string SectionName = "Etcd";

    public string? Url { get; set; }
    public int BatchSize { get; set; } = 50;
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? Prefix { get; set; }
}