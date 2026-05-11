using System.Diagnostics.CodeAnalysis;

namespace Castor;

[SuppressMessage("ReSharper", "AutoPropertyCanBeMadeGetOnly.Global")]
[SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Global")]
public class DatabaseOptions
{
    public const string SectionName = "Database";

    public string? Type { get; set; }
    public string? ConnectionString { get; set; }
    public string? TablePrefix { get; set; }
}