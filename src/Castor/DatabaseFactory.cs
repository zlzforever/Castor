using Castor.Services;
using Microsoft.Extensions.Options;

namespace Castor;

/// <summary>
/// 为多种数据库做准备
/// </summary>
/// <param name="options"></param>
/// <param name="serviceProvider"></param>
public class DatabaseFactory(IOptions<DatabaseOptions> options, IServiceProvider serviceProvider)
{
    public IRepository Create()
    {
        var type = options.Value.Type;
        var connectionString = options.Value.ConnectionString;
        if (string.IsNullOrEmpty(type))
        {
            throw new ArgumentNullException(nameof(type));
        }

        if (string.IsNullOrEmpty(connectionString))
        {
            throw new ArgumentNullException(nameof(connectionString));
        }

        return type.ToLowerInvariant() switch
        {
            "postgres" => serviceProvider.GetRequiredService<PostgreSQLRepository>(),
            _ => throw new NotImplementedException($"{type} repository not implemented")
        };
    }
}