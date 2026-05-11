using Castor;
using Castor.Services;
using Serilog;
using Serilog.Events;

var builder = Host.CreateApplicationBuilder(args);

var serilogSection = builder.Configuration.GetSection("Serilog");
if (serilogSection.GetChildren().Any())
{
    Log.Logger = new LoggerConfiguration().ReadFrom
        .Configuration(builder.Configuration)
        .Enrich.FromLogContext()
        .CreateLogger();
}
else
{
    var logPath = builder.Configuration["LOG_PATH"] ?? builder.Configuration["LOGPATH"];
    if (string.IsNullOrEmpty(logPath))
    {
        logPath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs/log.txt");
    }

    Log.Logger = new LoggerConfiguration()
        .MinimumLevel.Information()
        .MinimumLevel.Override("Microsoft.AspNetCore", LogEventLevel.Warning)
        .MinimumLevel.Override("Microsoft.EntityFrameworkCore", LogEventLevel.Warning)
        .MinimumLevel.Override("MicroserviceFramework.Mediator", LogEventLevel.Warning)
        .Enrich.FromLogContext()
        .Enrich.WithThreadId()
        .WriteTo.Console()
        .WriteTo.Async(x => x.File(logPath, rollingInterval: RollingInterval.Day))
        .CreateLogger();
}

builder.Logging.ClearProviders();
builder.Logging.AddSerilog();

builder.Services.Configure<EtcdOptions>(builder.Configuration.GetSection(EtcdOptions.SectionName));
builder.Services.Configure<DatabaseOptions>(builder.Configuration.GetSection(DatabaseOptions.SectionName));
builder.Services.AddSingleton<PostgreSQLRepository>();
builder.Services.AddSingleton<DatabaseFactory>();
builder.Services.AddHostedService<SyncBackgroundService>();


var host = builder.Build();
await host.RunAsync();