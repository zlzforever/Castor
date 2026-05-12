using Castor;
using Castor.Services;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using Serilog;
using ExportProcessorType = OpenTelemetry.ExportProcessorType;

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
        .Enrich.FromLogContext()
        .Enrich.WithThreadId()
        .WriteTo.Console()
        .WriteTo.Async(x => x.File(logPath, rollingInterval: RollingInterval.Day))
        .CreateLogger();
}

builder.Logging.ClearProviders();
builder.Logging.AddSerilog();

var otelEndpoint = builder.Configuration["OpenTelemetry:Endpoint"];
if (!string.IsNullOrEmpty(otelEndpoint))
{
    var serviceName = builder.Configuration["ServiceName"];
    if (string.IsNullOrEmpty(serviceName))
    {
        serviceName = "castor";
    }

    var apiKey = builder.Configuration["OpenTelemetry:ApiKey"];
    var authorization = string.IsNullOrEmpty(apiKey) ? null : $"Authorization={apiKey}";
    var samplerProbability = builder.Configuration.GetSection("OpenTelemetry")
        .GetValue<double?>("SamplerProbability") ?? 0.5;
    var instanceId =
        $"${Environment.GetEnvironmentVariable("DAPR_HOST_IP") ?? Environment.GetEnvironmentVariable("HOST_IP")}:{Environment.GetEnvironmentVariable("DAPR_HTTP_PORT")}";
    instanceId = string.IsNullOrWhiteSpace(instanceId) ? null : $"{serviceName}-{instanceId}";
    var @namespace = builder.Configuration["OpenTelemetry:Namespace"];
// 2. 添加 OpenTelemetry 服务
    builder.Services.AddOpenTelemetry()
        // 配置资源，让所有信号（Trace/Metrics/Logs）共享
        .ConfigureResource(configure =>
        {
            configure.AddService(
                    serviceName: serviceName, // 替换为你的服务名
                    serviceVersion: "1.0.0", serviceInstanceId: instanceId, serviceNamespace: @namespace,
                    autoGenerateServiceInstanceId: true)
                .AddAttributes(new Dictionary<string, object>
                {
                    ["deployment.environment"] = builder.Environment.EnvironmentName
                });
        })
        // 🔍 追踪（Traces）配置
        .WithTracing(tracing => tracing
            .SetSampler(new ParentBasedSampler(new TraceIdRatioBasedSampler(samplerProbability))) // 采样率
            .AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri(otelEndpoint);
                options.ExportProcessorType = ExportProcessorType.Batch; // 批量导出提升性能
                // 👇 关键：添加 Authorization Header
                options.Headers = authorization;
                options.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
            }))
        // 📊 指标（Metrics）配置
        .WithMetrics(metrics => metrics
            .AddRuntimeInstrumentation() // 运行时指标（CPU、内存、GC）
            .AddOtlpExporter(options =>
            {
                options.Endpoint = new Uri(otelEndpoint);
                options.ExportProcessorType = ExportProcessorType.Batch; // 批量导出提升性能
                // 👇 关键：添加 Authorization Header
                options.Headers = authorization;
                options.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
            }));
}

builder.Services.Configure<EtcdOptions>(builder.Configuration.GetSection(EtcdOptions.SectionName));
builder.Services.Configure<DatabaseOptions>(builder.Configuration.GetSection(DatabaseOptions.SectionName));
builder.Services.AddSingleton<PostgreSQLRepository>();
builder.Services.AddSingleton<DatabaseFactory>();
builder.Services.AddHostedService<SyncBackgroundService>();


var host = builder.Build();
await host.RunAsync();