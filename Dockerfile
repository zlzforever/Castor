FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore
RUN cd src/Castor && dotnet publish -o /output && \
    rm -f /output/appsettings.Development.json


FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app

# 安装 PostgreSQL 依赖的 krb5 库（正确、无错版本）
RUN apt-get update && apt-get install -y --no-install-recommends \
    krb5-user libgssapi-krb5-2 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=build /output .
COPY docker-entrypoint.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/docker-entrypoint.sh
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["dotnet","Castor.dll"]