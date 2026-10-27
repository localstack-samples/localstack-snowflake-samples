using Dapper;
using FleetTelemetry.Data;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace FleetTelemetry.Api;

/// <summary>Healthy when a session can be opened and answers a query.</summary>
internal sealed class SnowflakeHealthCheck(FleetDatabase database) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = await database.OpenAsync(cancellationToken);
            await connection.ExecuteScalarAsync<long>(new CommandDefinition("SELECT 1", cancellationToken: cancellationToken));
            return HealthCheckResult.Healthy();
        }
        catch (Exception e) when (e is not OperationCanceledException)
        {
            return HealthCheckResult.Unhealthy("Snowflake is not reachable", e);
        }
    }
}
