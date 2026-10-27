using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace FleetTelemetry.Data;

public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Registers the Snowflake data access. The caller registers an <see cref="IQueryTagSource"/>
    /// describing its unit of work (an HTTP request, a command-line run).
    /// </summary>
    public static IServiceCollection AddFleetSnowflake(this IServiceCollection services)
    {
        services.AddOptions<SnowflakeOptions>().BindConfiguration(SnowflakeOptions.Section).ValidateOnStart();
        services.AddSingleton<IValidateOptions<SnowflakeOptions>, SnowflakeOptionsValidator>();
        services.AddScoped<FleetDatabase>();
        services.AddScoped<FleetRegistry>();
        services.AddScoped<TelemetryWriter>();
        services.AddScoped<VehicleAnalytics>();
        services.AddScoped<FleetAnalytics>();
        services.AddScoped<TelemetryBackfill>();
        return services;
    }
}
