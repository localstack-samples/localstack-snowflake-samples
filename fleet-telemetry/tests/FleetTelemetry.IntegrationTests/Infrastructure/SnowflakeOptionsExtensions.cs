using FleetTelemetry.Data;

namespace FleetTelemetry.IntegrationTests.Infrastructure;

internal static class SnowflakeOptionsExtensions
{
    public static SnowflakeOptions With(this SnowflakeOptions o, string database, string schema) => new()
    {
        Account = o.Account,
        Host = o.Host,
        Port = o.Port,
        User = o.User,
        PrivateKeyPath = o.PrivateKeyPath,
        Role = o.Role,
        Warehouse = o.Warehouse,
        Database = database,
        Schema = schema,
        Application = o.Application,
    };

    public static void CopyTo(this SnowflakeOptions source, SnowflakeOptions target)
    {
        target.Account = source.Account;
        target.Host = source.Host;
        target.Port = source.Port;
        target.User = source.User;
        target.PrivateKeyPath = source.PrivateKeyPath;
        target.Role = source.Role;
        target.Warehouse = source.Warehouse;
        target.Database = source.Database;
        target.Schema = source.Schema;
        target.Application = source.Application;
    }
}
