using System.Data.Common;
using System.Text;
using Dapper;
using Microsoft.Extensions.Options;
using Snowflake.Data.Client;

namespace FleetTelemetry.Data;

/// <summary>Describes the unit of work running queries, for Snowflake's QUERY_TAG.</summary>
public interface IQueryTagSource
{
    /// <summary>Short, stable description of the operation, such as an HTTP route or a command name.</summary>
    string Operation { get; }

    /// <summary>Correlation id of the request or run, or null.</summary>
    string? CorrelationId { get; }
}

/// <summary>A fixed query tag, for command-line tools.</summary>
public sealed class FixedQueryTag(string operation) : IQueryTagSource
{
    public string Operation { get; } = operation;

    public string? CorrelationId { get; } = Guid.NewGuid().ToString("N");
}

/// <summary>
/// Opens pooled Snowflake sessions. Every session is tagged with the application, the
/// operation and a correlation id, so a slow query in Snowflake's query history leads
/// straight back to the request that ran it.
/// </summary>
public sealed class FleetDatabase(IOptions<SnowflakeOptions> options, IQueryTagSource tagSource)
{
    static FleetDatabase()
    {
        // Snowflake returns upper-case SNAKE_CASE column names; map them to PascalCase properties.
        DefaultTypeMap.MatchNamesWithUnderscores = true;
    }

    private readonly string _connectionString = options.Value.ToConnectionString();

    public async Task<DbConnection> OpenAsync(CancellationToken cancellationToken)
    {
        var connection = new SnowflakeDbConnection(_connectionString);
        try
        {
            await connection.OpenAsync(cancellationToken);
            var tag = QueryTag(options.Value.Application, tagSource);
            await connection.ExecuteAsync(new CommandDefinition(
                $"ALTER SESSION SET QUERY_TAG = '{tag}'", cancellationToken: cancellationToken));
            return connection;
        }
        catch
        {
            await connection.DisposeAsync();
            throw;
        }
    }

    /// <summary>A JSON query tag. Values are reduced to a safe character set, so the literal needs no escaping.</summary>
    internal static string QueryTag(string application, IQueryTagSource source)
    {
        var tag = new StringBuilder("{\"app\":\"").Append(Safe(application))
            .Append("\",\"op\":\"").Append(Safe(source.Operation)).Append('"');
        if (source.CorrelationId is { } id)
        {
            tag.Append(",\"id\":\"").Append(Safe(id)).Append('"');
        }

        return tag.Append('}').ToString();

        static string Safe(string value) => new(value
            .Where(c => char.IsAsciiLetterOrDigit(c) || c is ' ' or '/' or '{' or '}' or '-' or '_' or '.' or ':')
            .Take(200)
            .ToArray());
    }
}
