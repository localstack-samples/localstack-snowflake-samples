using Dapper;

namespace FleetTelemetry.Data;

/// <summary>Writes pings received over HTTP into the landing table.</summary>
public sealed class TelemetryWriter(FleetDatabase database)
{
    /// <summary>
    /// Inserts a validated JSON array of pings in one statement: the whole batch is one bound
    /// parameter, parsed and split server-side, so a batch lands completely or not at all.
    /// </summary>
    public async Task<int> InsertBatchAsync(string pingsJsonArray, CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenAsync(cancellationToken);
        return await connection.ExecuteAsync(new CommandDefinition(
            """
            INSERT INTO TELEMETRY_RAW (PAYLOAD)
            SELECT p.value FROM TABLE(FLATTEN(INPUT => PARSE_JSON(:pings))) p
            """,
            new { pings = pingsJsonArray },
            cancellationToken: cancellationToken));
    }
}
