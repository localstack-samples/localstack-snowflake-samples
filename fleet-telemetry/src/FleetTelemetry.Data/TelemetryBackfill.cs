using System.Data.Common;
using System.Globalization;
using Microsoft.Extensions.Logging;

namespace FleetTelemetry.Data;

/// <summary>Result of uploading one local file with PUT.</summary>
public sealed record UploadedFile(string File, string Status);

/// <summary>COPY INTO result for one staged file.</summary>
public sealed record LoadedFile(
    string File,
    string Status,
    long RowsParsed,
    long RowsLoaded,
    long ErrorsSeen,
    string? FirstError,
    long? FirstErrorLine);

public sealed record BackfillReport(IReadOnlyList<UploadedFile> Uploaded, IReadOnlyList<LoadedFile> Loaded)
{
    public long RowsLoaded => Loaded.Sum(f => f.RowsLoaded);

    public long RowsFailed => Loaded.Sum(f => f.ErrorsSeen);
}

/// <summary>
/// Bulk-loads NDJSON exports: PUT uploads them to the internal stage (compressed), COPY INTO
/// loads every file not loaded before, skipping bad lines (ON_ERROR = CONTINUE) and
/// reporting them per file. Snowflake's load history makes a re-run load nothing twice.
/// </summary>
public sealed partial class TelemetryBackfill(FleetDatabase database, FleetAnalytics analytics, ILogger<TelemetryBackfill> logger)
{
    public const string StagePath = "@TELEMETRY_STAGE/backfill/";

    public async Task<BackfillReport> RunAsync(string directory, CancellationToken cancellationToken)
    {
        var fullPath = Path.GetFullPath(directory);
        if (!Directory.EnumerateFiles(fullPath, "*.ndjson").Any())
        {
            throw new FileNotFoundException($"No *.ndjson files in {fullPath}.");
        }

        await using var connection = await database.OpenAsync(cancellationToken);

        // The driver runs PUT synchronously: it uploads the files itself.
        var uploaded = await Task.Run(
            () => ReadRows(
                connection,
                $"PUT 'file://{fullPath.Replace('\\', '/')}/*.ndjson' {StagePath} AUTO_COMPRESS = TRUE",
                r => new UploadedFile(r.GetString(r.GetOrdinal("SourceFileName")), r.GetString(r.GetOrdinal("ResultStatus")))),
            cancellationToken);
        foreach (var file in uploaded)
        {
            LogUploaded(file.File, file.Status);
        }

        var loaded = await Task.Run(
            () => ReadRows(
                connection,
                $"""
                COPY INTO TELEMETRY_RAW
                FROM {StagePath}
                FILE_FORMAT = (FORMAT_NAME = 'TELEMETRY_NDJSON')
                ON_ERROR = CONTINUE
                """,
                ReadCopyResult),
            cancellationToken);
        var report = new BackfillReport(uploaded, loaded.OfType<LoadedFile>().ToList());
        foreach (var file in report.Loaded)
        {
            if (file.ErrorsSeen > 0)
            {
                LogLoadedWithErrors(file.File, file.Status, file.RowsLoaded, file.RowsParsed, file.FirstErrorLine, file.FirstError);
            }
            else
            {
                LogLoaded(file.File, file.Status, file.RowsLoaded);
            }
        }

        if (report.Loaded.Count == 0)
        {
            LogNothingToLoad();
        }

        await analytics.RefreshAsync(cancellationToken);
        return report;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "PUT {File}: {Status}")]
    private partial void LogUploaded(string file, string status);

    [LoggerMessage(Level = LogLevel.Information, Message = "COPY {File}: {Status}, {RowsLoaded} rows loaded")]
    private partial void LogLoaded(string file, string status, long rowsLoaded);

    [LoggerMessage(
        Level = LogLevel.Warning,
        Message = "COPY {File}: {Status}, {RowsLoaded} of {RowsParsed} rows loaded, first error at line {Line}: {Error}")]
    private partial void LogLoadedWithErrors(string file, string status, long rowsLoaded, long rowsParsed, long? line, string? error);

    [LoggerMessage(Level = LogLevel.Information, Message = "COPY: every staged file was already loaded")]
    private partial void LogNothingToLoad();

    /// <summary>One row per file, or a single status row ("0 files processed") when there was nothing new.</summary>
    private static LoadedFile? ReadCopyResult(DbDataReader r)
    {
        if (r.FieldCount == 1)
        {
            return null;
        }

        return new LoadedFile(
            File: Path.GetFileName(r.GetString(r.GetOrdinal("file"))),
            Status: r.GetString(r.GetOrdinal("status")),
            RowsParsed: Convert.ToInt64(r.GetValue(r.GetOrdinal("rows_parsed")), CultureInfo.InvariantCulture),
            RowsLoaded: Convert.ToInt64(r.GetValue(r.GetOrdinal("rows_loaded")), CultureInfo.InvariantCulture),
            ErrorsSeen: Convert.ToInt64(r.GetValue(r.GetOrdinal("errors_seen")), CultureInfo.InvariantCulture),
            FirstError: r.IsDBNull(r.GetOrdinal("first_error")) ? null : r.GetString(r.GetOrdinal("first_error")),
            FirstErrorLine: r.IsDBNull(r.GetOrdinal("first_error_line"))
                ? null
                : Convert.ToInt64(r.GetValue(r.GetOrdinal("first_error_line")), CultureInfo.InvariantCulture));
    }

    private static List<T> ReadRows<T>(DbConnection connection, string sql, Func<DbDataReader, T> map)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();
        var rows = new List<T>();
        while (reader.Read())
        {
            rows.Add(map(reader));
        }

        return rows;
    }
}
