using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace FleetTelemetry.Telemetry;

/// <summary>The device wire format: snake_case properties, required fields enforced.</summary>
public static class TelemetryJson
{
    public static JsonSerializerOptions Options { get; } = new(JsonSerializerDefaults.Web)
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower,
        RespectNullableAnnotations = true,
        RespectRequiredConstructorParameters = true,
        NumberHandling = JsonNumberHandling.Strict,
        WriteIndented = false,
        Converters = { new UtcTimestampConverter() },
    };

    /// <summary>Reads any ISO 8601 offset; writes UTC with a Z suffix, as devices do.</summary>
    private sealed class UtcTimestampConverter : JsonConverter<DateTimeOffset>
    {
        public override DateTimeOffset Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options) =>
            reader.GetDateTimeOffset();

        public override void Write(Utf8JsonWriter writer, DateTimeOffset value, JsonSerializerOptions options) =>
            writer.WriteStringValue(value.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.FFFFFFF'Z'", CultureInfo.InvariantCulture));
    }
}
