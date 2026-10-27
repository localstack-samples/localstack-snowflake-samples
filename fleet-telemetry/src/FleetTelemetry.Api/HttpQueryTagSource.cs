using System.Diagnostics;
using FleetTelemetry.Data;

namespace FleetTelemetry.Api;

/// <summary>
/// Tags Snowflake queries with the HTTP route and the request's W3C trace id (taken from an
/// incoming <c>traceparent</c> header when there is one), so a query in Snowflake's history
/// can be matched to its trace.
/// </summary>
internal sealed class HttpQueryTagSource(IHttpContextAccessor accessor) : IQueryTagSource
{
    public string Operation => accessor.HttpContext is { } context
        ? $"{context.Request.Method} {(context.GetEndpoint() as RouteEndpoint)?.RoutePattern.RawText ?? context.Request.Path.Value}"
        : "background";

    public string? CorrelationId => Activity.Current?.TraceId.ToHexString() ?? accessor.HttpContext?.TraceIdentifier;
}
