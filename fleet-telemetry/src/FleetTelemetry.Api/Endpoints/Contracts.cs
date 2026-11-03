using FleetTelemetry.Kpis;

namespace FleetTelemetry.Api.Endpoints;

// Response bodies. Timestamps are UTC.

public sealed record TelemetryAccepted(int Accepted);

public sealed record Trip(
    int TripNo,
    DateTime StartedAt,
    DateTime EndedAt,
    long DurationS,
    double DistanceKm,
    double FuelUsedL,
    double? FuelEconomyLPer100Km,
    double? AvgSpeedKmh,
    double MaxSpeedKmh,
    long IdleS,
    long Samples);

public sealed record VehicleTrips(string VehicleId, DateOnly Date, IReadOnlyList<Trip> Trips);

public sealed record HarshBrakingEvent(
    DateTime OccurredAt,
    double Lat,
    double Lon,
    double FromSpeedKmh,
    double ToSpeedKmh,
    double DecelerationMs2);

public sealed record IdlingEvent(DateTime StartedAt, DateTime EndedAt, long DurationS, double Lat, double Lon);

public sealed record VehicleEvents<T>(string VehicleId, string Type, IReadOnlyList<T> Events);

public sealed record ActiveFault(
    string Code,
    string Severity,
    long Occurrences,
    DateTime FirstSeen,
    DateTime LastSeen,
    double MaxCoolantC);

public sealed record VehicleFaults(string VehicleId, IReadOnlyList<ActiveFault> Faults);

public sealed record FleetDaily(DateOnly From, DateOnly To, IReadOnlyList<FleetDayKpi> Days);
