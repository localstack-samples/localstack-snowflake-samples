using FleetTelemetry.Telemetry;

namespace FleetTelemetry.Simulation;

/// <summary>A vehicle from the fleet registry, with the depot it starts each day from.</summary>
public sealed record SimulatedVehicle(
    string VehicleId,
    string VehicleClass,
    double FuelTankL,
    double DepotLat,
    double DepotLon);

/// <summary>What the simulator knows about a trip it drove: first and last ping, odometer distance.</summary>
public sealed record TripTruth(
    DateTimeOffset StartedAt,
    DateTimeOffset EndedAt,
    double DistanceKm,
    double MaxSpeedKmh,
    int Samples);

/// <summary>A planned hard stop: the speed dropped from <c>FromKmh</c> to <c>ToKmh</c> at <c>At</c>.</summary>
public sealed record HarshBrakeTruth(DateTimeOffset At, double FromKmh, double ToKmh);

/// <summary>A planned wait with the engine running (a loading dock), first to last stationary ping.</summary>
public sealed record IdlingTruth(DateTimeOffset StartedAt, DateTimeOffset EndedAt);

/// <summary>One vehicle on one UTC day: the pings it sent and the ground truth behind them.</summary>
public sealed record VehicleDay(
    string VehicleId,
    DateOnly Date,
    IReadOnlyList<TelemetryPing> Pings,
    IReadOnlyList<TripTruth> Trips,
    IReadOnlyList<HarshBrakeTruth> HarshBrakes,
    IReadOnlyList<IdlingTruth> Idling);
