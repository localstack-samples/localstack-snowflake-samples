using Dapper;
using FleetTelemetry.Simulation;

namespace FleetTelemetry.Data;

/// <summary>Reads the fleet registry (VEHICLES and DEPOTS).</summary>
public sealed class FleetRegistry(FleetDatabase database)
{
    public async Task<bool> VehicleExistsAsync(string vehicleId, CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenAsync(cancellationToken);
        return await connection.ExecuteScalarAsync<long>(new CommandDefinition(
            "SELECT COUNT(*) FROM VEHICLES WHERE VEHICLE_ID = :vehicleId",
            new { vehicleId },
            cancellationToken: cancellationToken)) > 0;
    }

    /// <summary>Every vehicle with its depot location, in vehicle id order.</summary>
    public async Task<IReadOnlyList<SimulatedVehicle>> ListVehiclesAsync(CancellationToken cancellationToken)
    {
        await using var connection = await database.OpenAsync(cancellationToken);
        var rows = await connection.QueryAsync<VehicleRow>(new CommandDefinition(
            """
            SELECT v.VEHICLE_ID, v.VEHICLE_CLASS, v.FUEL_TANK_L::FLOAT AS FUEL_TANK_L,
                   d.LAT AS DEPOT_LAT, d.LON AS DEPOT_LON
            FROM VEHICLES v
            JOIN DEPOTS d ON d.DEPOT_ID = v.DEPOT_ID
            ORDER BY v.VEHICLE_ID
            """,
            cancellationToken: cancellationToken));
        return rows.Select(r => new SimulatedVehicle(r.VehicleId, r.VehicleClass, r.FuelTankL, r.DepotLat, r.DepotLon))
            .ToList();
    }

    private sealed class VehicleRow
    {
        public string VehicleId { get; init; } = "";

        public string VehicleClass { get; init; } = "";

        public double FuelTankL { get; init; }

        public double DepotLat { get; init; }

        public double DepotLon { get; init; }
    }
}
