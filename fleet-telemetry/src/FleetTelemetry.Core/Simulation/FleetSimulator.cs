using FleetTelemetry.Telemetry;

namespace FleetTelemetry.Simulation;

/// <summary>
/// Deterministic telemetry for a fleet: the same seed, vehicles and dates always produce
/// the same pings. Each vehicle leaves its depot in the morning, drives 2 to 4 delivery
/// trips with traffic stops, the occasional hard stop and loading-dock wait, and returns
/// to the depot. Devices report every <see cref="PingInterval"/> while the ignition is on.
/// </summary>
public sealed class FleetSimulator(int seed)
{
    public static readonly TimeSpan PingInterval = TimeSpan.FromSeconds(5);

    private static readonly FaultCode[] FaultCatalog =
    [
        new("P0420", "info"),
        new("P0301", "warning"),
        new("P0087", "warning"),
        new("U0100", "warning"),
        new("P0217", "critical"),
    ];

    /// <summary>Simulates every vehicle for <paramref name="days"/> consecutive days.</summary>
    public IReadOnlyList<VehicleDay> Simulate(IReadOnlyList<SimulatedVehicle> vehicles, DateOnly firstDay, int days)
    {
        ArgumentNullException.ThrowIfNull(vehicles);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(days);

        var result = new List<VehicleDay>(vehicles.Count * days);
        for (var v = 0; v < vehicles.Count; v++)
        {
            var vehicle = vehicles[v];
            var state = VehicleState.Initial(vehicle, new Random(unchecked((seed * 1_000_003) + v)));
            for (var d = 0; d < days; d++)
            {
                var rng = new Random(unchecked((seed * 1_000_003) + (v * 7_919) + ((d + 1) * 104_729)));
                result.Add(SimulateDay(vehicle, state, firstDay.AddDays(d), rng));
            }
        }

        return result;
    }

    private static VehicleDay SimulateDay(SimulatedVehicle vehicle, VehicleState state, DateOnly day, Random rng)
    {
        var driver = new Driver(vehicle, state, rng);
        state.Refuel(rng);
        state.Faults = rng.NextDouble() < 0.3 ? PlanFaults(rng) : null;

        var start = new DateTimeOffset(day.ToDateTime(new TimeOnly(5, 30)), TimeSpan.Zero)
            .AddMinutes(rng.Next(0, 90));
        var tripCount = rng.Next(2, 5);
        var time = start;
        for (var trip = 0; trip < tripCount; trip++)
        {
            var returning = trip == tripCount - 1;
            var (destLat, destLon, distanceKm) = returning
                ? (vehicle.DepotLat, vehicle.DepotLon, Math.Max(3, Geo.DistanceKm(state.Lat, state.Lon, vehicle.DepotLat, vehicle.DepotLon) * 1.25))
                : PickDestination(vehicle, state, rng);
            time = driver.DriveTrip(time, destLat, destLon, distanceKm);
            // Ignition off while unloading: 35 to 120 minutes without pings.
            time = time.AddMinutes(rng.Next(35, 121));
        }

        return new VehicleDay(vehicle.VehicleId, day, driver.Pings, driver.Trips, driver.HarshBrakes, driver.Idling);
    }

    private static (double Lat, double Lon, double DistanceKm) PickDestination(
        SimulatedVehicle vehicle, VehicleState state, Random rng)
    {
        var (min, max) = vehicle.VehicleClass == "van" ? (4.0, 18.0) : (10.0, 35.0);
        var straightKm = min + (rng.NextDouble() * (max - min));
        // Stay within about 40 km of the depot so a day's route remains regional.
        var bearing = Geo.DistanceKm(state.Lat, state.Lon, vehicle.DepotLat, vehicle.DepotLon) > 25
            ? Geo.BearingDeg(state.Lat, state.Lon, vehicle.DepotLat, vehicle.DepotLon) + ((rng.NextDouble() - 0.5) * 90)
            : rng.NextDouble() * 360;
        var (lat, lon) = Geo.Offset(state.Lat, state.Lon, bearing, straightKm);
        return (lat, lon, straightKm * 1.25);
    }

    private static FaultPlan PlanFaults(Random rng)
    {
        var first = FaultCatalog[rng.Next(FaultCatalog.Length)];
        var second = rng.NextDouble() < 0.25 ? FaultCatalog[rng.Next(FaultCatalog.Length)] : null;
        return new FaultPlan(
            ActiveFrom: rng.NextDouble() * 0.7,
            Codes: second is null || second == first ? [first] : [first, second]);
    }

    private sealed record FaultPlan(double ActiveFrom, FaultCode[] Codes);

    private sealed class VehicleState
    {
        public double Lat { get; set; }

        public double Lon { get; set; }

        public double OdometerKm { get; set; }

        public double FuelUsedL { get; set; }

        public double FuelInTankL { get; set; }

        public double CoolantC { get; set; }

        public FaultPlan? Faults { get; set; }

        public required double TankL { get; init; }

        public static VehicleState Initial(SimulatedVehicle vehicle, Random rng) => new()
        {
            Lat = vehicle.DepotLat,
            Lon = vehicle.DepotLon,
            OdometerKm = Math.Round(40_000 + (rng.NextDouble() * 360_000), 3),
            FuelUsedL = Math.Round(10_000 + (rng.NextDouble() * 90_000), 3),
            TankL = vehicle.FuelTankL,
        };

        public void Refuel(Random rng)
        {
            FuelInTankL = TankL * (0.9 + (rng.NextDouble() * 0.1));
            CoolantC = 30 + (rng.NextDouble() * 10);
        }
    }

    /// <summary>Emits pings for one vehicle-day and records the ground truth as it goes.</summary>
    private sealed class Driver(SimulatedVehicle vehicle, VehicleState state, Random rng)
    {
        private const double NormalBrakingKmhPerTick = 14;

        private readonly double _litresPerKm = vehicle.VehicleClass switch
        {
            "tractor" => 0.31,
            "rigid" => 0.23,
            _ => 0.105,
        };

        private readonly double _idleLitresPerTick = (vehicle.VehicleClass == "van" ? 0.8 : 2.4) / 720;

        private double _speed;
        private double _heading;
        private DateTimeOffset _time;
        private int _dayTick;

        public List<TelemetryPing> Pings { get; } = [];

        public List<TripTruth> Trips { get; } = [];

        public List<HarshBrakeTruth> HarshBrakes { get; } = [];

        public List<IdlingTruth> Idling { get; } = [];

        public DateTimeOffset DriveTrip(DateTimeOffset start, double destLat, double destLon, double distanceKm)
        {
            _time = start;
            _speed = 0;
            var firstPing = Pings.Count;
            var (fromLat, fromLon) = (state.Lat, state.Lon);
            var baseHeading = Geo.BearingDeg(fromLat, fromLon, destLat, destLon);
            var startOdometer = state.OdometerKm;
            // Urban, regional or highway cruising speed; trucks are limited to about 80 km/h.
            var cruise = (vehicle.VehicleClass == "van" ? 50 : 55) + (rng.Next(0, 3) * 12.5);
            double? harshAtKm = rng.NextDouble() < 0.4 ? distanceKm * (0.2 + (rng.NextDouble() * 0.5)) : null;
            double? dockAtKm = rng.NextDouble() < 0.3 ? distanceKm * (0.3 + (rng.NextDouble() * 0.4)) : null;

            // Ignition on: a few stationary pings before pulling away.
            Emit();
            Idle(rng.Next(2, 6));

            while (true)
            {
                var travelled = state.OdometerKm - startOdometer;
                var remainingM = (distanceKm - travelled) * 1000;
                Steer(fromLat, fromLon, destLat, destLon, baseHeading, travelled / distanceKm);

                if (remainingM <= StoppingDistanceM(_speed) + 30)
                {
                    BrakeToStop();
                    Idle(rng.Next(1, 4));
                    break;
                }

                if (dockAtKm is { } dock && travelled >= dock)
                {
                    dockAtKm = null;
                    BrakeToStop();
                    var idleStart = _time;
                    Idle(rng.Next(72, 109));
                    Idling.Add(new IdlingTruth(idleStart, _time));
                    continue;
                }

                if (harshAtKm is { } harsh && travelled >= harsh && _speed >= 48)
                {
                    harshAtKm = null;
                    var from = Rounded(_speed);
                    Tick(Math.Max(0, _speed - (48 + (rng.NextDouble() * 14))));
                    HarshBrakes.Add(new HarshBrakeTruth(_time, from, Rounded(_speed)));
                    continue;
                }

                if (_speed > 20 && rng.NextDouble() < 0.004)
                {
                    BrakeToStop();
                    Idle(rng.Next(3, 11));
                    continue;
                }

                Tick(_speed < cruise - 3
                    ? Math.Min(cruise, _speed + 6 + (rng.NextDouble() * 4))
                    : Math.Clamp(_speed + ((rng.NextDouble() - 0.5) * 4), cruise - 3, cruise + 3));
            }

            var trip = Pings.Skip(firstPing).ToList();
            Trips.Add(new TripTruth(
                trip[0].Ts,
                trip[^1].Ts,
                Math.Round(trip[^1].OdometerKm - trip[0].OdometerKm, 3),
                trip.Max(p => p.SpeedKmh),
                trip.Count));
            return _time;
        }

        private static double StoppingDistanceM(double speedKmh)
        {
            var metresPerSecond = speedKmh / 3.6;
            var deceleration = NormalBrakingKmhPerTick / 3.6 / PingInterval.TotalSeconds;
            return metresPerSecond * metresPerSecond / (2 * deceleration);
        }

        private static double Rounded(double speedKmh) => Math.Round(speedKmh, 1);

        private void Steer(double fromLat, double fromLon, double destLat, double destLon, double baseHeading, double progress)
        {
            var p = Math.Clamp(progress, 0, 1);
            // A gentle S-curve around the straight line, like a road would.
            var wobbleKm = Math.Sin(p * Math.PI * 3) * 0.4;
            var (lat, lon) = Geo.Offset(
                fromLat + ((destLat - fromLat) * p),
                fromLon + ((destLon - fromLon) * p),
                baseHeading + 90,
                wobbleKm);
            state.Lat = lat;
            state.Lon = lon;
            _heading = baseHeading + (Math.Cos(p * Math.PI * 3) * 15);
        }

        private void BrakeToStop()
        {
            while (_speed > 0)
            {
                Tick(Math.Max(0, _speed - (10 + (rng.NextDouble() * (NormalBrakingKmhPerTick - 10)))));
            }
        }

        private void Idle(int ticks)
        {
            for (var i = 0; i < ticks; i++)
            {
                Tick(0);
            }
        }

        private void Tick(double newSpeed)
        {
            var km = (_speed + newSpeed) / 2 / 3600 * PingInterval.TotalSeconds;
            state.OdometerKm += km;
            var litres = (km * _litresPerKm) + (newSpeed == 0 ? _idleLitresPerTick : 0);
            state.FuelUsedL += litres;
            state.FuelInTankL = Math.Max(0, state.FuelInTankL - litres);
            _speed = newSpeed;
            _time += PingInterval;
            Emit();
        }

        private void Emit()
        {
            _dayTick++;
            state.CoolantC = state.CoolantC < 86
                ? state.CoolantC + 1.1
                : 86 + (rng.NextDouble() * 6);
            var faults = ActiveFaults();
            var coolant = faults.Any(f => f.Code == "P0217") ? 106 + (rng.NextDouble() * 6) : state.CoolantC;
            var rpm = _speed == 0 ? 620 + rng.Next(0, 60) : 900 + (int)(_speed * 14) + rng.Next(-80, 80);

            Pings.Add(new TelemetryPing(
                MessageId: $"{vehicle.VehicleId}-{_time:yyyyMMddHHmmss}",
                VehicleId: vehicle.VehicleId,
                Ts: _time,
                Gps: new GpsFix(Math.Round(state.Lat, 5), Math.Round(state.Lon, 5), (int)Math.Round(((_heading % 360) + 360) % 360) % 360),
                SpeedKmh: Rounded(_speed),
                OdometerKm: Math.Round(state.OdometerKm, 3),
                Fuel: new FuelReading(
                    Math.Round(state.FuelInTankL / state.TankL * 100, 1),
                    Math.Round(state.FuelUsedL, 3)),
                Engine: new EngineReading(rpm, Math.Round(coolant, 1), faults)));
        }

        private FaultCode[] ActiveFaults()
        {
            // Faults appear part-way through the day (measured in pings) and stay active.
            if (state.Faults is not { } plan || _dayTick < plan.ActiveFrom * 2_000)
            {
                return [];
            }

            return plan.Codes;
        }
    }
}
