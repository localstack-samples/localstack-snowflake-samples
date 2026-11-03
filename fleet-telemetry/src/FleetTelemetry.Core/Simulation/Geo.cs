namespace FleetTelemetry.Simulation;

/// <summary>Small-distance geodesy, accurate enough for regional routes.</summary>
internal static class Geo
{
    private const double EarthRadiusKm = 6371.0;

    public static double DistanceKm(double lat1, double lon1, double lat2, double lon2)
    {
        var dLat = ToRadians(lat2 - lat1);
        var dLon = ToRadians(lon2 - lon1);
        var a = (Math.Sin(dLat / 2) * Math.Sin(dLat / 2))
            + (Math.Cos(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) * Math.Sin(dLon / 2) * Math.Sin(dLon / 2));
        return 2 * EarthRadiusKm * Math.Asin(Math.Sqrt(a));
    }

    public static double BearingDeg(double lat1, double lon1, double lat2, double lon2)
    {
        var y = Math.Sin(ToRadians(lon2 - lon1)) * Math.Cos(ToRadians(lat2));
        var x = (Math.Cos(ToRadians(lat1)) * Math.Sin(ToRadians(lat2)))
            - (Math.Sin(ToRadians(lat1)) * Math.Cos(ToRadians(lat2)) * Math.Cos(ToRadians(lon2 - lon1)));
        return (Math.Atan2(y, x) * 180 / Math.PI + 360) % 360;
    }

    public static (double Lat, double Lon) Offset(double lat, double lon, double bearingDeg, double distanceKm)
    {
        var bearing = ToRadians(bearingDeg);
        var dLat = distanceKm * Math.Cos(bearing) / EarthRadiusKm;
        var dLon = distanceKm * Math.Sin(bearing) / (EarthRadiusKm * Math.Cos(ToRadians(lat)));
        return (lat + (dLat * 180 / Math.PI), lon + (dLon * 180 / Math.PI));
    }

    private static double ToRadians(double degrees) => degrees * Math.PI / 180;
}
