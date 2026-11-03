using FleetTelemetry.Api;
using FleetTelemetry.Api.Endpoints;
using FleetTelemetry.Data;

var builder = WebApplication.CreateBuilder(args);
builder.Configuration.AddSnowflakeEnvironmentVariables();

builder.Services.AddProblemDetails();
builder.Services.AddExceptionHandler<BadRequestExceptionHandler>();
builder.Services.AddHttpContextAccessor();
builder.Services.AddSingleton(TimeProvider.System);
builder.Services.AddFleetSnowflake();
builder.Services.AddScoped<IQueryTagSource, HttpQueryTagSource>();
builder.Services.AddHealthChecks().AddCheck<SnowflakeHealthCheck>("snowflake");

var app = builder.Build();

app.UseExceptionHandler();
app.UseStatusCodePages();

app.MapHealthChecks("/health");
app.MapTelemetryEndpoints();
app.MapVehicleEndpoints();
app.MapFleetEndpoints();

app.Run();
