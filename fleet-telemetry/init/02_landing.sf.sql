-- Landing zone for vehicle telemetry. Both ingestion paths write here:
--   * POST /api/telemetry inserts each ping with PARSE_JSON(?);
--   * the backfill command PUTs NDJSON files to TELEMETRY_STAGE and runs COPY INTO.
-- The payload is kept exactly as the device sent it; typed views pick the fields.

USE ROLE FLEET_APP;
USE WAREHOUSE FLEET_WH;
USE SCHEMA FLEET.TELEMETRY;

CREATE TABLE IF NOT EXISTS TELEMETRY_RAW (
    PAYLOAD VARIANT
) COMMENT = 'One row per telemetry ping, as received from the vehicle';

-- One JSON document per line, gzip-compressed by PUT.
CREATE FILE FORMAT IF NOT EXISTS TELEMETRY_NDJSON
    TYPE = JSON COMPRESSION = AUTO
    COMMENT = 'Newline-delimited JSON telemetry exports';

CREATE STAGE IF NOT EXISTS TELEMETRY_STAGE
    FILE_FORMAT = TELEMETRY_NDJSON
    COMMENT = 'Backfill files uploaded by FleetTelemetry.Ingest';
