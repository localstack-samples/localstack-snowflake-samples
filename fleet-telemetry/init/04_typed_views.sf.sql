-- Typed views over the landing table. Every query in the API reads these, so a
-- new device field only needs a column here, never a change to the landing table.

USE ROLE FLEET_APP;
USE WAREHOUSE FLEET_WH;
USE SCHEMA FLEET.TELEMETRY;

-- One row per ping. Devices retry uploads, so a message can arrive more than
-- once; the view keeps one copy per MESSAGE_ID. Partitioning by VEHICLE_ID as well
-- lets a query's VEHICLE_ID filter apply before the deduplication. Device clocks may
-- report any UTC offset; TS is normalized to UTC.
CREATE OR REPLACE VIEW TELEMETRY AS
SELECT
    PAYLOAD:message_id::VARCHAR AS MESSAGE_ID,
    PAYLOAD:vehicle_id::VARCHAR AS VEHICLE_ID,
    CONVERT_TIMEZONE('UTC', PAYLOAD:ts::TIMESTAMP_TZ)::TIMESTAMP_NTZ AS TS,
    PAYLOAD:gps.lat::FLOAT AS LAT,
    PAYLOAD:gps.lon::FLOAT AS LON,
    PAYLOAD:gps.heading_deg::INTEGER AS HEADING_DEG,
    PAYLOAD:speed_kmh::FLOAT AS SPEED_KMH,
    PAYLOAD:odometer_km::FLOAT AS ODOMETER_KM,
    PAYLOAD:fuel.level_pct::FLOAT AS FUEL_LEVEL_PCT,
    PAYLOAD:fuel.used_l::FLOAT AS FUEL_USED_L,
    PAYLOAD:engine.rpm::INTEGER AS ENGINE_RPM,
    PAYLOAD:engine.coolant_c::FLOAT AS COOLANT_C,
    COALESCE(PAYLOAD:engine.dtc::ARRAY, ARRAY_CONSTRUCT()) AS FAULT_CODES
FROM TELEMETRY_RAW
QUALIFY ROW_NUMBER() OVER (PARTITION BY VEHICLE_ID, MESSAGE_ID ORDER BY TS) = 1;

-- One row per active diagnostic trouble code (DTC) per ping.
CREATE OR REPLACE VIEW TELEMETRY_FAULTS AS
SELECT
    t.VEHICLE_ID,
    t.TS,
    f.value:code::VARCHAR AS CODE,
    f.value:severity::VARCHAR AS SEVERITY,
    t.COOLANT_C
FROM TELEMETRY t,
    LATERAL FLATTEN(INPUT => t.FAULT_CODES) f;
