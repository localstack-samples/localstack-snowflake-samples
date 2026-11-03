-- Daily KPIs per vehicle, kept up to date by Snowflake from the typed view.
-- Dashboards read this table instead of scanning raw telemetry. The backfill
-- command and the tests refresh it on demand with ALTER DYNAMIC TABLE ... REFRESH.

USE ROLE FLEET_APP;
USE WAREHOUSE FLEET_WH;
USE SCHEMA FLEET.TELEMETRY;

CREATE OR REPLACE DYNAMIC TABLE FLEET_DAILY_KPIS
    TARGET_LAG = '15 minutes'
    WAREHOUSE = FLEET_WH
    COMMENT = 'Daily distance, fuel, idling and fault KPIs per vehicle (UTC days)'
AS
SELECT
    TS::DATE AS DAY,
    VEHICLE_ID,
    COUNT(*) AS SAMPLES,
    COUNT_IF(SPEED_KMH = 0) AS IDLE_SAMPLES,
    MAX(ODOMETER_KM) - MIN(ODOMETER_KM) AS DISTANCE_KM,
    MAX(FUEL_USED_L) - MIN(FUEL_USED_L) AS FUEL_USED_L,
    MAX(SPEED_KMH) AS MAX_SPEED_KMH,
    COUNT_IF(ARRAY_SIZE(FAULT_CODES) > 0) AS FAULT_SAMPLES
FROM TELEMETRY
GROUP BY TS::DATE, VEHICLE_ID;
