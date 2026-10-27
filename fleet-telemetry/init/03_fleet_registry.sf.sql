-- Fleet registry: depots and vehicles. In production this is synced from the
-- fleet management system; here it is reference data. The telemetry generator
-- reads it to decide which vehicles to simulate and where they start.

USE ROLE FLEET_APP;
USE WAREHOUSE FLEET_WH;
USE SCHEMA FLEET.TELEMETRY;

CREATE TABLE IF NOT EXISTS DEPOTS (
    DEPOT_ID VARCHAR(8) NOT NULL,
    NAME VARCHAR(64) NOT NULL,
    LAT FLOAT NOT NULL,
    LON FLOAT NOT NULL,
    CONSTRAINT PK_DEPOTS PRIMARY KEY (DEPOT_ID)
);

CREATE TABLE IF NOT EXISTS VEHICLES (
    VEHICLE_ID VARCHAR(16) NOT NULL,
    REGISTRATION VARCHAR(16) NOT NULL,
    MAKE VARCHAR(32) NOT NULL,
    MODEL VARCHAR(32) NOT NULL,
    VEHICLE_CLASS VARCHAR(16) NOT NULL,
    FUEL_TANK_L NUMBER(5, 0) NOT NULL,
    DEPOT_ID VARCHAR(8) NOT NULL,
    CONSTRAINT PK_VEHICLES PRIMARY KEY (VEHICLE_ID),
    CONSTRAINT FK_VEHICLES_DEPOT FOREIGN KEY (DEPOT_ID) REFERENCES DEPOTS (DEPOT_ID)
);

INSERT OVERWRITE INTO DEPOTS (DEPOT_ID, NAME, LAT, LON)
SELECT column1, column2, column3, column4 FROM VALUES
    ('RTM', 'Rotterdam Waalhaven', 51.8890, 4.4310),
    ('ANR', 'Antwerp Noorderlaan', 51.2600, 4.3600),
    ('DUI', 'Duisburg Logport', 51.3870, 6.6900);

INSERT OVERWRITE INTO VEHICLES (VEHICLE_ID, REGISTRATION, MAKE, MODEL, VEHICLE_CLASS, FUEL_TANK_L, DEPOT_ID)
SELECT column1, column2, column3, column4, column5, column6, column7 FROM VALUES
    ('TRK-101', '12-BXR-4', 'DAF', 'XF 480', 'tractor', 845, 'RTM'),
    ('TRK-102', '31-BXT-7', 'Scania', 'R 450', 'tractor', 700, 'RTM'),
    ('TRK-103', '58-BZD-2', 'Volvo', 'FH 460', 'tractor', 720, 'RTM'),
    ('TRK-104', '76-BZF-9', 'DAF', 'CF 410', 'rigid', 430, 'RTM'),
    ('TRK-105', '83-BZH-1', 'Volvo', 'FL 280', 'rigid', 300, 'RTM'),
    ('VAN-106', 'V-402-KP', 'Mercedes-Benz', 'Sprinter 317', 'van', 93, 'RTM'),
    ('VAN-107', 'V-417-KR', 'Ford', 'Transit 350', 'van', 80, 'RTM'),
    ('VAN-108', 'V-433-KS', 'Renault', 'Master 165', 'van', 80, 'RTM'),
    ('TRK-201', '1-XKD-118', 'DAF', 'XF 480', 'tractor', 845, 'ANR'),
    ('TRK-202', '1-XKF-207', 'MAN', 'TGX 18.470', 'tractor', 760, 'ANR'),
    ('TRK-203', '1-XLA-334', 'Scania', 'R 450', 'tractor', 700, 'ANR'),
    ('TRK-204', '1-XLC-409', 'Renault', 'D Wide', 'rigid', 400, 'ANR'),
    ('TRK-205', '1-XLE-552', 'MAN', 'TGL 12.250', 'rigid', 220, 'ANR'),
    ('VAN-206', '1-VNA-611', 'Mercedes-Benz', 'Sprinter 317', 'van', 93, 'ANR'),
    ('VAN-207', '1-VNC-728', 'Iveco', 'Daily 35S16', 'van', 90, 'ANR'),
    ('VAN-208', '1-VNE-845', 'Ford', 'Transit 350', 'van', 80, 'ANR'),
    ('TRK-301', 'DU-FL 3011', 'Mercedes-Benz', 'Actros 1848', 'tractor', 780, 'DUI'),
    ('TRK-302', 'DU-FL 3022', 'MAN', 'TGX 18.470', 'tractor', 760, 'DUI'),
    ('TRK-303', 'DU-FL 3033', 'Volvo', 'FH 460', 'tractor', 720, 'DUI'),
    ('TRK-304', 'DU-FL 3044', 'Mercedes-Benz', 'Atego 1530', 'rigid', 290, 'DUI'),
    ('TRK-305', 'DU-FL 3055', 'DAF', 'CF 410', 'rigid', 430, 'DUI'),
    ('VAN-306', 'DU-FL 3066', 'Volkswagen', 'Crafter 35', 'van', 75, 'DUI'),
    ('VAN-307', 'DU-FL 3077', 'Mercedes-Benz', 'Sprinter 317', 'van', 93, 'DUI'),
    ('VAN-308', 'DU-FL 3088', 'Iveco', 'Daily 35S16', 'van', 90, 'DUI');
