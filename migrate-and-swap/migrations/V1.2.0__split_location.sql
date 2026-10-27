-- V1.2.0: split the shelf address LOCATION ('A07-B12') into AISLE and BAY, so
-- pick lists can sort by aisle. Older handheld scanners stored some addresses
-- in lower case or with stray spaces ('a07-b12 '): the backfill normalises them.
-- Unassigned rows (LOCATION IS NULL) keep NULL aisle and bay.
--
-- This is a destructive change: LOCATION is dropped once the backfill is done.
-- On the green clone that is safe to rehearse; done in place on live it could
-- only be undone with time travel.
--
-- Views are dropped before structural changes and rebuilt by the repeatable
-- script, so every release leaves them consistent: V_STOCK_POSITION reads
-- LOCATION, so it is dropped first and recreated in its new shape by
-- R__inventory_views.sql in the same release (schemachange runs repeatable
-- scripts after all versioned ones). On green, nobody queries the view in
-- between.

DROP VIEW IF EXISTS {{ database }}.{{ schema }}.V_STOCK_POSITION;

ALTER TABLE {{ database }}.{{ schema }}.STOCK_LEVELS ADD COLUMN AISLE VARCHAR(4);
ALTER TABLE {{ database }}.{{ schema }}.STOCK_LEVELS ADD COLUMN BAY VARCHAR(4);

UPDATE {{ database }}.{{ schema }}.STOCK_LEVELS
SET AISLE = SPLIT_PART(UPPER(TRIM(LOCATION)), '-', 1),
    BAY = SPLIT_PART(UPPER(TRIM(LOCATION)), '-', 2)
WHERE LOCATION IS NOT NULL;

ALTER TABLE {{ database }}.{{ schema }}.STOCK_LEVELS DROP COLUMN LOCATION;
