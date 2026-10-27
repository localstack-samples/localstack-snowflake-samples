-- Re-applied on every deploy (schemachange "always" script, runs last).
--
-- Grants are why this script exists: CREATE SCHEMA ... CLONE does not copy
-- grants on the schema itself, and ALTER SCHEMA ... SWAP WITH keeps each
-- schema's grants with the schema object. Without this script the swapped-in
-- schema would come up without USAGE for INVENTORY_READER and every report
-- would fail at the moment of the release.

GRANT USAGE ON SCHEMA {{ database }}.{{ schema }} TO ROLE INVENTORY_READER;
GRANT SELECT ON ALL VIEWS IN SCHEMA {{ database }}.{{ schema }} TO ROLE INVENTORY_READER;
