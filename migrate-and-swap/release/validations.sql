-- The release gate. release.py runs every check against the migrated green
-- candidate before the swap; one failing check stops the release with exit
-- code 1 and live is never touched.
--
-- Format: each check starts with a "-- check: <name>" line, followed by
-- "-- " description lines and one query. A check PASSES when its query returns
-- no rows; every returned row is a violation. A query that fails to run (a
-- dropped table, a broken view) fails its check with the Snowflake error.
--
-- The file is a Jinja template. Context:
--   live, green                fully qualified schemas, e.g. RETAIL.INVENTORY
--   database, live_schema,     the same, unqualified, for INFORMATION_SCHEMA
--   green_schema
--   live_tables                base tables in live (change history excluded)
--   green_views                views in green

-- check: row_count_parity
-- A schema release must not add or lose rows: every table live has keeps
-- its row count in green. Also catches writes that reached live after the
-- clone was taken (they would be lost by the swap).
{% for t in live_tables %}
SELECT '{{ t }}' AS TABLE_NAME, LIVE_ROWS, GREEN_ROWS
FROM (
    SELECT
        (SELECT COUNT(*) FROM {{ live }}.{{ t }}) AS LIVE_ROWS,
        (SELECT COUNT(*) FROM {{ green }}.{{ t }}) AS GREEN_ROWS
)
WHERE LIVE_ROWS <> GREEN_ROWS
{% if not loop.last %}UNION ALL{% endif %}
{% endfor %};

-- check: no_negative_on_hand
-- Physical stock cannot be negative. Backorders are carried in ALLOCATED.
SELECT STORE_ID, SKU, ON_HAND
FROM {{ green }}.STOCK_LEVELS
WHERE ON_HAND < 0
ORDER BY STORE_ID, SKU;

-- check: no_orphan_movements
-- Every ledger movement belongs to a stock row (store and SKU).
SELECT m.MOVEMENT_ID, m.STORE_ID, m.SKU
FROM {{ green }}.STOCK_MOVEMENTS m
LEFT JOIN {{ green }}.STOCK_LEVELS s ON s.STORE_ID = m.STORE_ID AND s.SKU = m.SKU
WHERE s.SKU IS NULL
ORDER BY m.MOVEMENT_ID;

-- check: receipts_match_purchase_orders
-- Every RECEIPT movement references a purchase order that exists.
SELECT m.MOVEMENT_ID, m.PO_ID
FROM {{ green }}.STOCK_MOVEMENTS m
LEFT JOIN {{ green }}.PURCHASE_ORDERS po ON po.PO_ID = m.PO_ID
WHERE m.MOVEMENT_TYPE = 'RECEIPT' AND po.PO_ID IS NULL
ORDER BY m.MOVEMENT_ID;

-- check: no_view_dropped
-- Every view that live serves still exists in green: reports and the
-- INVENTORY_READER grants depend on the names.
SELECT TABLE_NAME FROM {{ database }}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{{ live_schema }}'
MINUS
SELECT TABLE_NAME FROM {{ database }}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = '{{ green_schema }}';

{% for v in green_views %}
-- check: view_selectable[{{ v }}]
-- The view compiles and runs against green's tables. COUNT(*) makes the
-- engine expand and execute the whole view; the outer filter never matches.
SELECT VIEW_ROWS FROM (SELECT COUNT(*) AS VIEW_ROWS FROM {{ green }}.{{ v }}) WHERE VIEW_ROWS < 0;
{% endfor %}
