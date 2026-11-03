-- V1.1.0: a reorder point per store and SKU, so replenishment can flag low stock.
--
-- Rule: seven days of cover at the store's recent sales rate, that is
-- REORDER_POINT = CEIL(units sold in the 28 days up to the latest movement / 4).
-- The window is anchored on the newest movement rather than CURRENT_DATE so a
-- re-run (or a rehearsal on a clone weeks later) computes the same values.
-- Rows with no sales in the window get 0.

ALTER TABLE {{ database }}.{{ schema }}.STOCK_LEVELS ADD COLUMN REORDER_POINT NUMBER(10, 0);

UPDATE {{ database }}.{{ schema }}.STOCK_LEVELS SET REORDER_POINT = 0;

UPDATE {{ database }}.{{ schema }}.STOCK_LEVELS s
SET REORDER_POINT = sales.REORDER_POINT
FROM (
    SELECT m.STORE_ID, m.SKU, CEIL(SUM(-m.QUANTITY) / 4) AS REORDER_POINT
    FROM {{ database }}.{{ schema }}.STOCK_MOVEMENTS m
    WHERE m.MOVEMENT_TYPE = 'SALE'
      AND m.MOVED_AT > (SELECT DATEADD(day, -28, MAX(MOVED_AT)) FROM {{ database }}.{{ schema }}.STOCK_MOVEMENTS)
    GROUP BY m.STORE_ID, m.SKU
) sales
WHERE s.STORE_ID = sales.STORE_ID AND s.SKU = sales.SKU;

ALTER TABLE {{ database }}.{{ schema }}.STOCK_LEVELS ALTER COLUMN REORDER_POINT SET NOT NULL;
