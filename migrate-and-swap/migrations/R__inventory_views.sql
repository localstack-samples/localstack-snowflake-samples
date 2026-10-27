-- The reporting views, owned by this repeatable script. schemachange re-runs it
-- whenever its content changes, after all versioned scripts of the release.
--
-- View bodies reference tables WITHOUT a database or schema. Snowflake
-- resolves unqualified names in a view against the view's own schema, so the
-- same view reads green's tables while it lives in green and live's tables
-- after the swap. A fully qualified body (FROM RETAIL.INVENTORY.STOCK_LEVELS)
-- would keep pointing at whatever schema carries that name, which after a
-- swap is the other side.

CREATE OR REPLACE VIEW {{ database }}.{{ schema }}.V_STOCK_POSITION AS
SELECT
    s.STORE_ID,
    st.STORE_NAME,
    s.SKU,
    p.PRODUCT_NAME,
    s.ON_HAND,
    s.ALLOCATED,
    s.ON_HAND - s.ALLOCATED AS AVAILABLE,
    s.REORDER_POINT,
    s.AISLE,
    s.BAY
FROM STOCK_LEVELS s
JOIN STORES st ON st.STORE_ID = s.STORE_ID
JOIN PRODUCTS p ON p.SKU = s.SKU;

-- Rows at or below their reorder point with nothing already on order.
CREATE OR REPLACE VIEW {{ database }}.{{ schema }}.V_REORDER_CANDIDATES AS
SELECT s.STORE_ID, s.SKU, s.ON_HAND - s.ALLOCATED AS AVAILABLE, s.REORDER_POINT, s.AISLE, s.BAY
FROM STOCK_LEVELS s
WHERE s.ON_HAND - s.ALLOCATED <= s.REORDER_POINT
  AND s.REORDER_POINT > 0
  AND NOT EXISTS (
      SELECT 1 FROM PURCHASE_ORDERS po
      WHERE po.STORE_ID = s.STORE_ID AND po.SKU = s.SKU AND po.STATUS = 'OPEN'
  );

CREATE OR REPLACE VIEW {{ database }}.{{ schema }}.V_OPEN_PURCHASE_ORDERS AS
SELECT po.PO_ID, po.STORE_ID, po.SKU, p.PRODUCT_NAME, po.ORDERED_QTY, po.ORDERED_AT, po.EXPECTED_ON
FROM PURCHASE_ORDERS po
JOIN PRODUCTS p ON p.SKU = po.SKU
WHERE po.STATUS = 'OPEN';
