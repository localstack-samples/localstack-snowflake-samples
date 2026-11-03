-- V1.0.0: the inventory schema as it stood in production when the team adopted
-- schemachange (tables, the reporting views of the time, and the reader role).
--
-- Every object is addressed as {{ database }}.{{ schema }}.<name>: release.py
-- renders {{ schema }} as the green candidate on a release and as the live
-- schema on the one-off bootstrap (make seed). View bodies use unqualified
-- names on purpose, so a view resolves tables in its own schema after a clone
-- or a swap (see R__inventory_views.sql).
--
-- Constraints are informational in Snowflake (only NOT NULL is enforced); they
-- document the model for people and BI tools. The release gate
-- (release/validations.sql) is what actually checks referential integrity.

CREATE ROLE IF NOT EXISTS INVENTORY_READER;
GRANT ROLE INVENTORY_READER TO ROLE SYSADMIN;
GRANT USAGE ON DATABASE {{ database }} TO ROLE INVENTORY_READER;

CREATE TABLE {{ database }}.{{ schema }}.STORES (
    STORE_ID    VARCHAR(8)   NOT NULL,
    STORE_NAME  VARCHAR(100) NOT NULL,
    REGION      VARCHAR(16)  NOT NULL,
    OPENED_ON   DATE         NOT NULL,
    CONSTRAINT PK_STORES PRIMARY KEY (STORE_ID)
);

CREATE TABLE {{ database }}.{{ schema }}.PRODUCTS (
    SKU           VARCHAR(16)   NOT NULL,
    PRODUCT_NAME  VARCHAR(200)  NOT NULL,
    CATEGORY      VARCHAR(32)   NOT NULL,
    UNIT_COST     NUMBER(10, 2) NOT NULL,
    CONSTRAINT PK_PRODUCTS PRIMARY KEY (SKU)
);

-- One row per store and SKU. LOCATION is the shelf address printed on the
-- label, '<aisle>-<bay>' (for example 'A07-B12'); NULL while unassigned.
-- ALLOCATED counts units promised to open online orders. It may exceed
-- ON_HAND: the store sells into backorder and fills it from the next delivery.
CREATE TABLE {{ database }}.{{ schema }}.STOCK_LEVELS (
    STORE_ID    VARCHAR(8)     NOT NULL,
    SKU         VARCHAR(16)    NOT NULL,
    ON_HAND     NUMBER(10, 0)  NOT NULL,
    ALLOCATED   NUMBER(10, 0)  NOT NULL DEFAULT 0,
    LOCATION    VARCHAR(16),
    UPDATED_AT  TIMESTAMP_NTZ  NOT NULL,
    CONSTRAINT PK_STOCK_LEVELS PRIMARY KEY (STORE_ID, SKU),
    CONSTRAINT FK_STOCK_LEVELS_STORE FOREIGN KEY (STORE_ID) REFERENCES {{ database }}.{{ schema }}.STORES (STORE_ID),
    CONSTRAINT FK_STOCK_LEVELS_PRODUCT FOREIGN KEY (SKU) REFERENCES {{ database }}.{{ schema }}.PRODUCTS (SKU)
);

CREATE TABLE {{ database }}.{{ schema }}.PURCHASE_ORDERS (
    PO_ID        VARCHAR(12)   NOT NULL,
    STORE_ID     VARCHAR(8)    NOT NULL,
    SKU          VARCHAR(16)   NOT NULL,
    ORDERED_QTY  NUMBER(10, 0) NOT NULL,
    STATUS       VARCHAR(10)   NOT NULL,  -- OPEN | RECEIVED | CANCELLED
    ORDERED_AT   TIMESTAMP_NTZ NOT NULL,
    EXPECTED_ON  DATE          NOT NULL,
    CONSTRAINT PK_PURCHASE_ORDERS PRIMARY KEY (PO_ID),
    CONSTRAINT FK_PURCHASE_ORDERS_STOCK FOREIGN KEY (STORE_ID, SKU)
        REFERENCES {{ database }}.{{ schema }}.STOCK_LEVELS (STORE_ID, SKU)
);

-- The stock ledger. QUANTITY is signed: SALE < 0, RECEIPT > 0 (with the PO it
-- received), ADJUSTMENT either way (counts, damage, shrink).
CREATE TABLE {{ database }}.{{ schema }}.STOCK_MOVEMENTS (
    MOVEMENT_ID    NUMBER(12, 0) NOT NULL,
    STORE_ID       VARCHAR(8)    NOT NULL,
    SKU            VARCHAR(16)   NOT NULL,
    MOVEMENT_TYPE  VARCHAR(12)   NOT NULL,  -- SALE | RECEIPT | ADJUSTMENT
    QUANTITY       NUMBER(10, 0) NOT NULL,
    PO_ID          VARCHAR(12),
    MOVED_AT       TIMESTAMP_NTZ NOT NULL,
    CONSTRAINT PK_STOCK_MOVEMENTS PRIMARY KEY (MOVEMENT_ID),
    CONSTRAINT FK_STOCK_MOVEMENTS_STOCK FOREIGN KEY (STORE_ID, SKU)
        REFERENCES {{ database }}.{{ schema }}.STOCK_LEVELS (STORE_ID, SKU),
    CONSTRAINT FK_STOCK_MOVEMENTS_PO FOREIGN KEY (PO_ID) REFERENCES {{ database }}.{{ schema }}.PURCHASE_ORDERS (PO_ID)
);

-- The reporting views of the time. R__inventory_views.sql has owned the views
-- since the team adopted schemachange.
CREATE VIEW {{ database }}.{{ schema }}.V_STOCK_POSITION AS
SELECT
    s.STORE_ID,
    st.STORE_NAME,
    s.SKU,
    p.PRODUCT_NAME,
    s.ON_HAND,
    s.ALLOCATED,
    s.ON_HAND - s.ALLOCATED AS AVAILABLE,
    s.LOCATION
FROM STOCK_LEVELS s
JOIN STORES st ON st.STORE_ID = s.STORE_ID
JOIN PRODUCTS p ON p.SKU = s.SKU;

CREATE VIEW {{ database }}.{{ schema }}.V_OPEN_PURCHASE_ORDERS AS
SELECT po.PO_ID, po.STORE_ID, po.SKU, p.PRODUCT_NAME, po.ORDERED_QTY, po.ORDERED_AT, po.EXPECTED_ON
FROM PURCHASE_ORDERS po
JOIN PRODUCTS p ON p.SKU = po.SKU
WHERE po.STATUS = 'OPEN';
