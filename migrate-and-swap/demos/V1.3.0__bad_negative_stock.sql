-- V1.3.0 (deliberately bad, for the demo): "report sellable stock".
--
-- The intent: ON_HAND should mean units a shopper can buy, so subtract the
-- units already allocated to open online orders and clear the allocation.
-- It passes review and passes on a staging data set where every store has
-- more stock than allocations. On production data it does not: a store that
-- sold into backorder has ALLOCATED > ON_HAND, and this statement drives
-- ON_HAND negative for exactly those rows.
--
-- The release gate's no_negative_on_hand check catches it on the green clone,
-- and the swap never happens.

UPDATE {{ database }}.{{ schema }}.STOCK_LEVELS
SET ON_HAND = ON_HAND - ALLOCATED,
    ALLOCATED = 0;
