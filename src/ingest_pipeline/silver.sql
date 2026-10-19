-- =============================================================================
-- SILVER · typed, validated, analytics-ready
-- =============================================================================
-- Reads the bronze streaming tables, enforces types, and attaches data-quality
-- EXPECTATIONS (logged to the pipeline event log every run). These tables are
-- published under the PLAIN entity names — members, products, orders, order_items,
-- web_sessions — which is exactly the input contract the feature-engineering
-- notebook (src/02_feature_engineering.py) consumes. So the medallion is:
--
--     raw JSON (Volume)  ->  bronze_*  ->  <entity>  ->  member_features / recs (notebooks 02/03)
--
-- JSON has no date/timestamp type, so bronze carries dates as strings and silver
-- casts them; arrays (health_conditions, condition_tags) survive JSON round-trip.
-- =============================================================================

CREATE OR REFRESH STREAMING TABLE members
  (CONSTRAINT valid_member_id EXPECT (member_id IS NOT NULL) ON VIOLATION DROP ROW,
   CONSTRAINT plausible_age   EXPECT (age BETWEEN 0 AND 120))
  COMMENT "Members: cleaned + typed (silver)."
AS SELECT
     CAST(member_id AS STRING)         AS member_id,
     CAST(first_name AS STRING)        AS first_name,
     CAST(last_name AS STRING)         AS last_name,
     CAST(age AS INT)                  AS age,
     CAST(gender AS STRING)            AS gender,
     CAST(state AS STRING)             AS state,
     CAST(plan_type AS STRING)         AS plan_type,
     CAST(enrollment_date AS DATE)     AS enrollment_date,
     health_conditions,                                    -- array<string>
     CAST(otc_wallet_monthly AS INT)   AS otc_wallet_monthly
   FROM STREAM(bronze_members);

CREATE OR REFRESH STREAMING TABLE products
  (CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
   CONSTRAINT positive_price   EXPECT (price > 0))
  COMMENT "Products catalog: cleaned + typed (silver)."
AS SELECT
     CAST(product_id AS STRING)        AS product_id,
     CAST(product_name AS STRING)      AS product_name,
     CAST(category AS STRING)          AS category,
     CAST(subcategory AS STRING)       AS subcategory,
     CAST(vendor AS STRING)            AS vendor,
     CAST(price AS DOUBLE)             AS price,
     condition_tags,                                       -- array<string>
     CAST(is_consumable AS BOOLEAN)    AS is_consumable,
     CAST(popularity_weight AS DOUBLE) AS popularity_weight
   FROM STREAM(bronze_products);

CREATE OR REFRESH STREAMING TABLE orders
  (CONSTRAINT valid_order_id  EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW,
   CONSTRAINT valid_member_id EXPECT (member_id IS NOT NULL),
   CONSTRAINT valid_date      EXPECT (order_date IS NOT NULL))
  COMMENT "Orders: cleaned + typed (silver)."
AS SELECT
     CAST(order_id AS STRING)   AS order_id,
     CAST(member_id AS STRING)  AS member_id,
     CAST(order_date AS DATE)   AS order_date,
     CAST(channel AS STRING)    AS channel
   FROM STREAM(bronze_orders);

CREATE OR REFRESH STREAMING TABLE order_items
  (CONSTRAINT positive_qty EXPECT (quantity > 0) ON VIOLATION DROP ROW,
   CONSTRAINT nonneg_price EXPECT (unit_price >= 0))
  COMMENT "Order line-items: cleaned + typed (silver)."
AS SELECT
     CAST(order_id AS STRING)   AS order_id,
     CAST(member_id AS STRING)  AS member_id,
     CAST(product_id AS STRING) AS product_id,
     CAST(quantity AS INT)      AS quantity,
     CAST(unit_price AS DOUBLE) AS unit_price,
     CAST(order_date AS DATE)   AS order_date
   FROM STREAM(bronze_order_items);

CREATE OR REFRESH STREAMING TABLE web_sessions
  (CONSTRAINT nonneg_views EXPECT (page_views >= 0))
  COMMENT "Web sessions: cleaned + typed (silver)."
AS SELECT
     CAST(session_id AS STRING)      AS session_id,
     CAST(member_id AS STRING)       AS member_id,
     CAST(session_ts AS TIMESTAMP)   AS session_ts,
     CAST(category_viewed AS STRING) AS category_viewed,
     CAST(page_views AS INT)         AS page_views,
     CAST(device AS STRING)          AS device
   FROM STREAM(bronze_web_sessions);
