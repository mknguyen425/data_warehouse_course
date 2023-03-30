WITH fact_sales_order_line__source AS (
  SELECT *
  FROM `vit-lam-data.wide_world_importers.sales__order_lines`
)

, fact_sales_order_line__rename_column AS(
  SELECT
   order_line_id  AS sales_order_line_key, 
   order_id AS sales_order_key,
   quantity,
   unit_price, 
   stock_item_id  AS product_key,
  FROM fact_sales_order_line__source
)

, fact_sales_order_line__cast_type AS(
SELECT 
   CAST(sales_order_line_key AS INTEGER) AS sales_order_line_key, 
   CAST(sales_order_key AS INTEGER) AS sales_order_key,
   CAST(quantity AS INTEGER) AS quantity,
   CAST(unit_price AS NUMERIC) AS unit_price, 
   CAST(product_key AS INTEGER) AS product_key,
FROM fact_sales_order_line__rename_column
)

, fact_sales_order_line__cal AS(
  SELECT *,
  quantity * unit_price AS gross_amount
  FROM fact_sales_order_line__cast_type
)

SELECT
  fact_line.sales_order_line_key,
  fact_line.sales_order_key,
  fact_line.quantity,
  fact_line.unit_price,
  fact_header.customer_key,
  fact_line.product_key,
  fact_line.gross_amount
FROM fact_sales_order_line__cal AS fact_line
LEFT JOIN {{ ref('stg_fact_sales_order')}} as fact_header
ON fact_line.sales_order_key = fact_header.sales_order_key
ORDER BY fact_line.gross_amount DESC


