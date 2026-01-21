--standardSQL
DECLARE grain STRING DEFAULT @grain;        -- 'day' | 'week' | 'month' | 'fy'
DECLARE n_periods INT64 DEFAULT @n_periods; -- 例: 5, 60 など

WITH base AS (
  SELECT
    sales_date,
    branch_name,
    staff_code,
    staff_name,
    customer_code,
    customer_name,
    item_code,
    item_name,
    unique_code_yj,
    quantity,
    sales_amount,
    gross_profit,
    fiscal_year
  FROM `salesdb-479915.sales_data.v_sales_fact_fy_norm`
),

periodized AS (
  SELECT
    CASE
      WHEN grain = 'day'   THEN 'day'
      WHEN grain = 'week'  THEN 'week'
      WHEN grain = 'month' THEN 'month'
      WHEN grain = 'fy'    THEN 'fy'
      ELSE 'month'
    END AS period,

    CASE
      WHEN grain = 'day'   THEN sales_date
      WHEN grain = 'week'  THEN DATE_TRUNC(sales_date, WEEK(MONDAY))
      WHEN grain = 'month' THEN DATE_TRUNC(sales_date, MONTH)
      WHEN grain = 'fy'    THEN DATE(fiscal_year - 1, 4, 1)  -- FY開始日(4/1)を代表日
      ELSE DATE_TRUNC(sales_date, MONTH)
    END AS period_date,

    branch_name,
    staff_code,
    staff_name,
    customer_code,
    item_code,
    unique_code_yj,
    quantity,
    sales_amount,
    gross_profit
  FROM base
),

latest_periods AS (
  SELECT
    period,
    period_date
  FROM periodized
  GROUP BY period, period_date
  ORDER BY period_date DESC
  LIMIT n_periods
)

SELECT
  p.period,
  p.period_date,
  p.branch_name,
  p.staff_code,
  p.staff_name,

  COUNT(DISTINCT p.customer_code) AS customers,
  COUNT(DISTINCT CONCAT(p.customer_code, '|', p.unique_code_yj)) AS customer_items,
  COUNT(DISTINCT p.unique_code_yj) AS items,
  COUNT(*) AS rows,

  SUM(p.sales_amount) AS sales_sum,
  SUM(p.gross_profit) AS gp_sum,
  SAFE_DIVIDE(SUM(p.gross_profit), NULLIF(SUM(p.sales_amount), 0)) AS gross_margin,
  SUM(p.quantity) AS qty_sum

FROM periodized p
JOIN latest_periods lp
  ON p.period = lp.period
 AND p.period_date = lp.period_date

GROUP BY
  period, period_date, branch_name, staff_code, staff_name

ORDER BY
  period_date DESC, branch_name, staff_code;
