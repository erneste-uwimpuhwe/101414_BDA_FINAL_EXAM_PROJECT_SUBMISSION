"""
Part 2 - Spark SQL analytics.

Registers the clean Parquet tables as SQL views and runs four analytical queries
(window functions, CTEs, multi-table joins). Query 4 shows how data that lives in
different stores could be joined in Spark SQL:
    mongo_users        -> what MongoDB holds: user profile + embedded purchase summary
    hbase_user_sessions-> what HBase holds: per-user session activity (from user_sessions)
(Part 3 loads the REAL data from MongoDB and HBase; here the views are built from the
clean Parquet tables so the SQL logic can be shown on its own.)
"""
import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
CLEAN = os.path.join(BASE, "spark", "clean")
OUT = os.path.join(BASE, "visuals")
os.makedirs(OUT, exist_ok=True)

spark = (SparkSession.builder.appName("BDA-sql").master("local[2]")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

for t in ["users", "products", "transactions", "transaction_items", "sessions", "page_views", "cart_items"]:
    spark.read.parquet(os.path.join(CLEAN, t)).createOrReplaceTempView(t)

def run(title, sql, csv=None, rows=20):
    print("\n" + "=" * 78)
    print(title)
    print("=" * 78)
    print(sql.strip())
    print("-" * 78)
    df = spark.sql(sql)
    df.show(rows, truncate=False)
    if csv:
        df.toPandas().to_csv(os.path.join(OUT, csv), index=False)

# ---------------------------------------------------------------- Q1
run("Q1: Monthly revenue, average order value and month-over-month growth (CTE + LAG window)", """
WITH monthly AS (
  SELECT txn_month,
         COUNT(*)                    AS orders,
         COUNT(DISTINCT user_id)     AS buyers,
         CAST(ROUND(SUM(total), 2) AS DECIMAL(14,2)) AS revenue
  FROM transactions
  GROUP BY txn_month
)
SELECT txn_month, orders, buyers, revenue,
       ROUND(revenue / orders, 2) AS avg_order_value,
       ROUND(100 * (revenue - LAG(revenue) OVER (ORDER BY txn_month))
                 / LAG(revenue) OVER (ORDER BY txn_month), 1) AS mom_growth_pct
FROM monthly
ORDER BY txn_month
""", "sql_monthly_revenue.csv")

# ---------------------------------------------------------------- Q2
run("Q2: Category revenue with share and cumulative share - Pareto (join + window functions)", """
WITH cat AS (
  SELECT p.category_name,
         ROUND(SUM(i.line_total), 2)              AS revenue,
         SUM(i.quantity)                          AS units,
         ROUND(SUM(i.line_total * p.avg_profit_margin), 2) AS est_profit
  FROM transaction_items i
  JOIN products p ON i.product_id = p.product_id
  GROUP BY p.category_name
)
SELECT category_name, revenue, units, est_profit,
       ROUND(100 * revenue / SUM(revenue) OVER (), 2) AS share_pct,
       ROUND(100 * SUM(revenue) OVER (ORDER BY revenue DESC) / SUM(revenue) OVER (), 2) AS cumulative_pct,
       RANK() OVER (ORDER BY est_profit DESC) AS profit_rank
FROM cat
ORDER BY revenue DESC
""", "sql_category_pareto.csv", rows=25)

# ---------------------------------------------------------------- Q3
run("Q3: Conversion rate by traffic source and device (conditional aggregation)", """
SELECT referrer, device_type,
       COUNT(*)                                                          AS sessions,
       SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END)  AS converted,
       SUM(CASE WHEN conversion_status = 'abandoned' THEN 1 ELSE 0 END)  AS abandoned,
       ROUND(100 * SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) / COUNT(*), 2) AS conversion_pct,
       ROUND(AVG(duration_seconds), 0)                                   AS avg_duration_s
FROM sessions
GROUP BY referrer, device_type
ORDER BY conversion_pct DESC
""", "sql_conversion_by_channel.csv", rows=20)

# ---------------------------------------------------------------- weekly revenue (chart data, no screen output)
spark.sql("""
SELECT date_trunc('week', txn_date) AS week_start, COUNT(*) AS orders, CAST(ROUND(SUM(total), 2) AS DECIMAL(14,2)) AS revenue
FROM transactions GROUP BY date_trunc('week', txn_date) ORDER BY week_start
""").toPandas().to_csv(os.path.join(OUT, "sql_weekly_revenue.csv"), index=False)

# ---------------------------------------------------------------- Q4 (notional multi-store join)
spark.sql("""
SELECT u.user_id, u.country, u.registration_month,
       COALESCE(t.total_orders, 0)  AS total_orders,
       COALESCE(t.total_spent, 0.0) AS total_spent
FROM users u
LEFT JOIN (SELECT user_id, COUNT(*) AS total_orders, ROUND(SUM(total), 2) AS total_spent
           FROM transactions GROUP BY user_id) t ON u.user_id = t.user_id
""").createOrReplaceTempView("mongo_users")          # = MongoDB users + embedded purchase_summary

spark.sql("""
SELECT user_id, COUNT(*) AS n_sessions, SUM(duration_seconds) AS total_duration_s,
       SUM(n_page_views) AS total_page_views, MAX(start_ts) AS last_session_ts
FROM sessions GROUP BY user_id
""").createOrReplaceTempView("hbase_user_sessions")  # = HBase user_sessions rolled up per user

run("Q4: Top customers - MongoDB profile/purchases joined with HBase session activity", """
SELECT m.user_id, m.country, m.total_orders, m.total_spent,
       h.n_sessions, h.total_page_views,
       ROUND(m.total_spent / h.n_sessions, 2) AS spend_per_session,
       NTILE(4) OVER (ORDER BY m.total_spent DESC) AS spend_quartile
FROM mongo_users m
JOIN hbase_user_sessions h ON m.user_id = h.user_id
WHERE m.total_orders > 0
ORDER BY m.total_spent DESC
""", rows=10)

print("SPARK SQL DONE")
spark.stop()
