import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession, functions as F, Window

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
IN = os.path.join(BASE, "integration")
OUT = os.path.join(BASE, "visuals")
os.makedirs(OUT, exist_ok=True)

spark = (SparkSession.builder.appName("BDA-CLV").master("local[2]")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
rd = lambda f: spark.read.option("header", True).csv(os.path.join(IN, f))

users = (rd("mongo_users.csv")
         .select("user_id", "country", "state", F.try_to_timestamp("registration_date").alias("registration_ts"),
                 F.col("ps_total_orders").cast("int").alias("ps_total_orders"),
                 F.col("ps_total_spent").cast("double").alias("ps_total_spent")))
txn = (rd("mongo_txn_metrics.csv")
       .select("user_id", F.col("orders").cast("int").alias("orders"),
               F.col("net_revenue").cast("double").alias("net_revenue"),
               F.col("est_profit").cast("double").alias("est_profit"),
               F.try_to_timestamp("first_purchase").alias("first_purchase"),
               F.try_to_timestamp("last_purchase").alias("last_purchase")))
ses = (rd("hbase_session_metrics.csv")
       .select("user_id", F.col("n_sessions").cast("int").alias("n_sessions"),
               F.col("total_duration_s").cast("long").alias("total_duration_s"),
               F.col("total_page_views").cast("long").alias("total_page_views"),
               F.col("converted_sessions").cast("int").alias("converted_sessions"),
               F.try_to_timestamp("last_session").alias("last_session")))

# ---- observation window (days of transaction history) used to annualise
w = txn.agg(F.min("first_purchase").alias("a"), F.max("last_purchase").alias("b")).first()
obs_days = max((w["b"] - w["a"]).days + 1, 1)
factor = 365.0 / obs_days
print(f"Observation window: {obs_days} days  -> annualisation factor {factor:.2f}")

# ---- JOIN: MongoDB profile + MongoDB purchases + HBase sessions (left joins keep non-buyers)
df = (users.join(txn, "user_id", "left").join(ses, "user_id", "left")
      .fillna({"orders": 0, "net_revenue": 0.0, "est_profit": 0.0, "n_sessions": 0, "total_duration_s": 0,
               "total_page_views": 0, "converted_sessions": 0})
      .withColumn("aov", F.when(F.col("orders") > 0, F.col("net_revenue") / F.col("orders")).otherwise(0.0))
      .withColumn("clv_12m_revenue", F.round(F.col("net_revenue") * factor, 2))
      .withColumn("clv_12m_profit", F.round(F.col("est_profit") * factor, 2))
      .withColumn("avg_session_min", F.when(F.col("n_sessions") > 0,
                                            F.col("total_duration_s") / F.col("n_sessions") / 60).otherwise(0.0))
      .withColumn("pages_per_session", F.when(F.col("n_sessions") > 0,
                                              F.col("total_page_views") / F.col("n_sessions")).otherwise(0.0))
      .withColumn("session_conv_rate", F.when(F.col("n_sessions") > 0,
                                              F.col("converted_sessions") / F.col("n_sessions")).otherwise(0.0)))

# ---- data validation: embedded MongoDB summary vs aggregation pipeline
bad = df.filter((F.col("orders") > 0) & (F.abs(F.col("ps_total_spent") - F.col("net_revenue")) > 0.01)).count()
print(f"Validation - users where embedded purchase_summary != pipeline revenue: {bad}")

# ---- CLV tiers (buyers only, quartiles by projected 12-month revenue)
buyers = df.filter(F.col("orders") > 0)
tiered = (buyers.withColumn("q", F.ntile(4).over(Window.orderBy(F.desc("clv_12m_revenue"))))
          .withColumn("clv_tier", F.when(F.col("q") == 1, "1_Platinum").when(F.col("q") == 2, "2_Gold")
                      .when(F.col("q") == 3, "3_Silver").otherwise("4_Bronze")).drop("q"))
nonbuyers = df.filter(F.col("orders") == 0).withColumn("clv_tier", F.lit("5_No purchase"))
final = tiered.unionByName(nonbuyers)

summary = (final.groupBy("clv_tier")
           .agg(F.count("*").alias("users"),
                F.round(F.avg("orders"), 2).alias("avg_orders"),
                F.round(F.avg("aov"), 2).alias("avg_order_value"),
                F.round(F.avg("clv_12m_revenue"), 2).alias("avg_clv_12m_revenue"),
                F.round(F.avg("clv_12m_profit"), 2).alias("avg_clv_12m_profit"),
                F.round(F.avg("n_sessions"), 2).alias("avg_sessions"),
                F.round(F.avg("avg_session_min"), 1).alias("avg_session_min"),
                F.round(F.avg("pages_per_session"), 1).alias("pages_per_session"))
           .orderBy("clv_tier"))
print("\n" + "=" * 90)
print("CLV TIERS: projected 12-month value and browsing engagement per tier")
print("=" * 90)
summary.show(truncate=False)
summary.toPandas().to_csv(os.path.join(OUT, "clv_tier_summary.csv"), index=False)

# ---- does engagement explain value?
c = buyers.agg(F.round(F.corr("n_sessions", "clv_12m_revenue"), 3).alias("sessions_vs_clv"),
               F.round(F.corr("avg_session_min", "clv_12m_revenue"), 3).alias("session_minutes_vs_clv"),
               F.round(F.corr("session_conv_rate", "clv_12m_revenue"), 3).alias("session_conv_rate_vs_clv"),
               F.round(F.corr("orders", "n_sessions"), 3).alias("orders_vs_sessions")).first()
print("Correlation with projected 12-month revenue (buyers only):")
print(f"  number of sessions        : {c['sessions_vs_clv']}")
print(f"  average session minutes   : {c['session_minutes_vs_clv']}")
print(f"  session conversion rate   : {c['session_conv_rate_vs_clv']}")
print(f"  orders vs sessions        : {c['orders_vs_sessions']}")

# ---- top customers
print("\n" + "=" * 90)
print("TOP 10 CUSTOMERS by projected 12-month revenue")
print("=" * 90)
(final.filter(F.col("orders") > 0).orderBy(F.desc("clv_12m_revenue"))
 .select("user_id", "country", "orders", "aov", "clv_12m_revenue", "clv_12m_profit", "n_sessions",
         F.round("avg_session_min", 1).alias("avg_session_min"), "clv_tier")
 .withColumn("aov", F.round("aov", 2)).show(10, truncate=False))

# ---- high-engagement visitors who never bought
p75 = final.approxQuantile("n_sessions", [0.75], 0.01)[0]
hot = final.filter((F.col("orders") == 0) & (F.col("n_sessions") >= p75))
print("=" * 90)
print(f"HIGH-ENGAGEMENT NON-BUYERS (0 orders, sessions >= {p75:.0f}, the top quartile): {hot.count():,} users")
print("=" * 90)
hot.orderBy(F.desc("n_sessions"), F.desc("total_page_views")) \
   .select("user_id", "country", "n_sessions", "total_page_views", F.round("avg_session_min", 1).alias("avg_session_min")) \
   .show(5, truncate=False)

final.drop("registration_ts", "first_purchase", "last_purchase", "last_session") \
     .toPandas().to_csv(os.path.join(OUT, "clv_by_user.csv"), index=False)
print("SPARK CLV DONE")
spark.stop()