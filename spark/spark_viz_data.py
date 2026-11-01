"""
Part 4 - prepare the two datasets the charts need that do not exist yet:
  1) conversion funnel (sessions reaching each stage)
  2) product page views vs units sold (all products)
"""
import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession, functions as F

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
CLEAN = os.path.join(BASE, "spark", "clean")
OUT = os.path.join(BASE, "visuals")
os.makedirs(OUT, exist_ok=True)

spark = (SparkSession.builder.appName("BDA-viz").master("local[2]")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
rd = lambda n: spark.read.parquet(os.path.join(CLEAN, n))

sessions, pv, cart = rd("sessions"), rd("page_views"), rd("cart_items")

# ---------------- funnel: number of sessions that reached each stage
total = sessions.count()
viewed = pv.filter(F.col("page_type") == "product_detail").select("session_id").distinct()
carted = cart.select("session_id").distinct()
checkout = pv.filter(F.col("page_type").isin("checkout", "confirmation")).select("session_id").distinct()
bought = sessions.filter(F.col("conversion_status") == "converted").select("session_id")

# a session counts for a stage only if it also passed every earlier stage
s2 = viewed
s3 = carted.join(s2, "session_id")
s4 = checkout.join(s3, "session_id")
s5 = bought.join(s4, "session_id")
stages = [("1. All sessions", total), ("2. Viewed a product", s2.count()),
          ("3. Added to cart", s3.count()), ("4. Reached checkout", s4.count()),
          ("5. Purchased", s5.count())]
rows, prev = [], None
for name, n in stages:
    rows.append((name, n, round(100 * n / total, 2), None if prev is None else round(100 * n / prev, 2)))
    prev = n
funnel = spark.createDataFrame(rows, ["stage", "sessions", "pct_of_all_sessions", "pct_of_previous_stage"])
print("\nCONVERSION FUNNEL (sessions that passed every earlier stage)")
funnel.show(truncate=False)
funnel.toPandas().to_csv(os.path.join(OUT, "funnel.csv"), index=False)

# ---------------- product views vs units sold
views = (pv.filter((F.col("page_type") == "product_detail") & F.col("product_id").isNotNull())
         .groupBy("product_id").agg(F.count("*").alias("views")))
sold = rd("transaction_items").groupBy("product_id").agg(F.sum("quantity").alias("units_sold"))
prod = (rd("products").select("product_id", "name")
        .join(views, "product_id", "left").join(sold, "product_id", "left")
        .fillna({"views": 0, "units_sold": 0}))
print("PRODUCTS: views vs units sold (top 5 by units)")
prod.orderBy(F.desc("units_sold")).show(5, truncate=40)
prod.toPandas().to_csv(os.path.join(OUT, "product_views_vs_sales.csv"), index=False)
print("SPARK VIZ DATA DONE")
spark.stop()
