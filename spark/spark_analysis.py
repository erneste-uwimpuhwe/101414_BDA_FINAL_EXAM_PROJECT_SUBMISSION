import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession, functions as F

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
CLEAN = os.path.join(BASE, "spark", "clean")
OUT = os.path.join(BASE, "visuals")
os.makedirs(OUT, exist_ok=True)

spark = (SparkSession.builder.appName("BDA-analysis").master("local[2]")
         .config("spark.driver.memory", "2g").config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
rd = lambda n: spark.read.parquet(os.path.join(CLEAN, n))
products = rd("products").select("product_id", "name")

def pair_stats(baskets, basket_col, label, min_pair=2):
    """baskets: DataFrame(basket_col, product_id) with ONE row per distinct product per basket."""
    n_baskets = baskets.select(basket_col).distinct().count()
    single = baskets.groupBy("product_id").agg(F.countDistinct(basket_col).alias("n"))
    a, b = baskets.alias("a"), baskets.alias("b")
    pairs = (a.join(b, (F.col(f"a.{basket_col}") == F.col(f"b.{basket_col}")) &
                    (F.col("a.product_id") < F.col("b.product_id")))
             .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
             .agg(F.count("*").alias("pair_count"))
             .filter(F.col("pair_count") >= min_pair))
    res = (pairs.join(single.alias("sa"), F.col("product_a") == F.col("sa.product_id")).drop("product_id")
           .withColumnRenamed("n", "n_a")
           .join(single.alias("sb"), F.col("product_b") == F.col("sb.product_id")).drop("product_id")
           .withColumnRenamed("n", "n_b")
           .withColumn("confidence_a_to_b", F.round(F.col("pair_count") / F.col("n_a"), 4))
           .withColumn("lift", F.round((F.col("pair_count") * n_baskets) / (F.col("n_a") * F.col("n_b")), 2))
           .join(products.withColumnRenamed("product_id", "product_a").withColumnRenamed("name", "name_a"), "product_a")
           .join(products.withColumnRenamed("product_id", "product_b").withColumnRenamed("name", "name_b"), "product_b")
           .select("product_a", "name_a", "product_b", "name_b", "pair_count", "n_a", "n_b",
                   "confidence_a_to_b", "lift")
           .orderBy(F.desc("pair_count"), F.desc("lift")))
    print("\n" + "=" * 70)
    print(f"A) Top product pairs: {label}  (baskets analysed: {n_baskets:,})")
    print("=" * 70)
    res.limit(10).show(truncate=30)
    return res

# A1: viewed together (sessions)
views = (rd("page_views").filter((F.col("page_type") == "product_detail") & F.col("product_id").isNotNull())
         .select("session_id", "product_id").distinct())
viewed = pair_stats(views, "session_id", "viewed together in the same session")
viewed.limit(50).toPandas().to_csv(os.path.join(OUT, "spark_viewed_together.csv"), index=False)

# A2: bought together (transactions)
bought_base = rd("transaction_items").select("transaction_id", "product_id").distinct()
bought = pair_stats(bought_base, "transaction_id", "bought together in the same order", min_pair=1)
bought.limit(50).toPandas().to_csv(os.path.join(OUT, "spark_bought_together.csv"), index=False)

# B: cohort analysis
users = rd("users").select("user_id", "registration_month")
cohort_size = users.groupBy("registration_month").agg(F.count("*").alias("cohort_users"))
txn = rd("transactions").filter(F.col("status") != "cancelled").select("user_id", "txn_month", "total")
joined = (txn.join(users, "user_id")
          .withColumn("month_index",
                      F.round(F.months_between(F.to_date(F.concat("txn_month", F.lit("-01"))),
                                               F.to_date(F.concat("registration_month", F.lit("-01")))), 0).cast("int")))
cohort = (joined.groupBy("registration_month", "month_index")
          .agg(F.round(F.sum("total"), 2).alias("revenue"),
               F.countDistinct("user_id").alias("active_users"),
               F.count("*").alias("orders"))
          .join(cohort_size, "registration_month")
          .withColumn("revenue_per_cohort_user", F.round(F.col("revenue") / F.col("cohort_users"), 2))
          .withColumn("active_pct", F.round(100 * F.col("active_users") / F.col("cohort_users"), 1))
          .orderBy("registration_month", "month_index"))
print("\n" + "=" * 70)
print("B) Cohort analysis: revenue per user by registration cohort and months since registration")
print("=" * 70)
cohort.select("registration_month", "cohort_users", "month_index", "active_users", "active_pct",
              "revenue", "revenue_per_cohort_user").show(60, truncate=False)
cohort.toPandas().to_csv(os.path.join(OUT, "spark_cohort.csv"), index=False)

print("SPARK ANALYSIS DONE")
spark.stop()