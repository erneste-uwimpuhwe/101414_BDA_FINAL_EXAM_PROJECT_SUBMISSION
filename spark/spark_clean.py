"""
Part 2 - Spark batch job 1: clean and normalise the raw dataset.

Input : data/users.json, products.json, categories.json, transactions.json, sessions.jsonl
Output: spark/clean/<table>.parquet  (typed, de-duplicated, validated, flattened tables)
        + a data-quality report (raw rows vs clean rows, rejected rows per rule)
"""
import os, sys
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
DATA = os.path.join(BASE, "data")
CLEAN = os.path.join(BASE, "spark", "clean")

spark = (SparkSession.builder.appName("BDA-clean")
         .master("local[2]")
         .config("spark.driver.memory", "2g")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.sql.session.timeZone", "UTC")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# ---------------- explicit schemas (faster than inference; cart_contents is a MAP
# because its keys are product ids -> inferring a struct would create 5,000 columns) ----
geo = StructType([StructField("city", StringType()), StructField("state", StringType()),
                  StructField("country", StringType()), StructField("ip_address", StringType())])
users_schema = StructType([StructField("user_id", StringType()), StructField("geo_data", geo),
                           StructField("registration_date", StringType()),
                           StructField("last_active", StringType())])
price_hist = ArrayType(StructType([StructField("price", DoubleType()), StructField("date", StringType())]))
products_schema = StructType([StructField("product_id", StringType()), StructField("name", StringType()),
                              StructField("category_id", StringType()), StructField("base_price", DoubleType()),
                              StructField("current_stock", LongType()), StructField("is_active", BooleanType()),
                              StructField("price_history", price_hist), StructField("creation_date", StringType())])
categories_schema = StructType([StructField("category_id", StringType()), StructField("name", StringType()),
                                StructField("subcategories", ArrayType(StructType([
                                    StructField("subcategory_id", StringType()), StructField("name", StringType()),
                                    StructField("profit_margin", DoubleType())])))])
items_schema = ArrayType(StructType([StructField("product_id", StringType()), StructField("quantity", LongType()),
                                     StructField("unit_price", DoubleType()), StructField("subtotal", DoubleType())]))
txn_schema = StructType([StructField("transaction_id", StringType()), StructField("session_id", StringType()),
                         StructField("user_id", StringType()), StructField("timestamp", StringType()),
                         StructField("items", items_schema), StructField("subtotal", DoubleType()),
                         StructField("discount", DoubleType()), StructField("total", DoubleType()),
                         StructField("payment_method", StringType()), StructField("status", StringType())])
pv = ArrayType(StructType([StructField("timestamp", StringType()), StructField("page_type", StringType()),
                           StructField("product_id", StringType()), StructField("category_id", StringType()),
                           StructField("view_duration", LongType())]))
sess_schema = StructType([StructField("session_id", StringType()), StructField("user_id", StringType()),
                          StructField("start_time", StringType()), StructField("end_time", StringType()),
                          StructField("duration_seconds", LongType()), StructField("geo_data", geo),
                          StructField("device_profile", StructType([StructField("type", StringType()),
                                                                     StructField("os", StringType()),
                                                                     StructField("browser", StringType())])),
                          StructField("viewed_products", ArrayType(StringType())), StructField("page_views", pv),
                          StructField("cart_contents", MapType(StringType(), StructType([
                              StructField("quantity", LongType()), StructField("price", DoubleType())]))),
                          StructField("conversion_status", StringType()), StructField("referrer", StringType())])

def ts(c):  # safe cast: bad strings become NULL instead of failing the job (Spark 4 is ANSI by default)
    return F.try_to_timestamp(F.col(c))

report = []
def track(name, raw, clean_df, rules=None):
    rows_raw, rows_clean = raw.count(), clean_df.count()
    report.append((name, rows_raw, rows_clean, rows_raw - rows_clean))
    clean_df.write.mode("overwrite").parquet(os.path.join(CLEAN, name))

# ------------------------------------------------------------------ users
raw = spark.read.schema(users_schema).option("multiLine", True).json(os.path.join(DATA, "users.json"))
users = (raw.filter(F.col("user_id").isNotNull()).dropDuplicates(["user_id"])
         .select("user_id",
                 F.coalesce(F.trim("geo_data.city"), F.lit("UNKNOWN")).alias("city"),
                 F.upper(F.trim("geo_data.state")).alias("state"),
                 F.upper(F.trim("geo_data.country")).alias("country"),
                 ts("registration_date").alias("registration_ts"),
                 ts("last_active").alias("last_active_ts"))
         .filter(F.col("registration_ts").isNotNull())
         .withColumn("registration_month", F.date_format("registration_ts", "yyyy-MM")))
track("users", raw, users)

# ------------------------------------------------------------------ categories (+ flattened subcategories)
raw = spark.read.schema(categories_schema).option("multiLine", True).json(os.path.join(DATA, "categories.json"))
cats = raw.dropDuplicates(["category_id"]).select("category_id", F.col("name").alias("category_name"))
track("categories", raw, cats)
subcats = (raw.select("category_id", F.explode("subcategories").alias("s"))
           .select("category_id", "s.subcategory_id", F.col("s.name").alias("subcategory_name"),
                   F.col("s.profit_margin").alias("profit_margin"))
           .filter(F.col("profit_margin").between(0, 1)))
subcats.write.mode("overwrite").parquet(os.path.join(CLEAN, "subcategories"))
avg_margin = subcats.groupBy("category_id").agg(F.round(F.avg("profit_margin"), 3).alias("avg_profit_margin"))

# ------------------------------------------------------------------ products (+ price history)
raw = spark.read.schema(products_schema).option("multiLine", True).json(os.path.join(DATA, "products.json"))
products = (raw.filter(F.col("product_id").isNotNull() & (F.col("base_price") > 0) & (F.col("current_stock") >= 0))
            .dropDuplicates(["product_id"])
            .withColumn("is_active", F.coalesce("is_active", F.lit(False)))
            .withColumn("creation_ts", ts("creation_date"))
            .join(cats, "category_id", "left").join(avg_margin, "category_id", "left")
            .select("product_id", "name", "category_id", "category_name", "avg_profit_margin",
                    "base_price", "current_stock", "is_active", "creation_ts"))
track("products", raw, products)
price_history = (raw.select("product_id", F.explode("price_history").alias("p"))
                 .select("product_id", F.col("p.price").alias("price"),
                         F.try_to_timestamp(F.col("p.date")).alias("price_ts")))
price_history.write.mode("overwrite").parquet(os.path.join(CLEAN, "price_history"))

# ------------------------------------------------------------------ transactions (+ line items)
raw = spark.read.schema(txn_schema).option("multiLine", True).json(os.path.join(DATA, "transactions.json"))
txn = (raw.filter(F.col("transaction_id").isNotNull() & F.col("user_id").isNotNull()
                  & (F.col("total") >= 0) & (F.size("items") > 0))
       .dropDuplicates(["transaction_id"])
       .withColumn("txn_ts", ts("timestamp"))
       .filter(F.col("txn_ts").isNotNull())
       .withColumn("status", F.lower(F.trim("status")))
       .withColumn("payment_method", F.lower(F.trim("payment_method")))
       .withColumn("totals_consistent", F.abs(F.col("subtotal") - F.col("discount") - F.col("total")) < 0.02)
       .withColumn("txn_date", F.to_date("txn_ts"))
       .withColumn("txn_month", F.date_format("txn_ts", "yyyy-MM")))
track("transactions", raw, txn.drop("items", "timestamp"))
items = (txn.select("transaction_id", "user_id", "txn_date", "txn_month", "status", F.explode("items").alias("i"))
         .select("transaction_id", "user_id", "txn_date", "txn_month", "status", "i.product_id",
                 F.col("i.quantity").alias("quantity"), F.col("i.unit_price").alias("unit_price"),
                 F.col("i.subtotal").alias("line_total"))
         .filter((F.col("quantity") > 0) & (F.col("unit_price") > 0)))
items.write.mode("overwrite").parquet(os.path.join(CLEAN, "transaction_items"))

# ------------------------------------------------------------------ sessions (+ page views + cart items)
raw = spark.read.schema(sess_schema).json(os.path.join(DATA, "sessions.jsonl"))
sess = (raw.filter(F.col("session_id").isNotNull() & F.col("user_id").isNotNull())
        .dropDuplicates(["session_id"])
        .withColumn("start_ts", ts("start_time")).withColumn("end_ts", ts("end_time"))
        .filter(F.col("start_ts").isNotNull() & (F.col("end_ts") >= F.col("start_ts")) & (F.col("duration_seconds") > 0))
        .withColumn("session_date", F.to_date("start_ts"))
        .withColumn("session_hour", F.hour("start_ts"))
        .withColumn("device_type", F.lower("device_profile.type")).withColumn("os", F.lower("device_profile.os"))
        .withColumn("browser", F.lower("device_profile.browser"))
        .withColumn("referrer", F.lower("referrer"))
        .withColumn("n_page_views", F.size("page_views"))
        .withColumn("n_products_viewed", F.size("viewed_products"))
        .withColumn("n_cart_items", F.size(F.map_keys("cart_contents"))))
sess_flat = sess.select("session_id", "user_id", "start_ts", "end_ts", "session_date", "session_hour",
                        "duration_seconds", F.col("geo_data.country").alias("country"),
                        "device_type", "os", "browser", "referrer", "conversion_status",
                        "n_page_views", "n_products_viewed", "n_cart_items")
track("sessions", raw, sess_flat)
(sess.select("session_id", "user_id", "session_date", F.explode("page_views").alias("p"))
 .select("session_id", "user_id", "session_date", F.try_to_timestamp(F.col("p.timestamp")).alias("view_ts"),
         F.col("p.page_type").alias("page_type"), F.col("p.product_id").alias("product_id"),
         F.col("p.category_id").alias("category_id"), F.col("p.view_duration").alias("view_duration"))
 .filter(F.col("view_duration") >= 0)
 .write.mode("overwrite").parquet(os.path.join(CLEAN, "page_views")))
(sess.select("session_id", "user_id", "session_date", F.explode("cart_contents").alias("product_id", "c"))
 .select("session_id", "user_id", "session_date", "product_id",
         F.col("c.quantity").alias("quantity"), F.col("c.price").alias("price"))
 .write.mode("overwrite").parquet(os.path.join(CLEAN, "cart_items")))

# ------------------------------------------------------------------ data-quality report
print("\n" + "=" * 64)
print("DATA QUALITY REPORT")
print("=" * 64)
spark.createDataFrame(report, ["table", "raw_rows", "clean_rows", "rejected"]).show(truncate=False)
bad = txn.filter(~F.col("totals_consistent")).count()
print("transactions where subtotal - discount != total:", bad)
print("Clean tables written to:", CLEAN)
print("SPARK CLEAN DONE")
spark.stop()
