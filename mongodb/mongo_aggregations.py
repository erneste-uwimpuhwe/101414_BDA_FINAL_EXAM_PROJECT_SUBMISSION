import os
import pandas as pd
from pymongo import MongoClient

OUT = r"D:\BDA_Project\visuals"
os.makedirs(OUT, exist_ok=True)
db = MongoClient("mongodb://127.0.0.1:27018")["ecommerce"]
pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 20)

# PIPELINE 1: Top-selling products ($unwind items -> $group -> $lookup -> $sort)
pipeline_top_products = [
    {"$unwind": "$items"},
    {"$group": {
        "_id": "$items.product_id",
        "units_sold": {"$sum": "$items.quantity"},
        "revenue": {"$sum": "$items.subtotal"},
        "orders": {"$sum": 1},
    }},
    {"$lookup": {"from": "products", "localField": "_id",
                 "foreignField": "_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$project": {
        "_id": 0,
        "product_id": "$_id",
        "product_name": "$product.name",
        "category": "$product.category.name",
        "units_sold": 1,
        "orders": 1,
        "revenue": {"$round": ["$revenue", 2]},
    }},
    {"$sort": {"units_sold": -1, "revenue": -1}},
    {"$limit": 10},
]

# PIPELINE 2: Revenue and estimated profit by category (uses embedded margin)
pipeline_category_revenue = [
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id",
                 "foreignField": "_id", "as": "product"}},
    {"$unwind": "$product"},
    {"$group": {
        "_id": {"category_id": "$product.category.category_id",
                "category": "$product.category.name"},
        "revenue": {"$sum": "$items.subtotal"},
        "units_sold": {"$sum": "$items.quantity"},
        "est_profit": {"$sum": {"$multiply": [
            "$items.subtotal", "$product.category.avg_profit_margin"]}},
        "distinct_products": {"$addToSet": "$items.product_id"},
    }},
    {"$project": {
        "_id": 0,
        "category_id": "$_id.category_id",
        "category": "$_id.category",
        "revenue": {"$round": ["$revenue", 2]},
        "est_profit": {"$round": ["$est_profit", 2]},
        "units_sold": 1,
        "products_sold": {"$size": "$distinct_products"},
    }},
    {"$sort": {"revenue": -1}},
]

# PIPELINE 3 (bonus): User segmentation by purchasing frequency
pipeline_segmentation = [
    {"$project": {
        "total_orders": "$purchase_summary.total_orders",
        "total_spent": "$purchase_summary.total_spent",
        "segment": {"$switch": {
            "branches": [
                {"case": {"$eq": ["$purchase_summary.total_orders", 0]}, "then": "1_No purchase"},
                {"case": {"$lte": ["$purchase_summary.total_orders", 2]}, "then": "2_Occasional (1-2)"},
                {"case": {"$lte": ["$purchase_summary.total_orders", 5]}, "then": "3_Regular (3-5)"},
            ],
            "default": "4_Loyal (6+)",
        }},
    }},
    {"$group": {
        "_id": "$segment",
        "users": {"$sum": 1},
        "avg_orders": {"$avg": "$total_orders"},
        "avg_spent": {"$avg": "$total_spent"},
        "total_spent": {"$sum": "$total_spent"},
    }},
    {"$project": {
        "_id": 0,
        "segment": "$_id",
        "users": 1,
        "avg_orders": {"$round": ["$avg_orders", 2]},
        "avg_spent": {"$round": ["$avg_spent", 2]},
        "total_spent": {"$round": ["$total_spent", 2]},
    }},
    {"$sort": {"segment": 1}},
]

def run(title, collection, pipeline, csv_name):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)
    df = pd.DataFrame(list(db[collection].aggregate(pipeline, allowDiskUse=True)))
    print(df.to_string(index=False))
    df.to_csv(os.path.join(OUT, csv_name), index=False)
    print(f"-> saved {csv_name} ({len(df)} rows)")

if __name__ == "__main__":
    run("PIPELINE 1: Top 10 best-selling products", "transactions",
        pipeline_top_products, "mongo_top_products.csv")
    run("PIPELINE 2: Revenue and estimated profit by category", "transactions",
        pipeline_category_revenue, "mongo_category_revenue.csv")
    run("PIPELINE 3: User segmentation by purchase frequency", "users",
        pipeline_segmentation, "mongo_user_segments.csv")