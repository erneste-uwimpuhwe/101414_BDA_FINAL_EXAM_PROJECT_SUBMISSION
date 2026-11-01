import os
import pandas as pd
from pymongo import MongoClient

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
OUT = os.path.join(BASE, "integration")
os.makedirs(OUT, exist_ok=True)
db = MongoClient("mongodb://127.0.0.1:27018")["ecommerce"]

# ---- users: profile + embedded summary
rows = []
for u in db.users.find({}, {"_id": 0, "user_id": 1, "geo_data": 1, "registration_date": 1, "purchase_summary": 1}):
    ps = u.get("purchase_summary", {})
    rows.append({"user_id": u["user_id"], "country": u["geo_data"]["country"], "state": u["geo_data"]["state"],
                 "registration_date": u["registration_date"],
                 "ps_total_orders": ps.get("total_orders", 0), "ps_total_spent": ps.get("total_spent", 0.0)})
users = pd.DataFrame(rows)
users.to_csv(os.path.join(OUT, "mongo_users.csv"), index=False)
print(f"mongo_users.csv: {len(users):,} users")

# ---- transactions: per-user purchase metrics via aggregation
pipeline = [
    {"$unwind": "$items"},
    {"$lookup": {"from": "products", "localField": "items.product_id", "foreignField": "_id", "as": "p"}},
    {"$unwind": "$p"},
    {"$group": {"_id": "$transaction_id", "user_id": {"$first": "$user_id"}, "ts": {"$first": "$timestamp"},
                "net_total": {"$first": "$total"},
                "profit": {"$sum": {"$multiply": ["$items.subtotal", "$p.category.avg_profit_margin"]}}}},
    {"$group": {"_id": "$user_id", "orders": {"$sum": 1}, "net_revenue": {"$sum": "$net_total"},
                "est_profit": {"$sum": "$profit"}, "first_purchase": {"$min": "$ts"},
                "last_purchase": {"$max": "$ts"}}},
    {"$project": {"_id": 0, "user_id": "$_id", "orders": 1,
                  "net_revenue": {"$round": ["$net_revenue", 2]},
                  "est_profit": {"$round": ["$est_profit", 2]},
                  "first_purchase": 1, "last_purchase": 1}},
]
txn = pd.DataFrame(list(db.transactions.aggregate(pipeline, allowDiskUse=True)))
txn.to_csv(os.path.join(OUT, "mongo_txn_metrics.csv"), index=False)
print(f"mongo_txn_metrics.csv: {len(txn):,} buyers (users with at least one order)")
print(txn.head(5).to_string(index=False))
print("MONGO EXPORT DONE")