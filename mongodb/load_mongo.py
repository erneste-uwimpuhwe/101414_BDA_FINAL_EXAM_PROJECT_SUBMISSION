import json, ijson, glob
from datetime import datetime
from pymongo import MongoClient, ASCENDING

DATA = r"D:\BDA_Project\data"
db = MongoClient("mongodb://127.0.0.1:27018")["ecommerce"]
dt = datetime.fromisoformat

for c in ["categories", "products", "users", "transactions", "sessions"]:
    db[c].drop()

def load(name):
    with open(f"{DATA}\\{name}.json", encoding="utf-8") as f:
        return json.load(f)

# ---- categories ----
cats = load("categories")
for c in cats:
    c["_id"] = c["category_id"]
db.categories.insert_many(cats)
cat_lookup = {c["category_id"]: c for c in cats}
print("categories:", db.categories.count_documents({}))

# ---- products (category embedded) ----
prods = load("products")
for p in prods:
    p["_id"] = p["product_id"]
    cat = cat_lookup[p["category_id"]]
    subs = cat["subcategories"]
    p["category"] = {
        "category_id": cat["category_id"],
        "name": cat["name"],
        "avg_profit_margin": round(sum(s["profit_margin"] for s in subs) / len(subs), 3),
    }
    p["creation_date"] = dt(p["creation_date"])
    for ph in p["price_history"]:
        ph["date"] = dt(ph["date"])
db.products.insert_many(prods)
print("products:", db.products.count_documents({}))

# ---- transactions (items embedded) + user stats ----
txns = load("transactions")
stats = {}
for t in txns:
    t["_id"] = t["transaction_id"]
    t["timestamp"] = dt(t["timestamp"])
    s = stats.setdefault(t["user_id"], {"total_orders": 0, "total_spent": 0.0, "last_purchase": None})
    s["total_orders"] += 1
    s["total_spent"] = round(s["total_spent"] + t["total"], 2)
    if s["last_purchase"] is None or t["timestamp"] > s["last_purchase"]:
        s["last_purchase"] = t["timestamp"]
for i in range(0, len(txns), 5000):
    db.transactions.insert_many(txns[i:i + 5000])
print("transactions:", db.transactions.count_documents({}))

# ---- users (purchase_summary embedded) ----
users = load("users")
for u in users:
    u["_id"] = u["user_id"]
    u["registration_date"] = dt(u["registration_date"])
    u["last_active"] = dt(u["last_active"])
    u["purchase_summary"] = stats.get(u["user_id"], {"total_orders": 0, "total_spent": 0.0, "last_purchase": None})
db.users.insert_many(users)
print("users:", db.users.count_documents({}))

# ---- sessions (streamed, batched) ----
batch, n = [], 0
for fp in sorted(glob.glob(DATA + "\\sessions_*.json")):
    with open(fp, "rb") as f:
        for s in ijson.items(f, "item", use_float=True):
            s["_id"] = s["session_id"]
            s["start_time"] = dt(s["start_time"])
            s["end_time"] = dt(s["end_time"])
            batch.append(s)
            if len(batch) == 5000:
                db.sessions.insert_many(batch)
                n += len(batch)
                batch = []
                print("sessions loaded:", n)
if batch:
    db.sessions.insert_many(batch)
    n += len(batch)
print("sessions:", n)

# ---- indexes ----
db.transactions.create_index([("user_id", ASCENDING)])
db.transactions.create_index([("timestamp", ASCENDING)])
db.transactions.create_index([("items.product_id", ASCENDING)])
db.sessions.create_index([("user_id", ASCENDING), ("start_time", ASCENDING)])
db.products.create_index([("category_id", ASCENDING)])
print("indexes created. DONE")
