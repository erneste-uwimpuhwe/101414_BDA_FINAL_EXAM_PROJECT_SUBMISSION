import glob, json
from datetime import datetime, timezone
import happybase, ijson

DATA = r"D:\BDA_Project\data"
conn = happybase.Connection("127.0.0.1", port=9090, timeout=120000)
sessions_tbl = conn.table("user_sessions")
metrics_tbl = conn.table("product_daily_metrics")

def epoch(iso):
    return int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp())

def b(v):
    return str(v).encode("utf-8")

metrics = {}  # (product_id, yyyymmdd) -> [views, cart_adds, purchases]
def bump(pid, day, idx, n=1):
    metrics.setdefault((pid, day), [0, 0, 0])[idx] += n

# ---------------- sessions ----------------
n = 0
batch = sessions_tbl.batch(batch_size=2000)
for fp in sorted(glob.glob(DATA + "\\sessions_*.json")):
    with open(fp, "rb") as f:
        for s in ijson.items(f, "item", use_float=True):
            rev = 9999999999 - epoch(s["start_time"])
            row = f'{s["user_id"]}#{rev:010d}'.encode()
            day = s["start_time"][:10].replace("-", "")
            views = [pv["product_id"] for pv in s["page_views"]
                     if pv["page_type"] == "product_detail" and pv["product_id"]]
            for pid in views:
                bump(pid, day, 0)
            for pid in s.get("cart_contents", {}):
                bump(pid, day, 1)
            batch.put(row, {
                b"info:session_id": b(s["session_id"]),
                b"info:start_time": b(s["start_time"]),
                b"info:end_time": b(s["end_time"]),
                b"info:duration_seconds": b(s["duration_seconds"]),
                b"info:conversion_status": b(s["conversion_status"]),
                b"info:referrer": b(s["referrer"]),
                b"info:city": b(s["geo_data"]["city"]),
                b"info:state": b(s["geo_data"]["state"]),
                b"info:country": b(s["geo_data"]["country"]),
                b"device:type": b(s["device_profile"]["type"]),
                b"device:os": b(s["device_profile"]["os"]),
                b"device:browser": b(s["device_profile"]["browser"]),
                b"activity:page_view_count": b(len(s["page_views"])),
                b"activity:viewed_products": b(",".join(s["viewed_products"])),
                b"activity:cart_contents": b(json.dumps(s["cart_contents"])),
            })
            n += 1
            if n % 10000 == 0:
                print("sessions loaded:", n, flush=True)
batch.send()
print("sessions loaded (total):", n)

# ---------------- purchases from transactions ----------------
with open(DATA + "\\transactions.json", encoding="utf-8") as f:
    for t in json.load(f):
        day = t["timestamp"][:10].replace("-", "")
        for it in t["items"]:
            bump(it["product_id"], day, 2, it["quantity"])

# ---------------- product daily metrics ----------------
mb = metrics_tbl.batch(batch_size=2000)
for (pid, day), (v, c, p) in metrics.items():
    mb.put(f"{pid}#{day}".encode(),
           {b"m:views": b(v), b"m:cart_adds": b(c), b"m:purchases": b(p)})
mb.send()
print("product_daily_metrics rows:", len(metrics))
print("HBASE LOAD DONE")