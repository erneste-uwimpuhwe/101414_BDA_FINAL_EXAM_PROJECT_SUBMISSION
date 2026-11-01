import csv, os
import happybase

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
OUT = os.path.join(BASE, "integration")
os.makedirs(OUT, exist_ok=True)

conn = happybase.Connection("127.0.0.1", port=9090, timeout=600000)
table = conn.table("user_sessions")
cols = [b"info:duration_seconds", b"info:conversion_status", b"info:start_time", b"activity:page_view_count"]

agg, n = {}, 0
for key, data in table.scan(columns=cols, batch_size=2000):
    uid = key.split(b"#")[0].decode()
    a = agg.setdefault(uid, {"n_sessions": 0, "total_duration_s": 0, "total_page_views": 0,
                             "converted_sessions": 0, "first_session": "9999", "last_session": ""})
    a["n_sessions"] += 1
    a["total_duration_s"] += int(data[b"info:duration_seconds"])
    a["total_page_views"] += int(data[b"activity:page_view_count"])
    if data[b"info:conversion_status"] == b"converted":
        a["converted_sessions"] += 1
    st = data[b"info:start_time"].decode()
    a["first_session"] = min(a["first_session"], st)
    a["last_session"] = max(a["last_session"], st)
    n += 1
    if n % 20000 == 0:
        print("session rows scanned:", n, flush=True)

path = os.path.join(OUT, "hbase_session_metrics.csv")
with open(path, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["user_id", "n_sessions", "total_duration_s", "total_page_views",
                "converted_sessions", "first_session", "last_session"])
    for uid, a in agg.items():
        w.writerow([uid, a["n_sessions"], a["total_duration_s"], a["total_page_views"],
                    a["converted_sessions"], a["first_session"], a["last_session"]])
print(f"session rows scanned (total): {n:,}")
print(f"hbase_session_metrics.csv: {len(agg):,} users with sessions")
print("HBASE EXPORT DONE")