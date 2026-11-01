import sys
from datetime import datetime, timezone
import happybase

USER = sys.argv[1] if len(sys.argv) > 1 else "user_000000"
PRODUCT = sys.argv[2] if len(sys.argv) > 2 else "prod_01476"
conn = happybase.Connection("127.0.0.1", port=9090, timeout=120000)
sess = conn.table("user_sessions")
prod = conn.table("product_daily_metrics")

def rev(iso):
    return 9999999999 - int(datetime.fromisoformat(iso).replace(tzinfo=timezone.utc).timestamp())

def show(key, data):
    d = {k.decode(): v.decode() for k, v in data.items()}
    print(key.decode(), "|", d.get("info:start_time"), "|", d.get("info:conversion_status"),
          "|", d.get("device:type"), "|", d.get("info:referrer"),
          "| page_views:", d.get("activity:page_view_count"))

print("=" * 70)
print(f"QUERY 1: 5 most recent sessions for {USER} (prefix scan, newest first)")
print("=" * 70)
for key, data in sess.scan(row_prefix=f"{USER}#".encode(), limit=5):
    show(key, data)

print("\n" + "=" * 70)
print(f"QUERY 2: sessions for {USER} in October 2026 (time-range scan)")
print("=" * 70)
start = f"{USER}#{rev('2026-10-31T23:59:59'):010d}".encode()
stop = f"{USER}#{rev('2026-10-01T00:00:00') + 1:010d}".encode()
k = 0
for key, data in sess.scan(row_start=start, row_stop=stop):
    show(key, data)
    k += 1
print("sessions in range:", k)

print("\n" + "=" * 70)
print(f"QUERY 3: all sessions of {USER} (count)")
print("=" * 70)
total = sum(1 for _ in sess.scan(row_prefix=f"{USER}#".encode(), filter=b"FirstKeyOnlyFilter()"))
print(USER, "has", total, "sessions")

print("\n" + "=" * 70)
print(f"QUERY 4: daily metrics for {PRODUCT} (first 10 days)")
print("=" * 70)
print("row key                 views  cart_adds  purchases")
for key, data in prod.scan(row_prefix=f"{PRODUCT}#".encode(), limit=10):
    print(f"{key.decode():<22} {data[b'm:views'].decode():>6} {data[b'm:cart_adds'].decode():>10} {data[b'm:purchases'].decode():>10}")