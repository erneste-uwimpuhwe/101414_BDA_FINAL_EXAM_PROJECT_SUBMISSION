"""
Step 4A - Convert the chunked sessions_*.json (one huge JSON array per file) into
JSON Lines (one session per line). A huge single-array file must be read by ONE Spark
task (multiLine=true); JSON Lines is splittable, so Spark can read it in parallel
and stay within memory on a small machine.
"""
import glob, json, os
import ijson

BASE = os.environ.get("BDA_BASE", r"D:\BDA_Project")
DATA = os.path.join(BASE, "data")
out_path = os.path.join(DATA, "sessions.jsonl")

n = 0
with open(out_path, "w", encoding="utf-8") as out:
    for fp in sorted(glob.glob(os.path.join(DATA, "sessions_*.json"))):
        with open(fp, "rb") as f:
            for s in ijson.items(f, "item", use_float=True):
                out.write(json.dumps(s, separators=(",", ":")) + "\n")
                n += 1
print(f"sessions.jsonl written: {n:,} sessions -> {out_path}")
