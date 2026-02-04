import json
from collections import defaultdict
from pathlib import Path

data_path = Path("data/batch.json")
out = Path("import")
out.mkdir(exist_ok=True)

print("📦 Loading JSON...")
data = json.load(data_path.open())

# ----------------------------------------
# 1) UNIQUE ITEMS → SEQUENTIAL IDS
# ----------------------------------------
items = set()
for r in data:
    items.add(r["item_id"])
    if r.get("next_item_id"):
        items.add(r["next_item_id"])

items_list = sorted(items)
id_map = {item_id: i for i, item_id in enumerate(items_list)}

print(f"📊 {len(items_list)} unique items")

# ----------------------------------------
# 2) AGGREGATE NEXT
# ----------------------------------------
next_agg = defaultdict(lambda: {"weight": 0, "sessions": set()})

for r in data:
    if r.get("next_item_id"):
        i1 = id_map[r["item_id"]]
        i2 = id_map[r["next_item_id"]]

        k = (i1, i2)
        next_agg[k]["weight"] += 1

        if r.get("session_id"):
            next_agg[k]["sessions"].add(str(r["session_id"]))

print(f"📊 NEXT edges aggregated: {len(next_agg)}")

# ----------------------------------------
# 3) SESSIONS + CONTAINS
# ----------------------------------------
sessions = sorted({r["session_id"] for r in data if r.get("session_id")})
session_map = {sid: i for i, sid in enumerate(sessions)}

contains = set()
for r in data:
    if r.get("session_id"):
        contains.add(
            (session_map[r["session_id"]], id_map[r["item_id"]])
        )

print(f"📊 Sessions: {len(sessions)}")
print(f"📊 CONTAINS: {len(contains)}")

# ========================================
# WRITE CSV (NEO4J-ADMIN FORMAT)
# ========================================

# ---- ITEMS ----
with open(out / "items.csv", "w") as f:
    f.write(":ID(Item)\t:LABEL\n")
    for i in range(len(items_list)):
        f.write(f"{i}\tItem\n")

# ---- SESSIONS ----
with open(out / "sessions.csv", "w") as f:
    f.write(":ID(Session)\t:LABEL\n")
    for i in range(len(sessions)):
        f.write(f"{i}\tSession\n")

# ---- NEXT ----
with open(out / "next.csv", "w") as f:
    f.write(":START_ID(Item)\t:END_ID(Item)\tweight:int\tsessions:string[]\n")

    for (i1, i2), v in next_agg.items():
        sess = ";".join(v["sessions"])      # ← NEO4J ARRAY FORMAT
        f.write(f"{i1}\t{i2}\t{v['weight']}\t{sess}\n")

# ---- CONTAINS ----
with open(out / "contains.csv", "w") as f:
    f.write(":START_ID(Session)\t:END_ID(Item)\n")

    for s_id, i_id in contains:
        f.write(f"{s_id}\t{i_id}\n")

# ---- LOOKUP ----
with open(out / "lookup.json", "w") as f:
    json.dump({
        "item_to_id": id_map,
        "session_to_id": session_map
    }, f)

print("✅ BULK FILES READY")
print(f"• items.csv: {len(items_list)}")
print(f"• sessions.csv: {len(sessions)}")
print(f"• next.csv: {len(next_agg)}")
print(f"• contains.csv: {len(contains)}")
