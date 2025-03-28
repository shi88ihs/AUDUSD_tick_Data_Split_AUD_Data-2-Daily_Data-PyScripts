import os
import pandas as pd
from pathlib import Path

# === SETTINGS ===
source_file = "AUDUSD_tick_UTC+0_00_2004-Parse.csv"
project_root = "/home/x/Workspace/QC_Lean_Projects/SumeetTradingAlgos"
output_dir = f"{project_root}/data/forex/oanda/audusd/tick"
source_dir = os.getcwd()
split_dir = os.path.join(source_dir, "split")

print(f"[INFO] Starting conversion of: {source_file}")
print(f"[INFO] Output symlinks to: {output_dir}")
print(f"[INFO] Split files stored in: {split_dir}")

# === CREATE OUTPUT DIRS ===
os.makedirs(output_dir, exist_ok=True)
os.makedirs(split_dir, exist_ok=True)

# === LOAD & FORMAT CSV ===
print("[INFO] Loading CSV...")
df = pd.read_csv(source_file)
df.columns = ['time', 'askprice', 'bidprice', 'asksize', 'bidsize']

# Clean and reformat
df['time'] = pd.to_datetime(df['time'])
df['bidsize'] = df['bidsize'].astype(float).round(2)
df['asksize'] = df['asksize'].astype(float).round(2)
df['bidprice'] = df['bidprice'].astype(float).round(5)
df['askprice'] = df['askprice'].astype(float).round(5)

# Reorder for QC tick format
df = df[['time', 'bidprice', 'askprice', 'bidsize', 'asksize']]
print(f"[INFO] Loaded {len(df)} tick rows.")

# === SPLIT BY DAY AND SYMLINK ===
grouped = df.groupby(df['time'].dt.date)
print(f"[INFO] Splitting into {len(grouped)} day files...")

for i, (day, group) in enumerate(grouped):
    date_str = pd.to_datetime(day).strftime("%Y%m%d")
    file_name = f"{date_str}.csv"
    split_path = os.path.join(split_dir, file_name)
    link_path = os.path.join(output_dir, file_name)

    group.to_csv(split_path, index=False, header=False)
    print(f"  → Saved split: {file_name} ({len(group)} rows)")

    if not os.path.exists(link_path):
        os.symlink(split_path, link_path)
        print(f"  ↪ Symlinked → {link_path}")
    else:
        print(f"  ↪ [Skipped] Symlink already exists: {link_path}")

print("[✅ DONE] All files converted and linked.")

