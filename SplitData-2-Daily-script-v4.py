import os
import pandas as pd
from pathlib import Path

# === CONFIG ===
source_file = "AUDUSD_tick_UTC+0_00_2004-Parse.csv"
project_root = "/home/x/Workspace/QC_Lean_Projects/SumeetTradingAlgos"
output_dir = f"{project_root}/data/forex/oanda/audusd/tick"
source_dir = os.getcwd()
split_dir = os.path.join(source_dir, "split")

# === SETUP ===
os.makedirs(output_dir, exist_ok=True)
os.makedirs(split_dir, exist_ok=True)
print(f"[INFO] Starting chunked conversion from: {source_file}")

# === READ IN CHUNKS ===
chunk_size = 1_000_000
day_buffers = {}

def flush_day_buffer(day_str, rows):
    file_path = os.path.join(split_dir, f"{day_str}.csv")
    link_path = os.path.join(output_dir, f"{day_str}.csv")
    pd.DataFrame(rows).to_csv(file_path, index=False, header=False)
    print(f"  → Wrote {len(rows)} ticks to {file_path}")

    if not os.path.exists(link_path):
        os.symlink(file_path, link_path)
        print(f"  ↪ Symlinked → {link_path}")

# === PROCESS CHUNKS ===
for chunk_i, chunk in enumerate(pd.read_csv(source_file, chunksize=chunk_size)):
    print(f"[INFO] Processing chunk {chunk_i + 1}...")

    chunk.columns = ['time', 'askprice', 'bidprice', 'asksize', 'bidsize']
    chunk['time'] = pd.to_datetime(chunk['time'])
    chunk['bidsize'] = chunk['bidsize'].astype(float).round(2)
    chunk['asksize'] = chunk['asksize'].astype(float).round(2)
    chunk['bidprice'] = chunk['bidprice'].astype(float).round(5)
    chunk['askprice'] = chunk['askprice'].astype(float).round(5)

    ##Changed iterate rows to itterate tuples:
    # for _, row in chunk.iterrows():
    #     t = row['time']
    #     day_str = t.strftime('%Y%m%d')
    #     line = [t, row['bidprice'], row['askprice'], row['bidsize'], row['asksize']]
    #     day_buffers.setdefault(day_str, []).append(line)
    
    for row in chunk.itertuples(index=False):
        t = row.time
        day_str = t.strftime('%Y%m%d')
        line = [t, row.bidprice, row.askprice, row.bidsize, row.asksize]
        day_buffers.setdefault(day_str, []).append(line)


    # Periodically flush to disk to keep memory low
    if (chunk_i + 1) % 5 == 0:
        print("[INFO] Flushing to disk...")
        for day_str, rows in day_buffers.items():
            flush_day_buffer(day_str, rows)
        day_buffers.clear()

# Final flush for any remaining data

