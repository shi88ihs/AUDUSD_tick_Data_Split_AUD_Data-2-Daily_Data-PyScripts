import os
import pandas as pd
from pathlib import Path

# Paths
source_file = "AUDUSD_tick_UTC+0_00_2004-Parse.csv"
project_root = "/home/x/Workspace/QC_Lean_Projects/SumeetTradingAlgos"
output_dir = f"{project_root}/data/forex/oanda/audusd/tick"
source_dir = os.getcwd()

# Create QC data output dir
os.makedirs(output_dir, exist_ok=True)

# Load and format data
df = pd.read_csv(source_file)
df.columns = ['time', 'askprice', 'bidprice', 'asksize', 'bidsize']

# Reorder & round as needed
df['time'] = pd.to_datetime(df['time'])
df['bidsize'] = df['bidsize'].astype(float).round(2)
df['asksize'] = df['asksize'].astype(float).round(2)
df['bidprice'] = df['bidprice'].astype(float).round(5)
df['askprice'] = df['askprice'].astype(float).round(5)

# Rearranged column order for QC: time,bidprice,askprice,bidsize,asksize
df = df[['time', 'bidprice', 'askprice', 'bidsize', 'asksize']]

# Split by day
for day, group in df.groupby(df['time'].dt.date):
    date_str = pd.to_datetime(day).strftime("%Y%m%d")
    file_name = f"{date_str}.csv"
    daily_path = os.path.join(source_dir, "split", file_name)
    output_path = os.path.join(output_dir, file_name)

    os.makedirs(os.path.dirname(daily_path), exist_ok=True)
    group.to_csv(daily_path, index=False, header=False)

    # Create symlink
    if not os.path.exists(output_path):
        os.symlink(daily_path, output_path)

