import os
import pandas as pd
import logging
from datetime import datetime

# === Configuration ===
CHUNK_FOLDER = "input/transformedGeoDB"
CHUNK_BASENAME = "benton_parcels_with_coords_20250613"
CHUNK_COUNT = 3

SALES_DIR = "output"
REPORTING_DIR = os.path.join(SALES_DIR, "reporting")
OUTPUT_PATH = os.path.join(REPORTING_DIR, "final_looker_ready_report.csv")

TARGET_STRS = ['36-21-31', '01-20-31', '06-20-30', '31-21-30', '12-20-31',
               '07-20-30', '08-20-30', '09-20-30', '35-21-31', '27-21-31', '10-20-31']

# === Logging Setup ===
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("process_log.log"),
        logging.StreamHandler()
    ]
)

# === Helper Functions ===
def load_csv_safe(path: str, desc: str = "") -> pd.DataFrame:
    try:
        df = pd.read_csv(path)
        logging.info(f"{desc}Loaded: {path} with {len(df)} rows.")
        return df
    except Exception as e:
        logging.error(f"Failed to load {desc}{path}: {e}")
        return pd.DataFrame()

# === Step 1: Load Chunked Parcel Files ===
parcel_dfs = []
for i in range(CHUNK_COUNT):
    chunk_path = os.path.join(CHUNK_FOLDER, f"{CHUNK_BASENAME}_part{i+1}.csv")
    df = load_csv_safe(chunk_path, desc=f"Parcel chunk {i+1}: ")
    if not df.empty:
        parcel_dfs.append(df)

if not parcel_dfs:
    raise SystemExit("❌ No parcel chunks could be loaded. Exiting.")

parcel_df = pd.concat(parcel_dfs, ignore_index=True)

# === Step 2: Load and Combine Sales Files ===
sales_frames = []
for str_code in TARGET_STRS:
    file_path = os.path.join(SALES_DIR, f"{str_code}.csv")
    df = load_csv_safe(file_path, desc=f"Sales data [{str_code}]: ")
    if not df.empty:
        df["S_T_R"] = str_code
        sales_frames.append(df)

if not sales_frames:
    raise SystemExit("❌ No sales data files were loaded. Exiting.")

sales_df = pd.concat(sales_frames, ignore_index=True)

# === Step 3: Filter to Warranty Deeds and Get Most Recent per Parcel ===
try:
    sales_df["sold_date"] = pd.to_datetime(sales_df["sold_date"], errors="coerce")
    wd_sales = sales_df[sales_df["deed_type"].str.startswith("WD", na=False)].copy()
    wd_latest = wd_sales.sort_values("sold_date").groupby("PARCELID", as_index=False).last()
    logging.info(f"Filtered to {len(wd_latest)} most recent warranty deed sales.")
except Exception as e:
    logging.error(f"Failed to process warranty deed filtering: {e}")
    raise SystemExit("❌ Error during deed filtering. Exiting.")

# === Step 4: Join to Parcel Data ===
try:
    merged = parcel_df.merge(wd_latest, on="PARCELID", how="inner", suffixes=("", "_sale"))
    logging.info(f"Merged result has {len(merged)} records.")
except Exception as e:
    logging.error(f"Merge failed: {e}")
    raise SystemExit("❌ Merge failed. Exiting.")

# === Step 5: Create BI Fields ===
try:
    merged["sale_to_land_value_ratio"] = merged["sold_price"] / merged["LAND_VAL"].replace({0: None})
    merged["out_of_state_owner"] = merged["owner_state"].str.upper() != "AR"
    merged["sale_year"] = pd.DatetimeIndex(merged["sold_date"]).year
    logging.info("Computed derived fields.")
except Exception as e:
    logging.error(f"Failed to compute derived fields: {e}")

# === Step 6: Save Final Output to reporting/ Folder ===
try:
    os.makedirs(REPORTING_DIR, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)
    logging.info(f"✅ Final dataset saved to: {OUTPUT_PATH}")
except Exception as e:
    logging.error(f"Failed to save output: {e}")
    raise SystemExit("❌ Failed to write final report. Exiting.")
