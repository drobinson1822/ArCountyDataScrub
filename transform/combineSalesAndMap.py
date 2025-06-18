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
               '07-20-30', '08-20-30', '09-20-30', '35-21-31', '27-21-31', '10-20-31', '11-20-31']

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

# === Step 3: Filter for Most Recent Sale Logic ===
try:
    sales_df["sold_date"] = pd.to_datetime(sales_df["sold_date"], errors="coerce")

    # 1. Most recent sale with "Warranty" in deed_type (case insensitive) and price > 0
    warranty_sales = sales_df[
        sales_df["deed_type"].str.contains("warranty", case=False, na=False) &
        (sales_df["sold_price"] > 0)
    ].copy()
    latest_warranty = warranty_sales.sort_values("sold_date").groupby("PARCELID", as_index=False).last()
    latest_warranty = latest_warranty.rename(columns={
        "sold_date": "warranty_sold_date",
        "sold_price": "warranty_sold_price",
        "deed_type": "warranty_deed_type"
    })

    # 2. Most recent record of any type
    latest_any = sales_df.sort_values("sold_date").groupby("PARCELID", as_index=False).last()

    # 3. Merge and prefer warranty deed
    merged_sales = latest_any.merge(
        latest_warranty[["PARCELID", "warranty_sold_date", "warranty_sold_price", "warranty_deed_type"]],
        on="PARCELID", how="left"
    )

    merged_sales["final_sold_date"] = merged_sales["warranty_sold_date"].combine_first(merged_sales["sold_date"])
    merged_sales["final_sold_price"] = merged_sales["warranty_sold_price"].combine_first(merged_sales["sold_price"])
    merged_sales["final_deed_type"] = merged_sales["warranty_deed_type"].combine_first(merged_sales["deed_type"])

    final_sales = merged_sales[[
        "PARCELID", "final_sold_date", "final_sold_price", "final_deed_type",
        "acre_area", "has_house", "owner_state"
    ]].rename(columns={
        "final_sold_date": "sold_date",
        "final_sold_price": "sold_price",
        "final_deed_type": "deed_type"
    })

    logging.info(f"Prepared {len(final_sales)} most recent sales (Warranty-preferred) for merging.")
except Exception as e:
    logging.error(f"Failed to process deed preference logic: {e}")
    raise SystemExit("❌ Error during preferred deed logic. Exiting.")

# === Step 5: Join final sales with parcel info ===
try:
    merged = parcel_df.merge(final_sales, on="PARCELID", how="inner", suffixes=("", "_sale"))
    logging.info(f"Merged parcel data with sales — final row count: {len(merged)}")

    # Compute BI fields on merged result
    merged["sale_to_land_value_ratio"] = merged["sold_price"] / merged["LAND_VAL"].replace({0: None})
    merged["out_of_state_owner"] = merged["owner_state"].str.upper() != "AR"
    merged["sale_year"] = pd.DatetimeIndex(merged["sold_date"]).year
    logging.info("Computed derived fields.")

    def safe_latlng(row):
        if pd.notnull(row['lat']) and pd.notnull(row['lon']):
            try:
                return f"{round(row['lat'], 6)},{round(row['lon'], 6)}"
            except Exception:
                return ""
        return ""

    merged["latlngCoords"] = merged.apply(safe_latlng, axis=1)
    logging.info("Created latlng field for mapping.")

    def format_latlng(row):
        if pd.notnull(row["lat"]) and pd.notnull(row["lon"]):
            try:
                if -90 <= row["lat"] <= 90 and -180 <= row["lon"] <= 180:
                    return f"{row['lat']},{row['lon']}"
            except Exception as e:
                logging.warning(f"Invalid lat/lon on row: {row.name} — {e}")
        return ""

    merged["latlng"] = merged.apply(format_latlng, axis=1)

    valid_count = (merged["latlng"] != "").sum()
    logging.info(f"✅ Created 'latlng' values for {valid_count:,} parcels.")
except Exception as e:
    logging.error(f"Failed during merging and BI field creation: {e}")
    raise SystemExit("❌ Merge or BI computation failed. Exiting.")

# === Step 6: Save Final Output to reporting/ Folder ===
try:
    os.makedirs(REPORTING_DIR, exist_ok=True)
    merged.to_csv(OUTPUT_PATH, index=False)
    logging.info(f"✅ Final dataset saved to: {OUTPUT_PATH}")
except Exception as e:
    logging.error(f"Failed to save output: {e}")
    raise SystemExit("❌ Failed to write final report. Exiting.")
