# === Setup ===
import os
import json
import time
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict
from tqdm import tqdm

# === Configuration ===
CHUNK_FOLDER = 'input/transformedGeoDB'
CHUNK_BASENAME = 'benton_parcels_with_coords_20250613'
CHUNK_COUNT = 3

TARGET_STRS = ['36-21-31', '01-20-31', '06-20-30', '31-21-30', '12-20-31', '07-20-30',
               '08-20-30', '09-20-30', '35-21-31', '27-21-31', '10-20-31']
OUTPUT_DIR = 'output'

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
}
RETRY_LIMIT = 3
RETRY_BACKOFF = 4

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# === Functions ===
def load_progress(filepath: str) -> set:
    if os.path.exists(filepath):
        with open(filepath, 'r') as f:
            return set(json.load(f))
    return set()

def save_progress(done_ids: set, filepath: str):
    with open(filepath, 'w') as f:
        json.dump(list(done_ids), f)

def fetch_parcel_html(parcel_id: str) -> str:
    url = f"https://www.arcountydata.com/parcel_sponsor.asp?parcelid={parcel_id}&county=Benton&AISGIS=Benton"
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.warning(f"Parcel {parcel_id} - Attempt {attempt} failed: {e}")
            if attempt < RETRY_LIMIT:
                time.sleep(RETRY_BACKOFF * attempt)
    return None

import re

def extract_owner_info(soup):
    result = {"owner_name": "", "owner_state": "", "owner_address": ""}

    try:
        # Safely get the owner name
        owner_td = soup.find("td", string="Owner Name:")
        if owner_td:
            next_td = owner_td.find_next_sibling("td")
            if next_td:
                result["owner_name"] = next_td.get_text(strip=True)
    except Exception as e:
        print(f"[WARN] Failed to extract owner name: {e}")

    try:
        # Safely get the mailing address
        mailing_td = soup.find("td", string="Mailing Address:")
        if mailing_td:
            td = mailing_td.find_next_sibling("td")
            if td:
                lines = []
                for br in td.find_all("br"):
                    sibling = br.previous_sibling
                    if sibling and isinstance(sibling, str):
                        lines.append(sibling.strip())
                result["owner_address"] = " | ".join(lines)

                # Try to extract state from one of the lines
                for line in lines:
                    match = re.search(r"([A-Za-z]{2}) (\d{5}(?:-\d{4})?)$", line)
                    if match:
                        result["owner_state"] = match.group(1).upper()
                        break
    except Exception as e:
        print(f"[WARN] Failed to extract mailing address or state: {e}")

    return result


def extract_sales_history(soup, parcel_id: str, acre_area: float, has_house: bool, owner_state: str) -> List[Dict]:
    sales_data = []
    panel = soup.find("div", class_="panel-heading", text="Sales History")
    if not panel:
        return []
    table = panel.find_next("table", class_="table-striped-yellow")
    if not table:
        return []

    for row in table.find_all("tr")[1:]:
        cols = row.find_all("td")
        if len(cols) < 3:
            continue
        try:
            sold_date = cols[1].text.strip()
            sold_price = float(cols[2].text.strip().replace(",", ""))
            deed_type = cols[-1].text.strip()
            sales_data.append({
                "PARCELID": parcel_id,
                "sold_date": sold_date,
                "sold_price": sold_price,
                "deed_type": deed_type,
                "acre_area": acre_area,
                "has_house": has_house,
                "owner_state": owner_state
            })
        except ValueError:
            continue
    return sales_data

def parse_parcel_page(html: str, parcel_id: str, acre_area: float) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    owner_info = extract_owner_info(soup)
    has_house = bool(soup.find("div", id="Improvements"))
    return extract_sales_history(soup, parcel_id, acre_area, has_house, owner_info["owner_state"])

def append_to_csv(rows: List[Dict], filepath: str):
    df = pd.DataFrame(rows)
    if os.path.exists(filepath):
        df.to_csv(filepath, mode='a', header=False, index=False)
    else:
        df.to_csv(filepath, index=False)

# === Run for Each S_T_R ===
def main():
    # Load and concatenate all chunk files
    chunk_paths = [
        os.path.join(CHUNK_FOLDER, f"{CHUNK_BASENAME}_part{i+1}.csv")
        for i in range(CHUNK_COUNT)
    ]

    dataframes = []
    for path in chunk_paths:
        if not os.path.exists(path):
            logging.error(f"Missing chunk file: {path}")
            return
        dataframes.append(pd.read_csv(path))

    df_all = pd.concat(dataframes, ignore_index=True)

    for target_str in TARGET_STRS:
        output_file = os.path.join(OUTPUT_DIR, f"{target_str}.csv")
        progress_file = os.path.join(OUTPUT_DIR, f"{target_str}_progress.json")

        df = df_all[df_all["S_T_R"] == target_str].dropna(subset=["PARCELID", "ACRE_AREA"])
        logging.info(f"Processing {len(df)} parcels in {target_str}...")

        completed = load_progress(progress_file)

        for _, row in tqdm(df.iterrows(), total=len(df), desc=f"{target_str}"):
            parcel_id = row["PARCELID"]
            if parcel_id in completed:
                continue

            html = fetch_parcel_html(parcel_id)
            if not html:
                continue

            sale_rows = parse_parcel_page(html, parcel_id, row["ACRE_AREA"])
            if sale_rows:
                append_to_csv(sale_rows, output_file)

            completed.add(parcel_id)
            if len(completed) % 5 == 0:
                save_progress(completed, progress_file)

            time.sleep(4)

        save_progress(completed, progress_file)
        logging.info(f"Finished {target_str}")
        
main()
