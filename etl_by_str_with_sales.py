
import os
import time
import json
import logging
import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
from typing import List, Dict
from tqdm import tqdm

# === Config ===
CSV_PATH = '/Users/davidrobinson/Library/Mobile Documents/com~apple~CloudDocs/Desktop/bvRealEstate/bentonville_parcels.csv'
TARGET_STR = '11-20-31'
OUTPUT_DIR = 'output'
PROGRESS_FILE = os.path.join(OUTPUT_DIR, f'{TARGET_STR}_progress.json')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, f'{TARGET_STR}.csv')

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9',
}
RETRY_LIMIT = 3
RETRY_BACKOFF = 4

# === Setup ===
os.makedirs(OUTPUT_DIR, exist_ok=True)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_progress() -> set:
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_progress(done_ids: set):
    with open(PROGRESS_FILE, 'w') as f:
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

def extract_owner_info(soup):
    owner_name = soup.find("td", string="Owner Name:")
    mailing_td = soup.find("td", string="Mailing Address:")
    result = {"owner_name": "", "owner_state": "", "owner_address": ""}
    if owner_name:
        result["owner_name"] = owner_name.find_next_sibling("td").get_text(strip=True)
    if mailing_td:
        td = mailing_td.find_next_sibling("td")
        lines = [br.previous_sibling.strip() for br in td.find_all("br") if br.previous_sibling]
        result["owner_address"] = " | ".join(lines)
        for line in lines:
            match = re.search(r"([A-Za-z]{2}) (\d{5}(?:-\d{4})?)$", line)
            if match:
                result["owner_state"] = match.group(1).upper()
                break
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

def main():
    if not os.path.exists(CSV_PATH):
        logging.error(f"CSV not found at {CSV_PATH}")
        return

    df = pd.read_csv(CSV_PATH)
    df = df[df["S_T_R"] == TARGET_STR].dropna(subset=["PARCELID", "ACRE_AREA"])

    logging.info(f"Loaded {len(df)} parcels from S_T_R {TARGET_STR}")

    completed = load_progress()

    for _, row in tqdm(df.iterrows(), total=len(df), desc=f"Processing {TARGET_STR}"):
        parcel_id = row["PARCELID"]
        if parcel_id in completed:
            continue

        html = fetch_parcel_html(parcel_id)
        if not html:
            continue

        sale_rows = parse_parcel_page(html, parcel_id, row["ACRE_AREA"])
        if sale_rows:
            append_to_csv(sale_rows, OUTPUT_FILE)

        completed.add(parcel_id)
        if len(completed) % 5 == 0:
            save_progress(completed)

        time.sleep(4)

    save_progress(completed)
    logging.info(f"Finished processing {TARGET_STR}")

if __name__ == "__main__":
    main()
