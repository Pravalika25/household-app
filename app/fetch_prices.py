import os
import requests
import csv
from dotenv import load_dotenv
from datetime import date
import time

# Load env variables
load_dotenv()
API_KEY = os.getenv("RAPIDAPI_KEY")

HEADERS = {
    "X-RapidAPI-Key": API_KEY,
}

WALMART_HOST = "walmart-data.p.rapidapi.com"
TARGET_HOST = "target1.p.rapidapi.com"

# Retry helper
def get_json_with_retry(url, headers, params, retries=3, delay=5):
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=10)
            r.raise_for_status()  # HTTP errors (like 503) will raise exception
            return r.json()
        except requests.exceptions.RequestException as e:
            print(f"Request error ({attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                return None
        except ValueError as e:
            print(f"Invalid JSON response ({attempt+1}/{retries})")
            return None

def fetch_walmart_price(query):
    url = f"https://{WALMART_HOST}/search"
    params = {"query": query, "page": "1"}
    data = get_json_with_retry(url, {**HEADERS, "X-RapidAPI-Host": WALMART_HOST}, params)
    if data and "items" in data and data["items"]:
        item = data["items"][0]
        return {
            "store": "Walmart",
            "name": item["title"],
            "price": item.get("price", {}).get("current_price") or item.get("salePrice"),
            "url": item.get("product_page_url") or item.get("link"),
        }
    else:
        print(f"No Walmart data for '{query}'")
        return None

def fetch_target_price(query):
    url = f"https://{TARGET_HOST}/search"
    params = {"q": query}
    data = get_json_with_retry(url, {**HEADERS, "X-RapidAPI-Host": TARGET_HOST}, params)
    if data and "data" in data and "search" in data["data"] and "products" in data["data"]["search"] and data["data"]["search"]["products"]:
        item = data["data"]["search"]["products"][0]
        return {
            "store": "Target",
            "name": item["item"]["product_description"]["title"],
            "price": item["price"]["current_retail"],
            "url": item["item"]["enrichment"]["buy_url"],
        }
    else:
        print(f"No Target data for '{query}'")
        return None

# Mock fallback for testing
def mock_price(query):
    return [
        {
            "store": "Walmart",
            "name": f"{query} - Walmart mock",
            "price": 9.99,
            "url": "https://example.com/walmart"
        },
        {
            "store": "Target",
            "name": f"{query} - Target mock",
            "price": 10.49,
            "url": "https://example.com/target"
        }
    ]

def collect_prices(use_mock=False):
    today = date.today().isoformat()
    out_path = "data/price_comparison.csv"
    with open("data/sku_master.csv", newline='', encoding='utf-8') as f, open(out_path, "w", newline='', encoding='utf-8') as out:
        reader = csv.DictReader(f)
        fieldnames = ["sku_id", "sku_name", "store", "product_name", "price", "currency", "collection_date", "source_url"]
        writer = csv.DictWriter(out, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            query = row["sku_name"]
            if use_mock:
                entries = mock_price(query)
            else:
                walmart = fetch_walmart_price(query)
                target = fetch_target_price(query)
                entries = [walmart, target]

            for entry in entries:
                if entry:
                    writer.writerow({
                        "sku_id": row["sku_id"],
                        "sku_name": row["sku_name"],
                        "store": entry["store"],
                        "product_name": entry["name"],
                        "price": entry["price"],
                        "currency": "USD",
                        "collection_date": today,
                        "source_url": entry["url"],
                    })

    print("✅ Data written to", out_path)

if __name__ == "__main__":
    # Set use_mock=True to test CSV generation without real API calls
    collect_prices(use_mock=True)
