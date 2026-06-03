import requests
import json
import time
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.getOrCreate()

# --- Config ---
STORE_NAME  = "fabric-pipeline-dev"
API_TOKEN   = "YOUR_TOKEN_HERE"
API_VERSION = "2024-04"
LIMIT       = 250

# --- Incremental: only fetch orders updated since last run ---
try:
    last_extracted = spark.sql(
        "SELECT MAX(extracted_at) FROM bronze_shopify_orders"
    ).collect()[0][0]
    updated_at_min = (last_extracted - timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"📅 Incremental run: fetching orders updated after {updated_at_min}")
except Exception:
    updated_at_min = None
    print("🆕 First run: fetching all orders")

# --- Fetch ---
url = (
    f"https://{STORE_NAME}.myshopify.com/admin/api/{API_VERSION}/orders.json"
    f"?limit={LIMIT}&status=any"
    + (f"&updated_at_min={updated_at_min}" if updated_at_min else "")
)
headers = {
    "X-Shopify-Access-Token": API_TOKEN,
    "Content-Type": "application/json"
}

all_orders = []
page = 1

while url:
    response = requests.get(url, headers=headers)

    if response.status_code == 429:
        print("⏳ Rate limited — waiting 5s...")
        time.sleep(5)
        continue
    elif response.status_code != 200:
        raise Exception(f"Shopify API error: {response.status_code} — {response.text}")

    orders = response.json().get("orders", [])
    all_orders.extend(orders)
    print(f"  Page {page}: {len(orders)} orders (total: {len(all_orders)})")

    link = response.headers.get("Link", "")
    if 'rel="next"' in link:
        url = link.split("<")[1].split(">")[0]
        page += 1
    else:
        url = None

# --- Write to Bronze ---
if all_orders:
    rdd       = spark.sparkContext.parallelize([json.dumps(o) for o in all_orders])
    df_bronze = spark.read.json(rdd)
    df_bronze = df_bronze.withColumn(
        "extracted_at",
        F.lit(datetime.now(timezone.utc).isoformat()).cast("timestamp")
    )
    df_bronze.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("bronze_shopify_orders")

    print(f"✅ Bronze: {len(all_orders)} orders written to bronze_shopify_orders")
else:
    print("ℹ️ No new orders — bronze unchanged")
