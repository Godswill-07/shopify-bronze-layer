# 🥉 Bronze Layer — Shopify Orders Ingestion

## Overview
Raw data ingestion pipeline that extracts orders from the 
Shopify REST API and lands them into a Delta Lake Bronze table 
on Microsoft Fabric Lakehouse.

## Architecture
Shopify REST API → Paginated Fetch → Delta Table (bronze_shopify_orders)
## Features
- Paginated API fetch (250 orders per page)
- Incremental loading (only fetches new/updated orders)
- Rate limit handling with automatic retry
- Schema evolution with mergeSchema
- Appends raw data — nothing is ever deleted

## Tech Stack
- Microsoft Fabric
- PySpark
- Delta Lake
- Shopify REST API (2024-04)
- Python (requests, json)

## Table Schema
| Column | Type | Description |
|---|---|---|
| id | long | Shopify order ID |
| created_at | timestamp | Order creation time |
| customer | struct | Nested customer object |
| line_items | array | List of ordered products |
| total_price | string | Order total |
| extracted_at | timestamp | Pipeline run timestamp |

## How to Run
1. Attach LH_Landing lakehouse to notebook
2. Set STORE_NAME and API_TOKEN in config section
3. Run notebook manually or via pipeline schedule

## Pipeline Schedule
Runs daily at 2:00 AM WAT via shopify_medallion_pipeline
