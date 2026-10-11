# 🍽️ Real-Time Restaurant Intelligence & Lakehouse Analytics

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-Structured_Streaming-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Azure Event Hubs](https://img.shields.io/badge/Azure_Event_Hubs-Kafka_Protocol-0078D4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-Medallion_Architecture-00ADD8?style=for-the-badge&logo=delta&logoColor=white)](https://delta.io)
[![Mosaic AI](https://img.shields.io/badge/Databricks_Mosaic_AI-LLM_Sentiment-6B46C1?style=for-the-badge&logo=openai&logoColor=white)](https://databricks.com/product/mosaic-ai)

> **An end-to-end cloud-native Lakehouse platform built on Databricks, Apache Spark Structured Streaming, and Delta Lake. Features multi-source ingestion (Azure Event Hubs + CDC), Medallion Architecture (Bronze ➔ Silver ➔ Gold), Mosaic AI LLM evaluation for automated review parsing, Customer 360 dimensions, and production Lakeview BI dashboards.**

---

## 📌 Table of Contents
- [Executive Overview](#-executive-overview)
- [System Architecture](#-system-architecture)
- [Medallion Lakehouse Pipeline](#-medallion-lakehouse-pipeline)
  - [Bronze Layer (Raw Ingestion)](#1-bronze-layer-raw-ingestion)
  - [Silver Layer (Quality & Standardization)](#2-silver-layer-quality--standardization)
  - [Mosaic AI LLM Review Enrichment](#3-mosaic-ai-llm-review-enrichment)
  - [Gold Layer (Business Aggregations & Customer 360)](#4-gold-layer-business-aggregations--customer-360)
- [Dashboards & Analytics Capabilities](#-dashboards--analytics-capabilities)
- [Repository Structure](#-repository-structure)
- [Data Flow & Quality Constraints](#-data-flow--quality-constraints)
- [Local Setup & Deployment](#-local-setup--deployment)
- [Key Engineering Innovations](#-key-engineering-innovations)

---

## 🎯 Executive Overview

Modern restaurant chains process high-frequency omnichannel orders (Dine-in, Takeaway, Delivery) alongside dynamic customer feedback across disparate stores. This platform delivers a production-grade Databricks Lakehouse implementation that:

1. **Ingests High-Velocity Order Streams**: Connects to **Azure Event Hubs** using Spark's Kafka connector with rate-limiting, fault-tolerant offsets, and SASL/SSL authentication.
2. **Synchronizes Operational Databases via CDC**: Incorporates **Lakeflow Connect Ingestion Gateways** for Change Data Capture from upstream relational databases.
3. **Automates Unstructured NLP Analytics**: Harnesses Databricks **Mosaic AI (`ai_query` with `databricks-gpt-oss-20b`)** directly in streaming SQL pipelines to classify sentiment and extract granular operational issues (Delivery, Food Quality, Pricing, Portion Size).
4. **Constructs an Omnichannel Customer 360**: Aggregates customer lifetime spend, visit frequency, favorite menus, and loyalty tier stratification.
5. **Drives Executive Decision Making**: Powers interactive **Databricks Lakeview Dashboards** for real-time chain metrics, revenue breakdowns, and review trends.

---

## 🏗️ System Architecture

```
                                  [ OPERATIONAL SOURCES ]
                                             │
               ┌─────────────────────────────┴─────────────────────────────┐
               ▼                                                           ▼
   [ Azure Event Hubs (Kafka) ]                                [ Enterprise RDBMS / CDC ]
       (Real-Time Orders)                                         (Customers & Menus)
               │                                                           │
               │ Spark Structured Streaming                                │ Lakeflow Connect Gateway
               ▼                                                           ▼
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │                            BRONZE LAYER (Delta Lake)                             │
   │  • `01_bronze.orders` (Raw streaming payload, JSON schema parsing)               │
   │  • `01_bronze.reviews` (Continuous customer reviews feed)                        │
   │  • `00_landing` (Lakeflow gateway landing tables)                                │
   └─────────────────────────────────────────┬────────────────────────────────────────┘
                                             │
                       Spark Declarative Pipelines / DLT Expectations
                                             ▼
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │                            SILVER LAYER (Delta Lake)                             │
   │  • Data Quality Enforcement (`@dp.expect_all_or_drop`, strict validation)        │
   │  • `02_silver.fact_orders`: Temporal features (hour, DOW, is_weekend), types     │
   │  • `02_silver.fact_order_items`: Exploded item array, line-item subtotals        │
   │  • `02_silver.fact_reviews`: Mosaic AI LLM parsing & issue categorization        │
   │  • `02_silver.dim_restaurants` & `02_silver.dim_customers`: Clean dimensions     │
   └─────────────────────────────────────────┬────────────────────────────────────────┘
                                             │
                        Window Functions, Materialized Views & Aggregations
                                             ▼
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │                             GOLD LAYER (Delta Lake)                              │
   │  • `03_gold.d_sales_summary`: Partitioned daily KPIs (Revenue, AOV, Channels)    │
   │  • `03_gold.d_customer_360`: Loyalty tiers (Platinum/Gold/Silver), Favorite dish │
   │  • `03_gold.d_restaurant_reviews`: Star ratings breakdown & sentiment counts     │
   └─────────────────────────────────────────┬────────────────────────────────────────┘
                                             │
                                             ▼
   ┌──────────────────────────────────────────────────────────────────────────────────┐
   │                          DATABRICKS LAKEVIEW DASHBOARDS                          │
   │  • Chain Performance (Revenue, Orders, AOV, Peak Hour Heatmaps, DOW volume)      │
   │  • Customer & Review Insights (Sentiment trends, Issue breakdown, Live feed)     │
   └──────────────────────────────────────────────────────────────────────────────────┘
```

---

## 💎 Medallion Lakehouse Pipeline

### 1. Bronze Layer (Raw Ingestion)
- **High-Throughput Streaming**: Reads JSON order streams from Azure Event Hubs via Kafka protocol (`SASL_SSL` / `PLAIN`).
- **Defensive Micro-Batching**: Enforces `maxOffsetsPerTrigger=50000`, safe failover (`failOnDataLoss=false`), and explicit schema unpacking into `StructType`.
- **Auditability**: Retains raw Kafka keys, values, and partition offsets alongside parsed fields (`order_id`, `restaurant_id`, `customer_id`, `items`, `total_amount`).

### 2. Silver Layer (Quality & Standardization)
Data quality constraints are declared declaratively via Spark Pipelines (`@dp.expect_all_or_drop`):
- `valid_order_id`, `valid_restaurant_id`, `valid_customer_id`: Non-null validation.
- `valid_payment_method`: Restricted to `('cash', 'card', 'wallet')`.
- `valid_order_status`: Restricted to `('completed', 'delivered', 'pending', 'ready', 'preparing', 'confirmed')`.
- `valid_total_amount`: Positive transaction values (`total_amount > 0`).

**Key Transformations:**
- **`fact_orders`**: Extracts temporal dimensions (`order_date`, `order_hour`, `day_of_week`, `is_weekend`) and parses embedded items JSON.
- **`fact_order_items`**: Unnests and explodes order line items using PySpark `explode()` for item-level revenue and category analysis.

### 3. Mosaic AI LLM Review Enrichment
Customer reviews are continuously streamed and enriched using Databricks **Mosaic AI (`ai_query`)** directly within SQL transformations:

```sql
WITH model_response AS (
    SELECT *,
        ai_query(
            'databricks-gpt-oss-20b',
            CONCAT(
                'Analyze the following review and return ONLY a valid JSON object with this exact structure: ',
                '{"sentiment": "<positive/neutral/negative>", ',
                '"issue_delivery": <true/false>, "issue_delivery_reason": "<reason or empty string>", ',
                '"issue_food_quality": <true/false>, "issue_food_quality_reason": "<reason or empty string>", ',
                '"issue_pricing": <true/false>, "issue_pricing_reason": "<reason or empty string>", ',
                '"issue_portion_size": <true/false>, "issue_portion_size_reason": "<reason or empty string>"} ',
                'Rules: sentiment must be exactly one of: positive, neutral, negative. Each issue field is true/false only. ',
                'Review text: ', review_text
            )
        ) AS analysis_json
    FROM STREAM `01_bronze`.reviews
)
SELECT
    review_id, order_id, restaurant_id, rating, review_text,
    get_json_object(analysis_json, '$.sentiment') AS sentiment,
    get_json_object(analysis_json, '$.issue_delivery')::BOOLEAN AS issue_delivery,
    get_json_object(analysis_json, '$.issue_food_quality')::BOOLEAN AS issue_food_quality,
    get_json_object(analysis_json, '$.issue_pricing')::BOOLEAN AS issue_pricing,
    get_json_object(analysis_json, '$.issue_portion_size')::BOOLEAN AS issue_portion_size,
    review_timestamp
FROM model_response;
```

### 4. Gold Layer (Business Aggregations & Customer 360)
- **`d_sales_summary`**: Daily materialized view partitioned by `order_date`. Aggregates total revenue, AOV, active restaurants, unique customers, and sales by channel (`dine_in`, `takeaway`, `delivery`).
- **`d_customer_360`**: Computes single customer view including lifetime spend, average order value, total reviews given, dynamic **Loyalty Tier Classification** (`Platinum >= 5000`, `Gold >= 2000`, `Silver >= 1000`, `Bronze`), favorite restaurant, and most ordered dish via Spark Window partition ranking (`row_number()`).
- **`d_restaurant_reviews`**: Aggregates star rating distributions (1-star to 5-star counts), average ratings, and positive/neutral/negative counts per restaurant location.

---

## 📊 Dashboards & Analytics Capabilities

The lakehouse exports native Databricks Lakeview definitions (`Restaurant Analytics.lvdash.json`) providing bi-temporal reporting:

| Dashboard View | Primary Metrics & Visualizations |
| :--- | :--- |
| **Chain Performance** | • KPI Counters: Total Orders, Total Revenue, Active Customers, AOV, Unique Customers<br>• Trend Analysis: Daily sales & order volume over time<br>• Product Analytics: Top 10 selling menu items by volume and revenue<br>• Channel Mix: Dine-in vs Takeaway vs Delivery breakdown<br>• Peak Trading: Order volume heatmap across day of week & operating hours |
| **Review & Sentiment Insights** | • Sentiment Trends: Real-time positive/neutral/negative rating volume<br>• Operational Bottlenecks: Issue breakdown counters (Delivery, Food Quality, Pricing, Portion Size)<br>• Location Performance: Average ratings and star distributions across UAE branches<br>• Live Review Stream: Real-time filterable positive & negative review feed |

---

## 📁 Repository Structure

```
restaurant_analytics/
├── Restaurant Analytics.lvdash.json      # Production Databricks Lakeview dashboard definition
├── pipeline_ingestion_eventhub/          # Streaming ingestion pipelines
│   └── transformations/
│       └── eventhub.py                   # PySpark Event Hubs / Kafka streaming reader (Bronze)
├── pipeline_transformation_silver/       # Data cleaning & NLP transformations
│   └── transformations/
│       ├── fact_orders.py                # Silver orders table with DLT data quality rules
│       ├── fact_order_items.py           # Silver unnested items table
│       └── fact_reviews.sql              # Mosaic AI LLM sentiment & issue classification
├── pipeline_transformation_gold/         # Dimensional models & materialized views
│   └── transformations/
│       ├── d_sales_summary.py            # Daily sales & channel metrics (Partitioned Gold MV)
│       ├── d_customer360.py              # Customer 360 & Loyalty tier profiling
│       └── d_restaurant_reviews.py       # Restaurant ratings & sentiment aggregations
├── Drafts/                               # Exploratory notebooks & ad-hoc queries
│   ├── Mosaic AI.dbquery.ipynb           # LLM prompt engineering & query prototyping
│   └── Sql queries used.dbquery.ipynb    # Ad-hoc reporting & verification queries
└── Project Details and scripts/          # Synthetic generators, config & documentation
    ├── projects/databricks-e2e-project/
    │   ├── 00_synthetic_data/            # Event Hubs producer, SQL DB generators, CSV mocks
    │   ├── diagrams/                     # Architecture & data flow diagrams
    │   ├── commands_used.md              # CLI & Databricks pipeline deployment commands
    │   └── dashboard_metrics.md          # Dashboard functional specifications
    └── Works/                            # Architecture exports, dashboards & screenshots
```

---

## 🚦 Data Flow & Quality Constraints

| Pipeline Entity | Quality Rules / Expectations | Target Schema |
| :--- | :--- | :--- |
| `01_bronze.orders` | Streaming JSON deserialization; offset tracking | Delta Lake (Bronze) |
| `02_silver.fact_orders` | `order_id IS NOT NULL`, `total_amount > 0`, `payment_method IN ('cash','card','wallet')`, `item_count > 0` | Delta Lake (Silver) |
| `02_silver.fact_order_items` | `quantity > 0`, `unit_price > 0`, `subtotal > 0`, non-null keys | Delta Lake (Silver) |
| `02_silver.fact_reviews` | `sentiment IN ('positive','neutral','negative')`, `rating > 0` | Delta Lake (Silver) |
| `03_gold.d_sales_summary` | Partitioned by `order_date`; idempotent batch aggregation | Delta Lake (Gold MV) |
| `03_gold.d_customer_360` | Customer-level window partition deduplication & RFM metrics | Delta Lake (Gold MV) |

---

## ⚙️ Local Setup & Deployment

### 1. Prerequisites
- Python 3.10+
- Databricks Workspace (Runtime 14.3+ with Unity Catalog enabled)
- Azure Event Hubs namespace or Kafka broker

### 2. Environment Configuration
Create a `.env` file in the generator root:
```env
EVENTHUB_CONNECTION_STRING="Endpoint=sb://<namespace>.servicebus.windows.net/;SharedAccessKeyName=...;SharedAccessKey=..."
EVENTHUB_NAME="orders"
```

### 3. Generate Synthetic Streaming Events
Stream live orders into Azure Event Hubs:
```bash
cd "Project Details and scripts/projects/databricks-e2e-project/00_synthetic_data"
pip install -r requirements.txt
python 04_eventhub_orders.py
```

### 4. Deploy Databricks Declarative Pipeline
Deploy the Medallion pipeline using Databricks CLI:
```bash
databricks pipelines create --json @pipeline.json
databricks pipelines start --pipeline-id <pipeline-id>
```

---

## 💡 Key Engineering Innovations

1. **Native GenAI SQL Integration**: Replaced heavy external microservices with Databricks Mosaic AI `ai_query` executing within streaming SQL workflows, slashing classification latency.
2. **Delta Lake Medallion Strictness**: Applied strict expectation enforcement (`@dp.expect_all_or_drop`) preventing malformed or zero-value transactions from polluting Silver/Gold analytical layers.
3. **Advanced Customer 360 Windowing**: Leveraged PySpark window functions (`row_number().over(Window.partitionBy(...))`) over streaming joins to compute dynamic favorites and loyalty status without expensive batch re-scans.
4. **Declarative Lakeview Analytics**: Implemented parameter-driven date-range slicing directly inside the Lakeview dashboard spec for responsive real-time slicing.

---

<div align="center">
  <b>Developed by Alvin M Josan</b> • 
  <a href="https://github.com/alvinmjosan">GitHub</a> • 
  <a href="https://www.linkedin.com/in/alvinmjosan">LinkedIn</a>
</div>
