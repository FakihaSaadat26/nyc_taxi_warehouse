# CS-404 Big Data Analytics — Complete Project
## Data Warehouse & Analytics Pipeline

**Course:** CS-404 Big Data Analytics — BDA Spring 2026  
**Instructor:** Ms. Zahida Kausar  
**Total Marks:** 60 (Assignment 02 + Assignment 03)

---

## 📌 Project Overview

This is a comprehensive data warehousing project for the NYC Yellow Taxi Trip Records dataset. The project spans two assignments and demonstrates the complete lifecycle from data ingestion to advanced analytics:

**Assignment 02 — Data Ingestion & Profiling:**
- Task 1: Dataset selection and justification (NYC Yellow Taxi Trip Records)
- Task 2: Automated HDFS ingestion pipeline (`ingest.py`)
- Task 3: Data profiling report with quality analysis and cleaning recommendations

**Assignment 03 — ETL Pipeline & Analytics:**
- Task 1 (`etl.py`)** — PySpark ETL: clean, transform, model into a star schema, load to HDFS as partitioned Parquet, and validate.
- Task 2 (`analytics.py`)** — 5 Spark SQL queries (with `RANK()`, `ROW_NUMBER()`, `LAG()` window functions + time-based analysis) and 4 visualisation charts.
- Task 3** — Pipeline optimization (caching, partitioning, broadcast joins) documented in `final_report.pdf`.

---

## 📂 Repository Structure

```
nyc_taxi_warehouse/
│
├── data/                      # (auto-created by ingest.py)
│   └── yellow_tripdata_2023-01.parquet
│
├── Part1- assembling the data/
│   ├── ingest.py              # Assignment 02, Task 2: HDFS ingestion pipeline
│   └── requirements.txt        # Part 1 dependencies
│
├── Part2- Cleaning, organizing and analysing/
│   ├── etl.py                 # Assignment 03, Task 1: PySpark ETL pipeline
│   ├── analytics.py           # Assignment 03, Task 2: Spark SQL queries + visualizations
│   ├── README.md              # Comprehensive project documentation (this file)
│   ├── requirements.txt        # Part 2 dependencies
│   ├── profiling_report.pdf   # Assignment 02, Task 3: Data profiling & quality analysis
│   ├── final_report.pdf       # Assignment 03, Task 3: Optimization techniques & report
│   ├── hdfs_screenshot.png    # HDFS /warehouse/ directory screenshot
│   ├── ingest.log             # (auto-generated) Ingestion pipeline execution log
│   ├── etl.log                # (auto-generated) ETL pipeline execution log
│   ├── analytics.log          # (auto-generated) Analytics query execution log
│   │
│   ├── warehouse/
│   │   └── processed/         # (auto-created by etl.py)
│   │       ├── fact_trips/    # Partitioned by pickup_month
│   │       ├── dim_time/
│   │       ├── dim_location/
│   │       ├── dim_payment/
│   │       └── dim_vendor/
│   │
│   └── charts/                # (auto-created by analytics.py)
│       ├── chart1_hourly_trend.png
│       ├── chart2_revenue_by_period.png
│       ├── chart3_tip_heatmap.png
│       └── chart4_dashboard.png
```

---

## 🗂️ Dataset Overview

| Attribute        | Detail                                                   |
|------------------|----------------------------------------------------------|
| **Name**         | NYC Yellow Taxi Trip Records — January 2023              |
| **Source**       | NYC Taxi & Limousine Commission (TLC) via AWS Open Data  |
| **URL**          | https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page |
| **Format**       | Parquet                                                  |
| **Size**         | ~50 MB (compressed); ~3M rows per month                  |
| **Attributes**   | 19 columns spanning numeric, categorical, and datetime   |

### Key Columns

| Column                   | Type      | Description                        |
|--------------------------|-----------|------------------------------------|
| `tpep_pickup_datetime`   | datetime  | Trip pickup timestamp              |
| `tpep_dropoff_datetime`  | datetime  | Trip dropoff timestamp             |
| `passenger_count`        | int       | Number of passengers               |
| `trip_distance`          | float     | Trip distance in miles             |
| `PULocationID`           | int       | Pickup zone ID                     |
| `DOLocationID`           | int       | Dropoff zone ID                    |
| `RatecodeID`             | int       | Rate code (standard, JFK, etc.)    |
| `payment_type`           | int       | Payment method (card, cash, etc.)  |
| `fare_amount`            | float     | Base metered fare                  |
| `tip_am(HDFS) installed and running (`hdfs` on PATH)

### 1. Create Virtual Environment & Install Dependencies

```bash
# Navigate to project root
cd nyc_taxi_warehouse

# Create virtual environment
python3 -m venv venv
source venv/bin/activate          # Linux / macOS
# venv\Scripts\activate           # Windows

# Install dependencies for both parts
cd Part1-\ assembling\ the\ data/
pip install -r requirements.txt
cd ../Part2-\ Cleaning,\ organizing\ and\ analysing/
pip install -r requirements.txt
```

### 2. Start Hadoop Services (if not running)

```bash
start-dfs.sh
start-yarn.sh
# Verify:
hdfs dfs -ls /
```

---

## 🚀 Part 1: Running the Ingestion Pipeline (`ingest.py`)

Navigate to `Part1- assembling the data/`:

```bash
python ingest.py
```

**What it does (in sequence):**

| Step       | Action                                                                   |
|------------|--------------------------------------------------------------------------|
| **Load**   | Downloads `yellow_tripdata_2023-01.parquet` from the TLC AWS endpoint    |
| **Validate** | Checks file size (≥10 MB), extension, encoding, and row count (≥500k) |
| **Upload** | Uploads to HDFS at `/warehouse/raw/nyc_taxi/year=2026/month=04/`         |
| **Organise** | Verifies file presence in HDFS and logs directory listing              |
| **Log**    | All steps logged to `ingest.log` and stdout                              |

**Expected output:**
```
2026-04-12 10:00:00 | INFO     | Pipeline started
2026-04-12 10:00:01 | INFO     | Downloading dataset …
2026-04-12 10:01:30 | INFO     | ✔ File exists …
2026-04-12 10:01:31 | INFO     | ✔ Row count meets minimum: 3,066,766 rows
2026-04-12 10:01:40 | INFO     | ✔ File uploaded to HDFS
2026-04-12 10:01:41 | INFO     | ✔ Pipeline finished in 101.2 seconds.
```

Navigate to `Part2- Cleaning, organizing and analysing/`:

### Generating the Profiling Report

In the same directory:

```bash
python profiling_report.py
```

Produces `profiling_report.pdf` containing:
1. Schema Description
2. Missing Value Analysis (heatmap + bar chart)
3. Statistical Summary (mean, median, std, min, max)
4. Distribution Analysis (histogram + KDE for 6 attributes)
5. Data Quality Issues (duplicates, outliers, inconsistencies)
6. Proposed Cleaning Strategy (specific justified action per issue)

---

## 🚀 Part 2:               │ date_key (PK)│
                    │ hour, dow    │
                    │ time_of_day  │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴────────────┐    ┌──────────────┐
│ dim_location │────│    fact_trips      │────│  dim_payment │
│ location_id  │    │ (date_key FK)      │    │ payment_key  │
│ location_type│    │ pickup_location_id │    │ payment_label│
└──────────────┘    │ dropoff_location_id│    │ rate_label   │
                    │ vendor_id (FK)     │    └──────────────┘
                    │ fare_amount        │
                    │ tip_amount, tip_pct│    ┌──────────────┐
                    │ total_amount       │────│  dim_vendor  │
                    │ trip_distance      │    │ vendor_id    │
                    │ trip_duration_min  │    │ vendor_name  │
                    │ is_airport_trip    │    └──────────────┘
                    │ time_of_day        │
                    │ distance_bucket    │
                    └───────────────────┘
```

---

## ⚙️ Setup Instructions

### Prerequisites

- Python 3.10+
- Apache Spark 3.4+ with PySpark (`SPARK_HOME` set)
- Java 8 or 11
- Hadoop HDFS running (from A2)
- A2 dataset already at `/warehouse/raw/nyc_taxi/year=2026/month=04/`

### Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

---

## 🚀 Running the ETL Pipeline

```bash
python etl.py
```

**Pipeline steps (automated, no manual intervention):**

| Step | Action |
|------|--------|
| **Extract** | Reads raw Parquet from HDFS (falls back to `./data/` if HDFS unavailable) |
| **Transform** | Applies 10 cleaning operations (each referencing A2 profiling findings) + derives 12 new columns |
| **Model** | Builds star schema: `fact_trips` + 4 dimension tables |
| **Load** | Writes Parquet to `/warehouse/processed/`; `fact_trips` partitioned by `pickup_month` |
| **Validate** | Row counts, null assertions, per-table summary logged to `etl.log` |
| **Optimize** | Demonstrates caching, partitioning, and broadcast joins |

**Expected output:**
```
ETL pipeline finished in ~120.0 s
✔ All validation checks passed.
```

---

## 📊 Running Analytics & Visualizations

```bash
python analytics.py
```

**Produces:**
- 5 Spark SQL query results printed to stdout
- 4 chart PNG files in `./charts/`
- `analytics.log` with query interpretations

**Spark SQL Queries:**

| Query | Business Question | Window Function |
|-------|-------------------|-----------------|
| Q1 | Revenue by time-of-day period | `RANK()` |
| Q2 | Trip distance vs tip % by payment type | `ROW_NUMBER()` |
| Q3 | Hourly demand trend (weekday vs weekend) | Time-based analysis |
| Q4 | Top revenue pickup zones | `RANK()` |
| Q5 | Airport vs city trips — week-over-week | `LAG()` |

---

## ⚡ Optimization Techniques Applied

| Technique | Where | Impact |
|-----------|-------|--------|
| **Caching** | `fact_trips.cache()` after model() | Avoids re-reading HDFS for 5+ downstream queries |
| **Partitioning** | `fact_trips` partitioned by `pickup_month` | Month-filter queries read 1/12 of data |
| **Broadcast Join** | `dim_vendor` (2 rows) broadcast to fact | Eliminates shuffle in vendor enrichment join |
| **Query Plan** | `.explain(True)` on vendor revenue query | Documents physical execution plan |

---

## 👥 Group Members

| Name | Roll Number |
|------|-------------|
| Member 1 | `BSCS-XX-XXX` |
| Member 2 | `BSCS-XX-XXX` |
| Member 3 | `BSCS-XX-XXX` |
| Member 4 | `BSCS-XX-XXX` |

> Replace placeholder names and roll numbers before submission.

--- & Deliverables

### Assignment 02 Deliverables
- **File Naming:** `GroupNumber_A2_BDA.zip`
- **Contents:**
  - `ingest.py` (Part 1)
  - `hdfs_screenshot.png` (HDFS directory listing)
  - `profiling_report.pdf`
  - `requirements.txt`
  - `README.md`

### Assignment 03 Deliverables
- **File Naming:** `GroupNumber_A3_BDA.zip`
- **Contents:**
  - `etl.py` (Part 2)
  - `analytics.py` (Part 2)
  - `hdfs_screenshot.png` of `/warehouse/processed/` after running `etl.py`
  - `final_report.pdf`
  - `requirements.txt`
  - `README.md`

---

## 📝 Notes & Additional Information

- **HDFS Path Convention:** The raw data follows partitioned warehouse convention: `/warehouse/raw/nyc_taxi/year=YYYY/month=MM/`
- **Pipeline Logging:** All pipeline steps are logged to respective `.log` files for audit trail and debugging
- **Data Profiling:** The profiling report samples up to 200,000 rows for memory-efficient processing
- **Dependency Chain:** Assignment 03 depends on successful completion of Assignment 02 ingestion
- **Project Link:** https://github.com/FakihaSaadat26/nyc_taxi_warehouse.gi
- Add `hdfs_screenshot.png` of `/warehouse/processed/` after running `etl.py`
- Run `hdfs dfs -ls -R /warehouse/processed/` and screenshot the output
