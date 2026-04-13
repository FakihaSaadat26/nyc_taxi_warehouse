# CS-404 Big Data Analytics — Assignment 02
## Data Source Selection & Ingestion Pipeline

**Course:** CS-404 Big Data Analytics — BDA Spring 2026  
**Instructor:** Ms. Zahida Kausar  
**Submission Deadline:** Wednesday, 15th April 2026 (11:59 PM)  
**Total Marks:** 30

---

##  Project Overview

This repository contains all deliverables for Assignment 02 of CS-404 Big Data Analytics at NUST SEECS. The project demonstrates:

1. **Dataset Selection** — Identification and justification of a large-scale real-world dataset (NYC Yellow Taxi Trip Records).
2. **HDFS Ingestion Pipeline** — A fully automated Python script (`ingest.py`) that downloads, validates, and uploads data to Hadoop HDFS.
3. **Data Profiling Report** — A comprehensive PDF report profiling data quality, distributions, and proposing cleaning strategies for the ETL pipeline in Assignment 3.

---
## 📂 Repository Link
https://github.com/FakihaSaadat26/nyc_taxi_warehouse.git
## 📂 Repository Structure

```
GroupNumber_A2_BDA/
│
├── ingest.py                  # Task 2: Automated HDFS ingestion pipeline
├── profiling_report.py        # Task 3: Data profiling report generator
├── requirements.txt           # Python dependencies
├── README.md                  # This file
│
├── data/                      # (auto-created) Raw downloaded dataset
│   └── yellow_tripdata_2023-01.parquet
│
├── profiling_report.pdf       # (auto-generated) Task 3 deliverable
├── hdfs_screenshot.png        # Task 2 deliverable: HDFS directory screenshot
└── ingest.log                 # (auto-generated) Pipeline execution log
```

---

## 🗂️ Dataset

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
| `tip_amount`             | float     | Tip amount                         |
| `tolls_amount`           | float     | Tolls charged                      |
| `total_amount`           | float     | Total amount charged               |
| `congestion_surcharge`   | float     | NYC congestion surcharge           |
| `airport_fee`            | float     | Airport surcharge                  |

---

##  Setup Instructions

### Prerequisites

- Python 3.10+
- Hadoop (HDFS) installed and running (`hdfs` on PATH)
- Java 8 or 11 (required by Hadoop)

### 1. Clone / Unzip the Submission



### 2. Create Virtual Environment & Install Dependencies

```bash
python3 -m venv venv
source venv/bin/activate          # Linux / macOS
# venv\Scripts\activate           # Windows

pip install -r requirements.txt
```

### 3. Start Hadoop Services (if not running)

```bash
start-dfs.sh
start-yarn.sh
# Verify:
hdfs dfs -ls /
```

---

## 🚀 Running the Ingestion Pipeline (`ingest.py`)

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

---

##  Generating the Profiling Report (`profiling_report.py`)

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

##  Submission Details

- **File Naming:** `GroupNumber_A2_BDA.zip`
- **Contents:**
  - `ingest.py`
  - `hdfs_screenshot.png`
  - `profiling_report.pdf`
  - `requirements.txt`
  - `README.md`

---

##  Notes

- The HDFS path follows partitioned warehouse convention: `/warehouse/raw/nyc_taxi/year=YYYY/month=MM/`
- All pipeline steps are logged to `ingest.log` for audit trail
- The profiling report samples up to 200,000 rows for memory-efficient processing
- This milestone feeds directly into Assignment 3 ETL design
