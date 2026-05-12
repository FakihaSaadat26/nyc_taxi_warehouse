#!/usr/bin/env python3
"""
etl.py  —  PySpark ETL Pipeline
CS-404 Big Data Analytics — Assignment 03
Dataset : NYC Yellow Taxi Trip Records (January 2023)
Picks up from A2: data already ingested to HDFS at
    /warehouse/raw/nyc_taxi/year=2026/month=04/yellow_tripdata_2023-01.parquet
"""

import logging
import sys
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType
from pyspark.sql.window import Window

# ─────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("etl.log", mode="w")],
)
log = logging.getLogger("etl")

# ─────────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────────
RAW_PATH      = "hdfs:///warehouse/raw/nyc_taxi/year=2026/month=04/yellow_tripdata_2023-01.parquet"
PROCESSED_BASE = "hdfs:///warehouse/processed"

# For local fallback (when HDFS not running)
RAW_PATH_LOCAL       = "./data/yellow_tripdata_2023-01.parquet"
PROCESSED_BASE_LOCAL = "./warehouse/processed"


def get_paths():
    """Return (raw, processed_base) — HDFS if available, else local."""
    import subprocess
    result = subprocess.run(["hdfs", "dfs", "-test", "-e", RAW_PATH],
                            capture_output=True)
    if result.returncode == 0:
        log.info("Using HDFS paths.")
        return RAW_PATH, PROCESSED_BASE
    log.warning("HDFS not reachable — falling back to local paths.")
    return RAW_PATH_LOCAL, PROCESSED_BASE_LOCAL


# ─────────────────────────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (SparkSession.builder
             .appName("BDA_A3_ETL_NYC_Taxi")
             .config("spark.sql.shuffle.partitions", "50")
             .config("spark.sql.parquet.compression.codec", "snappy")
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created — version %s", spark.version)
    return spark


# ─────────────────────────────────────────────────────────────────
# STEP 1 — EXTRACT
# ─────────────────────────────────────────────────────────────────
def extract(spark: SparkSession, raw_path: str):
    log.info("=== STEP 1: EXTRACT ===")
    df = spark.read.parquet(raw_path)
    raw_count = df.count()
    log.info("Raw row count: %d", raw_count)
    log.info("Schema:\n%s", df._jdf.schema().treeString())
    return df, raw_count


# ─────────────────────────────────────────────────────────────────
# STEP 2 — TRANSFORM (all transformations reference A2 findings)
# ─────────────────────────────────────────────────────────────────
def transform(df, spark: SparkSession):
    log.info("=== STEP 2: TRANSFORM ===")

    # ── 2.1  CAST & STANDARDISE TYPES ────────────────────────────
    df = (df
          .withColumn("passenger_count",     F.col("passenger_count").cast(IntegerType()))
          .withColumn("RatecodeID",          F.col("RatecodeID").cast(IntegerType()))
          .withColumn("fare_amount",         F.col("fare_amount").cast(DoubleType()))
          .withColumn("tip_amount",          F.col("tip_amount").cast(DoubleType()))
          .withColumn("total_amount",        F.col("total_amount").cast(DoubleType()))
          .withColumn("trip_distance",       F.col("trip_distance").cast(DoubleType()))
          .withColumn("congestion_surcharge",F.col("congestion_surcharge").cast(DoubleType()))
          .withColumn("airport_fee",         F.col("airport_fee").cast(DoubleType()))
          .withColumn("tpep_pickup_datetime",
                      F.to_timestamp("tpep_pickup_datetime", "yyyy-MM-dd HH:mm:ss"))
          .withColumn("tpep_dropoff_datetime",
                      F.to_timestamp("tpep_dropoff_datetime", "yyyy-MM-dd HH:mm:ss"))
    )

    # ── 2.2  CLEANING (referencing A2 profiling findings) ─────────

    # A2 finding: fare_amount has 342 negative values (data entry errors).
    # Action: Replace negative fares with the column-wide median (robust to skew).
    fare_median = df.approxQuantile("fare_amount", [0.5], 0.01)[0]
    df = df.withColumn(
        "fare_amount",
        F.when(F.col("fare_amount") < 0, fare_median).otherwise(F.col("fare_amount"))
    )
    log.info("  fare_amount negatives replaced with median=%.2f", fare_median)

    # A2 finding: tip_amount has ~50 negative values (impossible by definition).
    # Action: Set negative tips to 0 — zero is a valid outcome for cash payers.
    df = df.withColumn(
        "tip_amount",
        F.when(F.col("tip_amount") < 0, 0.0).otherwise(F.col("tip_amount"))
    )

    # A2 finding: total_amount <= 0 found in ~200 records.
    # Action: Flag with boolean column; retain record for zone/time analysis.
    df = df.withColumn("is_invalid_total", F.col("total_amount") <= 0)

    # A2 finding: passenger_count == 0 in ~6000 rows (meter activated, no count entered).
    # Action: Impute with mode (1) — most common single-passenger trip pattern.
    df = df.withColumn(
        "passenger_count",
        F.when(
            (F.col("passenger_count").isNull()) | (F.col("passenger_count") == 0), 1
        ).otherwise(F.col("passenger_count"))
    )

    # A2 finding: passenger_count > 6 violates NYC TLC legal capacity.
    # Action: Cap at 6.
    df = df.withColumn("passenger_count", F.least(F.col("passenger_count"), F.lit(6)))

    # A2 finding: trip_distance == 0 in ~1200 rows (GPS failure / meter error).
    # Action: Replace with global median distance.
    dist_median = df.approxQuantile("trip_distance", [0.5], 0.01)[0]
    df = df.withColumn(
        "trip_distance",
        F.when(F.col("trip_distance") == 0, dist_median).otherwise(F.col("trip_distance"))
    )

    # A2 finding: fare_amount IQR outliers identified (~3% of records).
    # Action: Winsorise at 1st/99th percentile.
    fare_p1, fare_p99 = df.approxQuantile("fare_amount", [0.01, 0.99], 0.01)
    df = df.withColumn(
        "fare_amount",
        F.least(F.greatest(F.col("fare_amount"), F.lit(fare_p1)), F.lit(fare_p99))
    )

    # A2 finding: negative/zero trip durations exist (dropoff <= pickup).
    # Action: Derive duration and flag invalid records for exclusion.
    df = df.withColumn(
        "trip_duration_min",
        (F.unix_timestamp("tpep_dropoff_datetime") -
         F.unix_timestamp("tpep_pickup_datetime")) / 60.0
    )
    df = df.withColumn(
        "is_invalid_duration",
        F.col("trip_duration_min") <= 0
    )

    # ── 2.3  DERIVE NEW COLUMNS ───────────────────────────────────

    # Time dimensions
    df = (df
          .withColumn("pickup_hour",    F.hour("tpep_pickup_datetime"))
          .withColumn("pickup_dow",     F.dayofweek("tpep_pickup_datetime"))   # 1=Sun … 7=Sat
          .withColumn("pickup_month",   F.month("tpep_pickup_datetime"))
          .withColumn("pickup_date",    F.to_date("tpep_pickup_datetime"))
          .withColumn("pickup_week",    F.weekofyear("tpep_pickup_datetime"))
    )

    # Time-of-day flag (business question 3 — demand pattern by period)
    df = df.withColumn(
        "time_of_day",
        F.when(F.col("pickup_hour").between(6, 9),   "Morning Rush")
         .when(F.col("pickup_hour").between(10, 15), "Midday")
         .when(F.col("pickup_hour").between(16, 19), "Evening Rush")
         .when(F.col("pickup_hour").between(20, 23), "Night")
         .otherwise("Late Night")
    )

    # Weekend flag
    df = df.withColumn(
        "is_weekend",
        F.col("pickup_dow").isin([1, 7]).cast("boolean")
    )

    # Airport trip flag (business question 5)
    # RatecodeID 2 = JFK, 3 = Newark; airport_fee > 0 also signals airport
    df = df.withColumn(
        "is_airport_trip",
        ((F.col("RatecodeID").isin([2, 3])) |
         (F.col("airport_fee") > 0)).cast("boolean")
    )

    # Tip percentage (for business question 2)
    df = df.withColumn(
        "tip_pct",
        F.when(F.col("fare_amount") > 0,
               F.round(F.col("tip_amount") / F.col("fare_amount") * 100, 2)
        ).otherwise(0.0)
    )

    # Revenue total (fare + surcharges, excluding tip)
    df = df.withColumn(
        "revenue_excl_tip",
        F.col("fare_amount") + F.coalesce("extra", F.lit(0)) +
        F.coalesce("mta_tax", F.lit(0)) +
        F.coalesce("tolls_amount", F.lit(0)) +
        F.coalesce("improvement_surcharge", F.lit(0)) +
        F.coalesce("congestion_surcharge", F.lit(0)) +
        F.coalesce("airport_fee", F.lit(0))
    )

    # Distance bucket
    df = df.withColumn(
        "distance_bucket",
        F.when(F.col("trip_distance") < 1,  "< 1 mi")
         .when(F.col("trip_distance") < 3,  "1–3 mi")
         .when(F.col("trip_distance") < 5,  "3–5 mi")
         .when(F.col("trip_distance") < 10, "5–10 mi")
         .otherwise("> 10 mi")
    )

    # Normalise payment_type to label
    df = df.withColumn(
        "payment_label",
        F.when(F.col("payment_type") == 1, "Credit Card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No Charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .otherwise("Unknown")
    )

    # Normalise RatecodeID to label
    df = df.withColumn(
        "rate_label",
        F.when(F.col("RatecodeID") == 1, "Standard")
         .when(F.col("RatecodeID") == 2, "JFK")
         .when(F.col("RatecodeID") == 3, "Newark")
         .when(F.col("RatecodeID") == 4, "Nassau/Westchester")
         .when(F.col("RatecodeID") == 5, "Negotiated")
         .when(F.col("RatecodeID") == 6, "Group Ride")
         .otherwise("Unknown")
    )

    # Drop rows with invalid duration (cannot be corrected — A2 finding)
    before = df.count()
    df = df.filter(~F.col("is_invalid_duration"))
    after = df.count()
    log.info("  Dropped %d invalid-duration rows (%d → %d)", before - after, before, after)

    log.info("  Transform complete. Final row count: %d", after)
    return df


# ─────────────────────────────────────────────────────────────────
# STEP 3 — MODEL (Star Schema Tables)
# ─────────────────────────────────────────────────────────────────
def model(df, spark: SparkSession):
    """Split transformed DataFrame into fact + dimension tables."""
    log.info("=== STEP 3: MODEL (Star Schema) ===")

    # ── DIM_TIME ─────────────────────────────────────────────────
    dim_time = (df.select(
        F.col("pickup_date").alias("date_key"),
        F.col("pickup_hour").alias("hour"),
        F.col("pickup_dow").alias("day_of_week"),
        F.col("pickup_month").alias("month"),
        F.col("pickup_week").alias("week_of_year"),
        F.col("time_of_day"),
        F.col("is_weekend"),
        F.date_format("pickup_date", "EEEE").alias("day_name"),
        F.date_format("pickup_date", "MMMM").alias("month_name"),
    ).distinct())
    log.info("  DimTime rows: %d", dim_time.count())

    # ── DIM_LOCATION ─────────────────────────────────────────────
    pu_locs = df.select(
        F.col("PULocationID").alias("location_id"),
        F.lit("pickup").alias("location_type")
    )
    do_locs = df.select(
        F.col("DOLocationID").alias("location_id"),
        F.lit("dropoff").alias("location_type")
    )
    dim_location = pu_locs.union(do_locs).distinct()
    log.info("  DimLocation rows: %d", dim_location.count())

    # ── DIM_PAYMENT ──────────────────────────────────────────────
    dim_payment = (df.select(
        F.col("payment_type").alias("payment_key"),
        F.col("payment_label"),
        F.col("rate_label"),
        F.col("RatecodeID").alias("rate_code"),
    ).distinct())
    log.info("  DimPayment rows: %d", dim_payment.count())

    # ── DIM_VENDOR ───────────────────────────────────────────────
    vendor_names = {1: "Creative Mobile Technologies", 2: "VeriFone Inc."}
    dim_vendor = spark.createDataFrame([
        (k, v) for k, v in vendor_names.items()
    ], ["vendor_id", "vendor_name"])
    log.info("  DimVendor rows: %d", dim_vendor.count())

    # ── FACT_TRIPS ───────────────────────────────────────────────
    fact_trips = df.select(
        F.monotonically_increasing_id().alias("trip_id"),
        F.col("pickup_date").alias("date_key"),
        F.col("PULocationID").alias("pickup_location_id"),
        F.col("DOLocationID").alias("dropoff_location_id"),
        F.col("payment_type").alias("payment_key"),
        F.col("VendorID").alias("vendor_id"),
        # Measures
        F.col("trip_distance"),
        F.col("trip_duration_min"),
        F.col("passenger_count"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("tip_pct"),
        F.col("tolls_amount"),
        F.col("total_amount"),
        F.col("revenue_excl_tip"),
        F.col("congestion_surcharge"),
        F.col("airport_fee"),
        # Flags / buckets
        F.col("time_of_day"),
        F.col("is_weekend"),
        F.col("is_airport_trip"),
        F.col("is_invalid_total"),
        F.col("distance_bucket"),
        F.col("pickup_hour"),
        F.col("pickup_month"),
        F.col("pickup_dow"),
    )
    log.info("  FactTrips rows: %d", fact_trips.count())

    # Cache fact table — reused in analytics (Optimization technique 1)
    fact_trips.cache()
    log.info("  FactTrips cached in memory.")

    return fact_trips, dim_time, dim_location, dim_payment, dim_vendor


# ─────────────────────────────────────────────────────────────────
# STEP 4 — LOAD (write Parquet to HDFS / local)
# ─────────────────────────────────────────────────────────────────
def load(fact_trips, dim_time, dim_location, dim_payment, dim_vendor,
         processed_base: str):
    log.info("=== STEP 4: LOAD ===")

    tables = {
        "fact_trips":    (fact_trips,    "pickup_month"),   # partition by month
        "dim_time":      (dim_time,      None),
        "dim_location":  (dim_location,  None),
        "dim_payment":   (dim_payment,   None),
        "dim_vendor":    (dim_vendor,    None),
    }

    counts = {}
    for name, (df_tbl, partition_col) in tables.items():
        path = f"{processed_base}/{name}"
        writer = df_tbl.write.mode("overwrite").format("parquet")
        if partition_col:
            # Optimization technique 2: Partitioning by month for efficient
            # predicate pushdown in time-range queries
            writer = writer.partitionBy(partition_col)
            log.info("  Writing %s partitioned by %s → %s", name, partition_col, path)
        else:
            log.info("  Writing %s → %s", name, path)
        writer.save(path)
        cnt = df_tbl.count()
        counts[name] = cnt
        log.info("  ✔ %s written: %d rows", name, cnt)

    return counts


# ─────────────────────────────────────────────────────────────────
# STEP 5 — VALIDATE
# ─────────────────────────────────────────────────────────────────
def validate(spark: SparkSession, processed_base: str,
             counts_written: dict, raw_count: int):
    log.info("=== STEP 5: VALIDATE ===")

    key_not_null_cols = {
        "fact_trips":   ["date_key", "pickup_location_id", "fare_amount", "total_amount"],
        "dim_time":     ["date_key", "hour"],
        "dim_location": ["location_id"],
        "dim_payment":  ["payment_key"],
        "dim_vendor":   ["vendor_id"],
    }

    all_ok = True
    for name, not_null_cols in key_not_null_cols.items():
        path = f"{processed_base}/{name}"
        try:
            df_check = spark.read.parquet(path)
            actual_count = df_check.count()
            expected = counts_written.get(name, -1)

            # Row count check
            if actual_count == expected:
                log.info("  ✔ [%s] Row count OK: %d", name, actual_count)
            else:
                log.error("  ✗ [%s] Count mismatch: expected %d, got %d",
                          name, expected, actual_count)
                all_ok = False

            # Null assertion
            for col in not_null_cols:
                if col not in df_check.columns:
                    continue
                null_count = df_check.filter(F.col(col).isNull()).count()
                if null_count == 0:
                    log.info("  ✔ [%s.%s] No nulls.", name, col)
                else:
                    log.warning("  ⚠ [%s.%s] %d nulls found.", name, col, null_count)

        except Exception as e:
            log.error("  ✗ [%s] Validation failed: %s", name, e)
            all_ok = False

    # Row count traceability
    fact_count = counts_written.get("fact_trips", 0)
    dropped = raw_count - fact_count
    log.info("─" * 60)
    log.info("Row Count Summary:")
    log.info("  Raw (A2 ingested)        : %d", raw_count)
    log.info("  Post-ETL (fact_trips)    : %d", fact_count)
    log.info("  Dropped (invalid duration): %d (%.2f%%)",
             dropped, dropped / raw_count * 100)
    log.info("─" * 60)

    if all_ok:
        log.info("✔ All validation checks passed.")
    else:
        log.warning("⚠ Some validation checks failed — review log.")


# ─────────────────────────────────────────────────────────────────
# OPTIMIZATION SHOWCASE
# ─────────────────────────────────────────────────────────────────
def demonstrate_optimizations(spark: SparkSession, fact_trips, dim_vendor,
                               processed_base: str):
    log.info("=== OPTIMIZATION DEMONSTRATIONS ===")

    # ── OPT 1: CACHING ───────────────────────────────────────────
    # fact_trips already cached in model() step.
    # Below we demonstrate re-using the cached DF without re-reading HDFS.
    log.info("Opt-1 CACHING: fact_trips is cached; re-count reads from memory.")
    cached_count = fact_trips.count()
    log.info("  Cached count: %d (no HDFS read triggered)", cached_count)

    # ── OPT 2: BROADCAST JOIN ────────────────────────────────────
    # dim_vendor is tiny (2 rows). Broadcast it to avoid shuffle.
    log.info("Opt-2 BROADCAST JOIN: joining dim_vendor (2 rows) via broadcast().")
    enriched = fact_trips.join(
        F.broadcast(dim_vendor),
        fact_trips["vendor_id"] == dim_vendor["vendor_id"],
        how="left"
    )
    vendor_revenue = (enriched.groupBy("vendor_name")
                               .agg(F.round(F.sum("total_amount"), 2).alias("total_revenue"))
                               .orderBy("total_revenue", ascending=False))
    vendor_revenue.show(truncate=False)
    log.info("  Broadcast join complete.")

    # ── OPT 3: QUERY PLAN ANALYSIS ───────────────────────────────
    log.info("Opt-3 QUERY PLAN (.explain(True)) for vendor revenue query:")
    enriched.groupBy("vendor_name").agg(F.sum("total_amount")).explain(True)


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    start = datetime.now()
    log.info("╔═══════════════════════════════════════════════╗")
    log.info("║  BDA A3 — PySpark ETL Pipeline  NYC Taxi      ║")
    log.info("╚═══════════════════════════════════════════════╝")

    raw_path, processed_base = get_paths()
    spark = create_spark()

    try:
        df_raw, raw_count       = extract(spark, raw_path)
        df_clean                = transform(df_raw, spark)
        fact, d_time, d_loc, d_pay, d_ven = model(df_clean, spark)
        counts                  = load(fact, d_time, d_loc, d_pay, d_ven, processed_base)
        validate(spark, processed_base, counts, raw_count)
        demonstrate_optimizations(spark, fact, d_ven, processed_base)
        elapsed = (datetime.now() - start).total_seconds()
        log.info("ETL pipeline finished in %.1f s", elapsed)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
