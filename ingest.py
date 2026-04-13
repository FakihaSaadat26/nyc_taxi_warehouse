#!/usr/bin/env python3
"""
ingest.py - Automated HDFS Ingestion Pipeline
CS-404 Big Data Analytics - Assignment 02
Dataset: NYC Taxi Trip Records (Yellow Cab) - 2023
"""

import os
import sys
import logging
import subprocess
import hashlib
import urllib.request
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────
DATASET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
)
LOCAL_DIR = Path("./data")
LOCAL_FILE = LOCAL_DIR / "yellow_tripdata_2023-01.parquet"
CONVERTED_CSV = LOCAL_DIR / "yellow_tripdata_2023-01.csv"

HDFS_BASE = "/warehouse/raw/nyc_taxi/year=2026/month=04"
MIN_ROWS = 500_000
MIN_FILE_SIZE_MB = 10
EXPECTED_ENCODING = "utf-8"

# ─────────────────────────────────────────────────────────────────
# LOGGING SETUP
# ─────────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(message)s"
logging.basicConfig(
    level=logging.INFO,
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("ingest.log", mode="w"),
    ],
)
log = logging.getLogger("ingest")


# ─────────────────────────────────────────────────────────────────
# STEP 1: LOAD / DOWNLOAD
# ─────────────────────────────────────────────────────────────────
def load_dataset() -> Path:
    """Download the dataset if it is not already present locally."""
    log.info("=== STEP 1: LOAD ===")
    LOCAL_DIR.mkdir(parents=True, exist_ok=True)

    if LOCAL_FILE.exists():
        log.info("Dataset already present at %s — skipping download.", LOCAL_FILE)
        return LOCAL_FILE

    log.info("Downloading dataset from %s …", DATASET_URL)
    try:
        def _progress(block_num, block_size, total_size):
            downloaded = block_num * block_size
            pct = downloaded / total_size * 100 if total_size > 0 else 0
            if block_num % 500 == 0:
                log.info("  Download progress: %.1f%%", pct)

        urllib.request.urlretrieve(DATASET_URL, LOCAL_FILE, reporthook=_progress)
        log.info("Download complete: %s", LOCAL_FILE)
    except Exception as exc:
        log.error("Download failed: %s", exc)
        raise

    # Convert parquet → CSV so downstream steps work with text format
    log.info("Converting parquet to CSV …")
    try:
        import pandas as pd  # noqa: PLC0415
        df = pd.read_parquet(LOCAL_FILE)
        df.to_csv(CONVERTED_CSV, index=False)
        log.info("Converted to CSV: %s  (%d rows)", CONVERTED_CSV, len(df))
        return CONVERTED_CSV
    except Exception as exc:
        log.warning("Could not convert to CSV (%s); will upload parquet directly.", exc)
        return LOCAL_FILE


# ─────────────────────────────────────────────────────────────────
# STEP 2: VALIDATE
# ─────────────────────────────────────────────────────────────────
def validate_dataset(file_path: Path) -> None:
    """Pre-upload validation: size, extension, encoding, row count."""
    log.info("=== STEP 2: VALIDATE ===")
    errors: list[str] = []

    # 2a. File existence
    if not file_path.exists():
        log.error("File not found: %s", file_path)
        raise FileNotFoundError(file_path)
    log.info("✔ File exists: %s", file_path)

    # 2b. Extension check
    allowed_extensions = {".csv", ".parquet", ".json", ".tsv"}
    if file_path.suffix.lower() not in allowed_extensions:
        errors.append(f"Unexpected file extension: {file_path.suffix}")
    else:
        log.info("✔ File extension valid: %s", file_path.suffix)

    # 2c. File size check
    size_bytes = file_path.stat().st_size
    size_mb = size_bytes / (1024 ** 2)
    log.info("   File size: %.2f MB", size_mb)
    if size_mb < MIN_FILE_SIZE_MB:
        errors.append(
            f"File too small ({size_mb:.2f} MB < {MIN_FILE_SIZE_MB} MB minimum)."
        )
    else:
        log.info("✔ File size acceptable (%.2f MB)", size_mb)

    # 2d. Encoding detection (CSV only)
    if file_path.suffix.lower() == ".csv":
        try:
            import chardet  # noqa: PLC0415
            with open(file_path, "rb") as fh:
                raw = fh.read(100_000)
            detection = chardet.detect(raw)
            encoding = detection.get("encoding", "unknown")
            confidence = detection.get("confidence", 0)
            log.info(
                "   Detected encoding: %s (confidence: %.0f%%)",
                encoding,
                confidence * 100,
            )
            if encoding and encoding.lower().replace("-", "") not in {"utf8", "ascii"}:
                log.warning(
                    "⚠ Encoding is %s, not UTF-8. Re-encoding may be needed.", encoding
                )
            else:
                log.info("✔ Encoding is UTF-8 / ASCII compatible.")
        except ImportError:
            log.warning("chardet not installed; skipping encoding detection.")

    # 2e. Row count verification (CSV only)
    if file_path.suffix.lower() == ".csv":
        try:
            import pandas as pd  # noqa: PLC0415
            sample = pd.read_csv(file_path, nrows=10)
            log.info("   Sample columns: %s", list(sample.columns))

            # Fast row count via line count
            with open(file_path, "rb") as fh:
                row_count = sum(1 for _ in fh) - 1  # subtract header
            log.info("   Row count: %d", row_count)

            if row_count < MIN_ROWS:
                errors.append(
                    f"Too few rows ({row_count:,} < {MIN_ROWS:,} minimum)."
                )
            else:
                log.info("✔ Row count meets minimum: %d rows", row_count)
        except Exception as exc:
            log.error("Row count check failed: %s", exc)
            errors.append(str(exc))
    else:
        # Parquet: use pandas
        try:
            import pandas as pd  # noqa: PLC0415
            df = pd.read_parquet(file_path)
            log.info("   Parquet row count: %d", len(df))
            if len(df) < MIN_ROWS:
                errors.append(
                    f"Too few rows ({len(df):,} < {MIN_ROWS:,} minimum)."
                )
            else:
                log.info("✔ Row count meets minimum: %d rows", len(df))
        except Exception as exc:
            log.warning("Could not verify parquet row count: %s", exc)

    # 2f. MD5 checksum
    md5 = hashlib.md5()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            md5.update(chunk)
    log.info("   MD5 checksum: %s", md5.hexdigest())

    if errors:
        for e in errors:
            log.error("Validation error: %s", e)
        raise ValueError(f"Validation failed with {len(errors)} error(s). See log.")

    log.info("✔ All validation checks passed.")


# ─────────────────────────────────────────────────────────────────
# STEP 3: UPLOAD TO HDFS
# ─────────────────────────────────────────────────────────────────
def _run_hdfs(args: list[str]) -> subprocess.CompletedProcess:
    """Execute an HDFS shell command via subprocess."""
    cmd = ["C:\\hadoop-3.2.2\\bin\\hdfs.cmd", "dfs"] + args
    log.info("Running: %s", " ".join(cmd))
    result = subprocess.run(
        cmd, capture_output=True, text=True
    )
    if result.returncode != 0:
        log.error("HDFS command failed (rc=%d): %s", result.returncode, result.stderr)
        raise RuntimeError(result.stderr)
    if result.stdout:
        log.info("HDFS stdout: %s", result.stdout.strip())
    return result


def upload_to_hdfs(file_path: Path) -> str:
    """Create HDFS directory structure and upload the file."""
    log.info("=== STEP 3: UPLOAD ===")

    # 3a. Create target directory
    hdfs_dir = HDFS_BASE
    try:
        _run_hdfs(["-mkdir", "-p", hdfs_dir])
        log.info("✔ HDFS directory ensured: %s", hdfs_dir)
    except RuntimeError as exc:
        log.error("Could not create HDFS directory: %s", exc)
        raise

    # 3b. Upload file (overwrite if exists)
    hdfs_target = f"{hdfs_dir}/{file_path.name}"
    try:
        _run_hdfs(["-put", "-f", str(file_path), hdfs_target])
        log.info("✔ File uploaded to HDFS: %s", hdfs_target)
    except RuntimeError as exc:
        log.error("Upload failed: %s", exc)
        raise

    return hdfs_target


# ─────────────────────────────────────────────────────────────────
# STEP 4: ORGANISE & VERIFY
# ─────────────────────────────────────────────────────────────────
def organise_and_verify(hdfs_path: str) -> None:
    """Verify the uploaded file is visible in HDFS and log directory listing."""
    log.info("=== STEP 4: ORGANISE & VERIFY ===")

    # List the directory
    try:
        result = _run_hdfs(["-ls", "-h", HDFS_BASE])
        log.info("HDFS directory listing:\n%s", result.stdout)
    except RuntimeError:
        log.warning("Could not list HDFS directory.")

    # Confirm file exists
    try:
        _run_hdfs(["-test", "-e", hdfs_path])
        log.info("✔ Confirmed file exists in HDFS: %s", hdfs_path)
    except RuntimeError:
        log.error("File does NOT exist at %s — upload may have failed.", hdfs_path)
        raise

    # Check HDFS file size
    try:
        result = _run_hdfs(["-du", "-h", hdfs_path])
        log.info("HDFS file size: %s", result.stdout.strip())
    except RuntimeError:
        log.warning("Could not retrieve file size from HDFS.")

    log.info("✔ Ingestion pipeline completed successfully.")
    log.info("HDFS target path: %s", hdfs_path)


# ─────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────
def main():
    start = datetime.now()
    log.info("╔══════════════════════════════════════════════════╗")
    log.info("║   BDA Assignment 02 — HDFS Ingestion Pipeline   ║")
    log.info("║   Dataset: NYC Yellow Taxi Trip Records 2023     ║")
    log.info("╚══════════════════════════════════════════════════╝")
    log.info("Pipeline started at %s", start.isoformat())

    try:
        file_path = load_dataset()
        validate_dataset(file_path)
        hdfs_path = upload_to_hdfs(file_path)
        organise_and_verify(hdfs_path)
        elapsed = (datetime.now() - start).total_seconds()
        log.info("Pipeline finished in %.1f seconds.", elapsed)
        sys.exit(0)
    except Exception as exc:
        log.critical("Pipeline aborted: %s", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
