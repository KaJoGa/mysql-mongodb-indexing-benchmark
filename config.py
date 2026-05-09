"""Central configuration for the SQL vs NoSQL indexing benchmark.

Credentials may be overridden via environment variables. Constants here are
intentionally simple module-level values so every other module can import
without side effects.
"""

import os
from pathlib import Path

# --- Database connections -------------------------------------------------

MYSQL_CONFIG = {
    "host": "localhost",
    "port": 3306,
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASS", ""),
    "database": "benchmark_db",
}

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB_NAME = "benchmark_db"

# --- Schema names ---------------------------------------------------------

MYSQL_TABLE = "users"
MONGO_COLLECTION = "users"

# --- Experimental design --------------------------------------------------

DATASET_SIZES = [10_000, 50_000, 100_000]
TRIALS_PER_CELL = 10
WARMUP_TRIALS = 1
RANDOM_SEED = 42

# Insert batch size for MySQL executemany; well under default max_allowed_packet
MYSQL_INSERT_BATCH = 5_000

# --- Output paths ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "output"
FIGURES_DIR = OUTPUT_DIR / "figures"
RAW_CSV_PATH = OUTPUT_DIR / "raw_timings.csv"
RAW_CSV_QUICK_PATH = OUTPUT_DIR / "raw_timings_quick.csv"
SUMMARY_CSV_PATH = OUTPUT_DIR / "summary_stats.csv"

CSV_COLUMNS = [
    "database",
    "dataset_size",
    "indexing",
    "operation",
    "trial_no",
    "execution_time_ms",
    "timestamp",
]
