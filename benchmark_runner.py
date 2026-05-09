"""Main benchmark loop.

Iterates 12 setups (database x dataset_size x indexing) and runs CRUD
operations within each. Per the agreed design:

  for each (db, size, indexed):
      drop & recreate
      bulk insert N records
      ensure index state
      pick 10 trial-target emails (deterministic)
      for op in [create, read, update, delete]:
          1 warmup trial (untimed)
          10 timed trials (perf_counter, ms)
          if op == delete: re-insert the deleted records before next op
      append rows to CSV

Resume: if all 4 ops of a setup have the expected trial count in the
existing CSV, that setup is skipped. Partially-completed setups are
purged from the CSV and redone.
"""

from __future__ import annotations

import argparse
import csv
import logging
import random
import sys
import time
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

import config
import data_generator
from mongo_handler import MongoHandler
from mysql_handler import MySQLHandler

OPERATIONS = ["create", "read", "update", "delete"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
# Quiet down third-party loggers that emit INFO-level chatter on every connect
for noisy in ("mysql.connector", "pymongo"):
    logging.getLogger(noisy).setLevel(logging.WARNING)
log = logging.getLogger("benchmark")


# --- CSV helpers ----------------------------------------------------------


def ensure_output_dirs() -> None:
    config.OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    config.FIGURES_DIR.mkdir(parents=True, exist_ok=True)


def init_csv_if_missing(path: Path) -> None:
    if not path.exists():
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(config.CSV_COLUMNS)


def load_existing_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def rewrite_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def append_rows(path: Path, rows: list[dict]) -> None:
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=config.CSV_COLUMNS)
        writer.writerows(rows)


# --- resume logic ---------------------------------------------------------


def setup_key(db: str, size: int, indexed: bool) -> tuple[str, int, str]:
    return (db, size, "indexed" if indexed else "no_index")


def determine_completed_setups(
    csv_path: Path, expected_trials: int
) -> set[tuple[str, int, str]]:
    """Return the set of setups that already have a full result set.

    A setup is "complete" iff every operation has at least `expected_trials`
    rows. Partial setups are pruned from the CSV in-place so the rerun
    starts from a clean state for those setups.
    """
    rows = load_existing_rows(csv_path)
    if not rows:
        return set()

    by_setup: dict[tuple[str, int, str], dict[str, int]] = {}
    for r in rows:
        key = (r["database"], int(r["dataset_size"]), r["indexing"])
        by_setup.setdefault(key, {}).setdefault(r["operation"], 0)
        by_setup[key][r["operation"]] += 1

    complete = set()
    for key, op_counts in by_setup.items():
        if all(op_counts.get(op, 0) >= expected_trials for op in OPERATIONS):
            complete.add(key)

    incomplete = set(by_setup) - complete
    if incomplete:
        log.info("Pruning %d partial setup(s) from CSV before resuming", len(incomplete))
        kept = [
            r
            for r in rows
            if (r["database"], int(r["dataset_size"]), r["indexing"]) in complete
        ]
        rewrite_csv(csv_path, kept)

    return complete


# --- core run -------------------------------------------------------------


def run_one_setup(
    handler,
    size: int,
    indexed: bool,
    n_trials: int,
    csv_path: Path,
) -> None:
    db_name = handler.name
    indexing_label = "indexed" if indexed else "no_index"
    log.info(
        "Setup: db=%s size=%d indexing=%s -> drop, insert, %sindex",
        db_name,
        size,
        indexing_label,
        "" if indexed else "no ",
    )

    # ------ data preparation
    # Generate `size + n_trials` records up-front: the first `size` go into the
    # bulk-loaded dataset, the trailing `n_trials` are reserved as fresh records
    # for the Create-op trials. Pre-generating them in one call keeps emails
    # globally unique across the whole benchmark via `email_at(i)`.
    all_records = data_generator.generate_records(size + n_trials, seed=config.RANDOM_SEED)
    bulk_records = all_records[:size]
    create_records = all_records[size:]

    trial_indices = data_generator.pick_trial_indices(size, n_trials, seed=config.RANDOM_SEED)
    trial_emails = [data_generator.email_at(i) for i in trial_indices]

    # ------ schema + bulk load
    handler.reset_schema()
    t0 = time.perf_counter()
    handler.bulk_insert(bulk_records)
    elapsed = time.perf_counter() - t0
    log.info("  bulk inserted %d records in %.2fs", size, elapsed)

    actual = handler.count()
    if actual != size:
        raise RuntimeError(
            f"Sanity check failed: expected {size} rows after bulk insert, got {actual}"
        )

    # ------ index
    handler.drop_email_index()
    if indexed:
        handler.create_email_index()
        log.info("  created idx_email")

    # Pre-fetch full records for the deletion trials so we can restore them
    # afterwards (per spec, before moving on to any next op).
    delete_records = handler.fetch_records_by_emails(trial_emails)
    if len(delete_records) != n_trials:
        raise RuntimeError(
            f"Could not pre-fetch all {n_trials} delete-target records "
            f"(got {len(delete_records)})"
        )

    # Random new balances are deterministic given a fixed seed.
    rng = random.Random(config.RANDOM_SEED + 7)
    new_balances = [round(rng.uniform(0.0, 100_000.0), 2) for _ in range(n_trials)]

    # ------ run each operation
    pending_rows: list[dict] = []
    for op in OPERATIONS:
        log.info("  op=%s starting (warmup + %d trials)", op, n_trials)

        # 1 warmup trial (untimed) using the first trial target. This stabilizes
        # connections and warms caches the same way for every cell.
        if op == "create":
            warmup_record = dict(create_records[0])
            warmup_record["email"] = data_generator.email_at(size + n_trials)  # off-list
            handler.op_create(warmup_record)
            handler.op_delete(warmup_record["email"])  # undo warmup so it doesn't
            # affect downstream counts beyond what trial-create will add.
        elif op == "read":
            handler.op_read(trial_emails[0])
        elif op == "update":
            handler.op_update(trial_emails[0], new_balances[0])
        elif op == "delete":
            # Warmup delete on a non-trial record so trial deletes still find
            # all 10 of their pre-selected emails.
            warmup_email = data_generator.email_at(size + n_trials)
            warmup_record = dict(create_records[0])
            warmup_record["email"] = warmup_email
            handler.op_create(warmup_record)
            handler.op_delete(warmup_email)

        # 10 timed trials
        bar = tqdm(range(n_trials), desc=f"    {op}", leave=False)
        for trial_no in bar:
            try:
                if op == "create":
                    rec = create_records[trial_no]
                    t0 = time.perf_counter()
                    handler.op_create(rec)
                    t1 = time.perf_counter()
                elif op == "read":
                    email = trial_emails[trial_no]
                    t0 = time.perf_counter()
                    handler.op_read(email)
                    t1 = time.perf_counter()
                elif op == "update":
                    email = trial_emails[trial_no]
                    t0 = time.perf_counter()
                    handler.op_update(email, new_balances[trial_no])
                    t1 = time.perf_counter()
                elif op == "delete":
                    email = trial_emails[trial_no]
                    t0 = time.perf_counter()
                    handler.op_delete(email)
                    t1 = time.perf_counter()
                else:
                    raise AssertionError(op)

                ms = (t1 - t0) * 1000.0
                pending_rows.append(
                    {
                        "database": db_name,
                        "dataset_size": size,
                        "indexing": indexing_label,
                        "operation": op,
                        "trial_no": trial_no + 1,
                        "execution_time_ms": f"{ms:.4f}",
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                    }
                )
            except Exception as e:  # log & continue per spec
                log.warning(
                    "Trial failed (db=%s size=%d %s op=%s trial=%d): %s",
                    db_name,
                    size,
                    indexing_label,
                    op,
                    trial_no + 1,
                    e,
                )

        # After Delete, re-insert the removed records (per spec). The next
        # operation type, if any, then sees the original dataset state.
        if op == "delete":
            for rec in delete_records:
                handler.op_create(rec)

    append_rows(csv_path, pending_rows)
    log.info("  appended %d rows to CSV", len(pending_rows))


def run(quick: bool) -> tuple[Path, dict]:
    ensure_output_dirs()
    csv_path = config.RAW_CSV_QUICK_PATH if quick else config.RAW_CSV_PATH
    sizes = [10_000] if quick else config.DATASET_SIZES
    n_trials = 1 if quick else config.TRIALS_PER_CELL

    init_csv_if_missing(csv_path)
    completed = determine_completed_setups(csv_path, n_trials)

    setups: list[tuple[str, int, bool]] = []
    for db in ("mysql", "mongodb"):
        for size in sizes:
            for indexed in (True, False):
                setups.append((db, size, indexed))

    pending = [s for s in setups if setup_key(*s) not in completed]
    log.info(
        "%d setups total, %d already complete, %d pending",
        len(setups),
        len(completed),
        len(pending),
    )

    versions: dict = {}

    for i, (db, size, indexed) in enumerate(pending, start=1):
        log.info("[%d/%d] -- %s | %d records | %s", i, len(pending), db, size,
                 "indexed" if indexed else "no_index")

        if db == "mysql":
            handler = MySQLHandler()
        else:
            handler = MongoHandler()
        try:
            versions.setdefault(db, handler.server_version())
            run_one_setup(handler, size, indexed, n_trials, csv_path)
        finally:
            handler.close()

    return csv_path, versions


def print_summary(csv_path: Path, versions: dict, quick: bool) -> None:
    import importlib.metadata as md

    log.info("=" * 60)
    log.info("BENCHMARK %s COMPLETE", "QUICK" if quick else "FULL")
    log.info("=" * 60)
    log.info("Output CSV : %s", csv_path)
    log.info("Python     : %s", sys.version.split()[0])
    log.info("MySQL      : %s", versions.get("mysql", "n/a"))
    log.info("MongoDB    : %s", versions.get("mongodb", "n/a"))
    for pkg in ("mysql-connector-python", "pymongo", "faker", "pandas", "numpy", "matplotlib", "seaborn", "tqdm"):
        try:
            log.info("%-25s %s", pkg, md.version(pkg))
        except md.PackageNotFoundError:
            log.info("%-25s (not installed)", pkg)
    log.info("=" * 60)
    if quick:
        log.info("Quick mode validated. Script is ready for the full run "
                 "(remove --quick).")
    else:
        log.info("Full run finished. Run analyze.py next to produce stats and figures.")


def main() -> None:
    parser = argparse.ArgumentParser(description="MySQL vs MongoDB indexing benchmark")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick mode: 1 trial per cell, only the 10K dataset, separate CSV.",
    )
    args = parser.parse_args()

    csv_path, versions = run(quick=args.quick)
    print_summary(csv_path, versions, quick=args.quick)


if __name__ == "__main__":
    main()
