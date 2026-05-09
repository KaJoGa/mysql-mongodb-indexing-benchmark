# SQL vs NoSQL Indexing Benchmark

Python benchmark suite comparing CRUD performance of **MySQL 8.0 (InnoDB)** vs **MongoDB 7.0**, with and without a B-tree index on `email`, across 10K / 50K / 100K records. Output is a CSV of per-trial timings plus summary stats and figures suitable for an IEEE-format paper.

## Paper

This repository contains the code, raw data, and figures supporting:

> K. J. Gamaliel and R. I. Kierana, "Evaluating SQL and NoSQL Database
> Performance with and without Indexing: A Case Study Using MySQL and
> MongoDB," Computer Science Department, Bina Nusantara University, 2026.

Once the paper is published, please cite the paper rather than this
repository directly.

## Experimental design

| Variable      | Values                                |
|---------------|---------------------------------------|
| Database      | MySQL 8.0, MongoDB 7.0                |
| Dataset size  | 10,000 / 50,000 / 100,000             |
| Indexing      | B-tree on `email` / no secondary idx  |
| CRUD op       | Create, Read, Update, Delete          |

2 × 3 × 2 × 4 = **48 cells**, 10 trials each = **480 measurements** in a full run. Operations are run in the order C → R → U → D within each `(db, size, indexing)` setup, with the deleted records re-inserted before any next op to keep state consistent (per spec).

Timing uses `time.perf_counter()`, recorded in milliseconds. Connection setup is excluded; the database call (and its commit, where applicable) is included.

## Project layout

```
benchmark/
├── README.md
├── requirements.txt        # pip freeze of the .venv
├── .gitignore
├── config.py               # connection params, seeds, sizes
├── data_generator.py       # Faker-based deterministic dataset
├── mysql_handler.py        # MySQL setup + CRUD wrappers
├── mongo_handler.py        # MongoDB setup + CRUD wrappers
├── benchmark_runner.py     # main loop, --quick flag, resume
├── analyze.py              # post-hoc stats + plots
└── output/                 # benchmark results (committed, see paper data)
    ├── raw_timings.csv
    ├── raw_timings_quick.csv
    ├── summary_stats.csv
    ├── improvement_ratios.csv
    └── figures/
        ├── bar_create.png
        ├── bar_read.png
        ├── bar_update.png
        ├── bar_delete.png
        └── heatmap_improvement.png
```

## Setup

Tested environment:

- Windows 11
- Python 3.12 (e.g. `py -3.12`)
- MySQL 8.0 on `localhost:3306`, InnoDB (Community Server)
- MongoDB 7.0 on `localhost:27017`

```powershell
# from the project root
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

### Database credentials

`config.py` reads MySQL credentials from environment variables, with `root` / empty password as default. Set them in your shell **before** running the benchmark if your MySQL needs a password:

```powershell
$env:MYSQL_USER = "root"
$env:MYSQL_PASS = "your_password"
$env:MONGO_URI  = "mongodb://localhost:27017/"   # only if non-default
```

The benchmark creates a database named `benchmark_db` (MySQL) / a database called `benchmark_db` with collection `users` (MongoDB) automatically. The MySQL user must have `CREATE`, `DROP`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`, and `INDEX` privileges on it.

## Running

### Quick smoke test (≈ seconds)

```powershell
python benchmark_runner.py --quick
```

Runs **1 trial per cell** with **only the 10K dataset**. Writes to `output/raw_timings_quick.csv` (separate file so it doesn't pollute a real full run). At the end it prints actual server versions and library versions — keep these for the paper's methodology section.

### Full run

```powershell
python benchmark_runner.py
```

Writes to `output/raw_timings.csv`. The run is **resumable**: if interrupted, the next invocation skips setups that already have all 4 ops × 10 trials in the CSV. Partially-completed setups are pruned and redone (so no setup contributes mixed-state rows).

### Analysis

```powershell
python analyze.py                              # uses output/raw_timings.csv
python analyze.py --input output/raw_timings_quick.csv   # for the quick run
```

Outputs:

- `output/summary_stats.csv` — mean, std, min, max, n per cell
- `output/improvement_ratios.csv` — `(mean_no_index − mean_indexed) / mean_no_index × 100`
- `output/figures/bar_<op>.png` — 4 grouped bar charts (one per CRUD), 300 DPI
- `output/figures/heatmap_improvement.png` — improvement-ratio heatmap, 300 DPI

## Customization

All experimental knobs live in `config.py` — edit the constants there, no other code changes needed. The bar charts auto-relabel sizes (1 K / 1 M / etc.).

```python
# config.py
DATASET_SIZES   = [10_000, 50_000, 100_000]   # add/remove entries to change cells
TRIALS_PER_CELL = 10                          # measurements per cell
WARMUP_TRIALS   = 1                           # untimed warmup before each op
RANDOM_SEED     = 42                          # change to re-roll dataset & trial picks
```

Examples:

- **Add a 500K dataset**: `DATASET_SIZES = [10_000, 50_000, 100_000, 500_000]`
- **Smaller paper-size sweep**: `DATASET_SIZES = [1_000, 5_000, 25_000]`
- **More precise timings**: `TRIALS_PER_CELL = 30` (and expect ~3× longer run)
- **Re-roll data without changing sizes**: `RANDOM_SEED = 123`

After editing `config.py`, **delete `output/raw_timings.csv` first** — the runner's resume logic compares against trial counts in the existing CSV, so leaving it in place will mix old and new measurements.

```powershell
Remove-Item output\raw_timings.csv -ErrorAction SilentlyContinue
python benchmark_runner.py
python analyze.py
```

Estimated run time scales roughly linearly with `sum(DATASET_SIZES)` × `TRIALS_PER_CELL`. The current default takes ~5–10 min on the target hardware.

## Reproducibility

- `Faker(seed=42)` and `random.seed(42)` everywhere
- Same generated record list is loaded into both databases
- Email scheme is deterministic (`user_{i:08d}@example.com`) so trial-target emails are computed by index, not picked randomly from the live DB
- Trial-target indices are picked by a separate seeded RNG (`seed + 1`) and sorted, so the order is stable across runs
- `time.perf_counter()` only; ms = `(t_end − t_start) * 1000`

## Authors

- **Kalev Jordan Gamaliel** — kalev.gamaliel@binus.ac.id
- **Reiki Indrasyahdewa Kierana** — reiki.kierana@binus.ac.id

Computer Science Department, Bina Nusantara University, Indonesia.

Supervised by Dr. Ir. Alexander Agung Santoso Gunawan and
Rilo Chandra Pradana, S.Si., M.Kom.

## Troubleshooting

| Symptom | Likely cause / fix |
|---|---|
| `Access denied for user 'root'@'localhost' (using password: NO)` | Set `$env:MYSQL_PASS` before launching, or update `MYSQL_CONFIG` |
| `Can't connect to MySQL server on 'localhost:3306'` | MySQL service not running. Windows: `services.msc` → start MySQL |
| `pymongo.errors.ServerSelectionTimeoutError` | MongoDB not running, or wrong URI in `MONGO_URI` |
| `SERVER VERSION: ...-MariaDB` instead of MySQL | XAMPP ships MariaDB by default. Install MySQL 8.0 separately and stop the XAMPP MySQL module |
| Bulk insert seems slow (minutes for 100K) | Confirm InnoDB engine; we use `executemany` in 5K batches, expect ~5–15 s for 100K |
| Wrong Python interpreter | `.venv\Scripts\python.exe --version` should report 3.12.x; recreate the venv with `py -3.12 -m venv .venv` if not |

## Deviations from the original spec

- **Quick mode CSV path.** Quick-mode runs write to `output/raw_timings_quick.csv` rather than `output/raw_timings.csv`. This keeps a real full-run dataset safe from accidental overwrite and lets resume logic for the full run stay simple. Both modes can coexist on disk.
- **Setup scope (confirmed with the user).** Each `(database, dataset_size, indexing)` setup runs all four CRUD ops in sequence (C → R → U → D), rather than dropping/re-inserting per cell. This was explicitly confirmed; it matches the spec's "After Delete: re-insert the removed records before moving to the next operation type" wording. Twelve setups, four ops each, instead of forty-eight independent setups.
- **`improvement_ratios.csv`.** Not in the original deliverables list, but the heatmap is computed from these and we save them as CSV too so the paper can cite exact numbers without re-deriving them from the figure.
