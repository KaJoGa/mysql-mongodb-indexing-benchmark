"""Deterministic dataset generator shared by both database loaders.

The same record list is loaded into MySQL and MongoDB so any timing
difference is engine-attributable, not data-attributable.

Email scheme is deterministic (`user_{i:08d}@example.com`) so trial targets
can be computed by index without an extra round-trip to the database.
"""

import random
from datetime import datetime

from faker import Faker

from config import RANDOM_SEED


def email_at(index: int) -> str:
    """Deterministic, unique email for a record at a given global index."""
    return f"user_{index:08d}@example.com"


def generate_records(n: int, seed: int = RANDOM_SEED, start_index: int = 0) -> list[dict]:
    """Generate n records starting at `start_index`.

    `start_index` lets callers ask for fresh records that don't collide with
    a bulk-loaded dataset (e.g., for the Create operation trials).
    """
    fake = Faker()
    Faker.seed(seed + start_index)
    random.seed(seed + start_index)

    records = []
    for i in range(n):
        idx = start_index + i
        records.append(
            {
                "name": fake.name(),
                "email": email_at(idx),
                "city": fake.city(),
                "age": random.randint(18, 80),
                "registration_date": fake.date_time_between(start_date="-5y", end_date=datetime(2026, 1, 1)),
                "balance": round(random.uniform(0.0, 100_000.0), 2),
            }
        )
    return records


def pick_trial_indices(dataset_size: int, n_trials: int, seed: int = RANDOM_SEED) -> list[int]:
    """Pick `n_trials` distinct indices spread across the dataset.

    Spread reduces the chance of a single buffer-pool page serving every
    lookup, which would mask real index-vs-scan differences.
    """
    rng = random.Random(seed + 1)
    indices = rng.sample(range(dataset_size), n_trials)
    indices.sort()
    return indices
