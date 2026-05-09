"""MongoDB handler for the benchmark.

Mirrors `MySQLHandler` so the runner can treat them interchangeably.
We intentionally don't copy the `id` field across — each MongoDB document
gets its default `_id` (per spec).
"""

from __future__ import annotations

from pymongo import MongoClient
from pymongo.errors import OperationFailure

import config


class MongoHandler:
    name = "mongodb"

    def __init__(self) -> None:
        self.client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=5000)
        # Force a server round-trip so we fail fast on bad URI
        self.client.admin.command("ping")
        self.db = self.client[config.MONGO_DB_NAME]
        self.coll = self.db[config.MONGO_COLLECTION]

    # --- schema -----------------------------------------------------------

    def reset_schema(self) -> None:
        self.db.drop_collection(config.MONGO_COLLECTION)
        # Recreate explicitly so that count() is well-defined immediately.
        self.db.create_collection(config.MONGO_COLLECTION)
        self.coll = self.db[config.MONGO_COLLECTION]

    def bulk_insert(self, records: list[dict]) -> None:
        # Copy to avoid pymongo mutating caller dicts (it adds _id in place).
        docs = [dict(r) for r in records]
        if docs:
            # ordered=False lets the server insert in parallel and continue past
            # any single failure; for a deterministic dataset this is purely a
            # throughput optimization.
            self.coll.insert_many(docs, ordered=False)

    def count(self) -> int:
        return self.coll.count_documents({})

    # --- index ------------------------------------------------------------

    def create_email_index(self) -> None:
        self.coll.create_index("email", name="idx_email")

    def drop_email_index(self) -> None:
        try:
            self.coll.drop_index("idx_email")
        except OperationFailure:
            pass

    # --- timed CRUD -------------------------------------------------------

    def op_create(self, record: dict) -> None:
        # Copy: insert_one writes the auto-assigned _id back into the dict,
        # which would otherwise mutate the caller's reusable trial record.
        self.coll.insert_one(dict(record))

    def op_read(self, email: str):
        return self.coll.find_one({"email": email})

    def op_update(self, email: str, new_balance: float) -> None:
        self.coll.update_one({"email": email}, {"$set": {"balance": new_balance}})

    def op_delete(self, email: str) -> None:
        self.coll.delete_one({"email": email})

    # --- helpers ----------------------------------------------------------

    def fetch_records_by_emails(self, emails: list[str]) -> list[dict]:
        if not emails:
            return []
        cursor = self.coll.find(
            {"email": {"$in": emails}},
            {"_id": 0, "name": 1, "email": 1, "city": 1, "age": 1, "registration_date": 1, "balance": 1},
        )
        return list(cursor)

    def server_version(self) -> str:
        return self.client.server_info()["version"]

    def close(self) -> None:
        self.client.close()
