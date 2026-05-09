"""MySQL handler for the benchmark.

Provides a small class wrapping connection, schema setup, bulk insert,
index management, and timed CRUD operations. CRUD ops include the work
that must happen for the operation to be observable (e.g., commit on
writes, fetchone on reads).
"""

from __future__ import annotations

import mysql.connector

import config


class MySQLHandler:
    name = "mysql"

    def __init__(self) -> None:
        self._ensure_database()
        self.conn = mysql.connector.connect(**config.MYSQL_CONFIG, autocommit=False)
        self.cur = self.conn.cursor()

    @staticmethod
    def _ensure_database() -> None:
        # Connect without selecting a database so we can CREATE it on a fresh
        # MySQL install where `benchmark_db` doesn't exist yet.
        cfg = {k: v for k, v in config.MYSQL_CONFIG.items() if k != "database"}
        conn = mysql.connector.connect(**cfg)
        cur = conn.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS {config.MYSQL_CONFIG['database']}")
        conn.commit()
        cur.close()
        conn.close()

    # --- schema -----------------------------------------------------------

    def reset_schema(self) -> None:
        self.cur.execute(f"DROP TABLE IF EXISTS {config.MYSQL_TABLE}")
        self.cur.execute(
            f"""
            CREATE TABLE {config.MYSQL_TABLE} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) NOT NULL,
                city VARCHAR(255) NOT NULL,
                age INT NOT NULL,
                registration_date DATETIME NOT NULL,
                balance DOUBLE NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """
        )
        self.conn.commit()

    def bulk_insert(self, records: list[dict]) -> None:
        sql = (
            f"INSERT INTO {config.MYSQL_TABLE} "
            "(name, email, city, age, registration_date, balance) "
            "VALUES (%s, %s, %s, %s, %s, %s)"
        )
        rows = [
            (r["name"], r["email"], r["city"], r["age"], r["registration_date"], r["balance"])
            for r in records
        ]
        # Chunked executemany keeps each packet under MySQL's max_allowed_packet
        # and commits once at the end so 100K rows complete in seconds, not minutes.
        batch = config.MYSQL_INSERT_BATCH
        for i in range(0, len(rows), batch):
            self.cur.executemany(sql, rows[i : i + batch])
        self.conn.commit()

    def count(self) -> int:
        self.cur.execute(f"SELECT COUNT(*) FROM {config.MYSQL_TABLE}")
        (n,) = self.cur.fetchone()
        return n

    # --- index ------------------------------------------------------------

    def create_email_index(self) -> None:
        self.cur.execute(f"CREATE INDEX idx_email ON {config.MYSQL_TABLE}(email)")
        self.conn.commit()

    def drop_email_index(self) -> None:
        try:
            self.cur.execute(f"DROP INDEX idx_email ON {config.MYSQL_TABLE}")
            self.conn.commit()
        except mysql.connector.Error as e:
            # 1091: Can't DROP, doesn't exist. Safe to ignore so the runner
            # can call drop unconditionally before deciding to recreate.
            if e.errno != 1091:
                raise

    # --- timed CRUD -------------------------------------------------------

    def op_create(self, record: dict) -> None:
        self.cur.execute(
            f"INSERT INTO {config.MYSQL_TABLE} "
            "(name, email, city, age, registration_date, balance) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            (
                record["name"],
                record["email"],
                record["city"],
                record["age"],
                record["registration_date"],
                record["balance"],
            ),
        )
        self.conn.commit()

    def op_read(self, email: str):
        self.cur.execute(
            f"SELECT id, name, email, city, age, registration_date, balance "
            f"FROM {config.MYSQL_TABLE} WHERE email = %s",
            (email,),
        )
        return self.cur.fetchone()

    def op_update(self, email: str, new_balance: float) -> None:
        self.cur.execute(
            f"UPDATE {config.MYSQL_TABLE} SET balance = %s WHERE email = %s",
            (new_balance, email),
        )
        self.conn.commit()

    def op_delete(self, email: str) -> None:
        self.cur.execute(
            f"DELETE FROM {config.MYSQL_TABLE} WHERE email = %s",
            (email,),
        )
        self.conn.commit()

    # --- helpers ----------------------------------------------------------

    def fetch_records_by_emails(self, emails: list[str]) -> list[dict]:
        if not emails:
            return []
        placeholders = ",".join(["%s"] * len(emails))
        self.cur.execute(
            f"SELECT name, email, city, age, registration_date, balance "
            f"FROM {config.MYSQL_TABLE} WHERE email IN ({placeholders})",
            emails,
        )
        rows = self.cur.fetchall()
        return [
            {
                "name": r[0],
                "email": r[1],
                "city": r[2],
                "age": r[3],
                "registration_date": r[4],
                "balance": r[5],
            }
            for r in rows
        ]

    def server_version(self) -> str:
        self.cur.execute("SELECT VERSION()")
        (v,) = self.cur.fetchone()
        return v

    def close(self) -> None:
        try:
            self.cur.close()
        finally:
            self.conn.close()
