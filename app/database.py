from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from typing import Generator, Iterable

from .config import settings


class Database:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path or str(settings.db_path)
        self._init_db()

    @contextmanager
    def connection(self) -> Generator[sqlite3.Connection, None, None]:
        conn = sqlite3.connect(self.db_path, timeout=30.0, check_same_thread=False)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            yield conn
            conn.commit()
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS posts (
                    id INTEGER PRIMARY KEY,
                    rating INTEGER NOT NULL,
                    c1 INTEGER NOT NULL,
                    c2 INTEGER NOT NULL,
                    c3 INTEGER NOT NULL,
                    c4 INTEGER NOT NULL,
                    c5 INTEGER NOT NULL,
                    vec_idx INTEGER NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_vec_idx ON posts(vec_idx)")

    def upsert_posts(
        self,
        rows: Iterable[tuple[int, int, int, int, int, int, int, int]],
    ) -> None:
        with self.connection() as conn:
            conn.executemany(
                """
                INSERT INTO posts (id, rating, c1, c2, c3, c4, c5, vec_idx)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    rating=excluded.rating,
                    c1=excluded.c1,
                    c2=excluded.c2,
                    c3=excluded.c3,
                    c4=excluded.c4,
                    c5=excluded.c5,
                    vec_idx=excluded.vec_idx
                """,
                rows,
            )

    def get_recent(self, limit: int, offset: int = 0) -> list[sqlite3.Row]:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                SELECT id, rating, c1, c2, c3, c4, c5, vec_idx
                FROM posts
                ORDER BY id DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            )
            return cursor.fetchall()

    def get_post(self, post_id: int) -> sqlite3.Row | None:
        with self.connection() as conn:
            cursor = conn.execute(
                "SELECT id, rating, c1, c2, c3, c4, c5, vec_idx FROM posts WHERE id = ?",
                (post_id,),
            )
            return cursor.fetchone()

    def get_posts_by_ids(self, post_ids: list[int]) -> dict[int, sqlite3.Row]:
        if not post_ids:
            return {}

        placeholders = ",".join("?" for _ in post_ids)
        with self.connection() as conn:
            cursor = conn.execute(
                f"SELECT id, rating, c1, c2, c3, c4, c5, vec_idx FROM posts WHERE id IN ({placeholders})",
                post_ids,
            )
            return {int(row["id"]): row for row in cursor.fetchall()}

    def get_posts_by_vec_idxs(self, vec_idxs: list[int]) -> dict[int, sqlite3.Row]:
        if not vec_idxs:
            return {}

        placeholders = ",".join("?" for _ in vec_idxs)
        with self.connection() as conn:
            cursor = conn.execute(
                f"SELECT id, rating, c1, c2, c3, c4, c5, vec_idx FROM posts WHERE vec_idx IN ({placeholders})",
                vec_idxs,
            )
            return {int(row["vec_idx"]): row for row in cursor.fetchall()}

    def count(self) -> int:
        with self.connection() as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM posts")
            row = cursor.fetchone()
            return int(row[0]) if row else 0

    def get_stats(self) -> dict[str, int]:
        with self.connection() as conn:
            cursor = conn.execute(
                """
                SELECT
                    COUNT(*) AS post_count,
                    COALESCE(MIN(id), -1) AS min_id,
                    COALESCE(MAX(id), -1) AS max_id,
                    COALESCE(MIN(vec_idx), -1) AS min_vec_idx,
                    COALESCE(MAX(vec_idx), -1) AS max_vec_idx
                FROM posts
                """
            )
            row = cursor.fetchone()
            if row is None:
                return {
                    "post_count": 0,
                    "min_id": -1,
                    "max_id": -1,
                    "min_vec_idx": -1,
                    "max_vec_idx": -1,
                }
            return {
                "post_count": int(row["post_count"]),
                "min_id": int(row["min_id"]),
                "max_id": int(row["max_id"]),
                "min_vec_idx": int(row["min_vec_idx"]),
                "max_vec_idx": int(row["max_vec_idx"]),
            }

    def get_vec_idx(self, post_id: int) -> int | None:
        with self.connection() as conn:
            cursor = conn.execute("SELECT vec_idx FROM posts WHERE id = ?", (post_id,))
            row = cursor.fetchone()
            return int(row[0]) if row else None

    def prev_existing_id(self, id_upper: int) -> int | None:
        with self.connection() as conn:
            cursor = conn.execute(
                "SELECT id FROM posts WHERE id <= ? ORDER BY id DESC LIMIT 1",
                (int(id_upper),),
            )
            row = cursor.fetchone()
            return int(row[0]) if row else None

    def next_existing_id(self, id_lower: int) -> int | None:
        with self.connection() as conn:
            cursor = conn.execute(
                "SELECT id FROM posts WHERE id >= ? ORDER BY id ASC LIMIT 1",
                (int(id_lower),),
            )
            row = cursor.fetchone()
            return int(row[0]) if row else None

    def max_vec_idx(self) -> int:
        with self.connection() as conn:
            cursor = conn.execute("SELECT COALESCE(MAX(vec_idx), -1) FROM posts")
            row = cursor.fetchone()
            return int(row[0]) if row else -1

    def existing_ids(self, post_ids: list[int], chunk_size: int = 900) -> set[int]:
        if not post_ids:
            return set()

        result: set[int] = set()
        with self.connection() as conn:
            for start in range(0, len(post_ids), chunk_size):
                chunk = post_ids[start:start + chunk_size]
                placeholders = ",".join("?" for _ in chunk)
                cursor = conn.execute(f"SELECT id FROM posts WHERE id IN ({placeholders})", chunk)
                result.update(int(r[0]) for r in cursor.fetchall())
        return result


db = Database()
