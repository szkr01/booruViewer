from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from app.database import Database


def _post_row(post_id: int, vec_idx: int) -> tuple[int, int, int, int, int, int, int, int]:
    return (
        post_id,
        0,
        0x12,
        0x34,
        0x1234567890ABCDEF,
        0x0FEDCBA098765432,
        0,
        vec_idx,
    )


class DatabaseLookupTests(unittest.TestCase):
    def test_get_posts_by_ids_accepts_100k_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            database = Database(str(Path(tmp) / "posts.sqlite"))
            database.upsert_posts(
                [
                    _post_row(10, 2),
                    _post_row(50000, 50000),
                    _post_row(99999, 99999),
                ]
            )

            rows = database.get_posts_by_ids(list(range(100000)))

        self.assertEqual(sorted(rows), [10, 50000, 99999])
        self.assertEqual(int(rows[50000]["vec_idx"]), 50000)

    def test_get_posts_by_vec_idxs_accepts_100k_values(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            database = Database(str(Path(tmp) / "posts.sqlite"))
            database.upsert_posts(
                [
                    _post_row(10, 2),
                    _post_row(50000, 50000),
                    _post_row(99999, 99999),
                ]
            )

            rows = database.get_posts_by_vec_idxs(list(range(100000)))

        self.assertEqual(sorted(rows), [2, 50000, 99999])
        self.assertEqual(int(rows[2]["id"]), 10)


if __name__ == "__main__":
    unittest.main()
