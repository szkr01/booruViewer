from __future__ import annotations

from bisect import bisect_left, bisect_right
import unittest

from app.gap_finder import iter_missing_ranges


def _make_prev(existing: list[int]):
    ordered = sorted(existing)

    def _prev(id_upper: int) -> int | None:
        idx = bisect_right(ordered, id_upper) - 1
        if idx < 0:
            return None
        return ordered[idx]

    return _prev


def _make_next(existing: list[int]):
    ordered = sorted(existing)

    def _next(id_lower: int) -> int | None:
        idx = bisect_left(ordered, id_lower)
        if idx >= len(ordered):
            return None
        return ordered[idx]

    return _next


class GapFinderTests(unittest.TestCase):
    def test_detects_top_gap(self) -> None:
        existing = [900, 899, 898, 500]

        ranges = list(
            iter_missing_ranges(
                latest_head_id=1000,
                gap_threshold=50,
                probe_step=32,
                prev_existing_id=_make_prev(existing),
                next_existing_id=_make_next(existing),
            )
        )

        self.assertEqual((ranges[0].upper_id, ranges[0].lower_id), (1000, 901))

    def test_detects_multiple_interior_gaps(self) -> None:
        existing = [1000, 999, 998, 700, 699, 698, 300, 299]

        ranges = list(
            iter_missing_ranges(
                latest_head_id=1000,
                gap_threshold=80,
                probe_step=32,
                prev_existing_id=_make_prev(existing),
                next_existing_id=_make_next(existing),
            )
        )

        self.assertEqual(
            [(r.upper_id, r.lower_id) for r in ranges[:2]],
            [(997, 701), (697, 301)],
        )

    def test_ignores_small_gaps(self) -> None:
        existing = [1000, 980, 960, 940, 920]

        ranges = list(
            iter_missing_ranges(
                latest_head_id=1000,
                gap_threshold=30,
                probe_step=16,
                prev_existing_id=_make_prev(existing),
                next_existing_id=_make_next(existing),
                min_probe_id=920,
            )
        )

        self.assertEqual(ranges, [])

    def test_probe_step_below_threshold_finds_offset_gap(self) -> None:
        existing = [1000, 999, 998, 997, 550, 549, 548]

        ranges = list(
            iter_missing_ranges(
                latest_head_id=1000,
                gap_threshold=200,
                probe_step=64,
                prev_existing_id=_make_prev(existing),
                next_existing_id=_make_next(existing),
            )
        )

        self.assertEqual((ranges[0].upper_id, ranges[0].lower_id), (996, 551))

    def test_empty_db_returns_full_range(self) -> None:
        ranges = list(
            iter_missing_ranges(
                latest_head_id=600,
                gap_threshold=100,
                probe_step=64,
                prev_existing_id=lambda _id: None,
                next_existing_id=lambda _id: None,
            )
        )

        self.assertEqual((ranges[0].upper_id, ranges[0].lower_id), (600, 1))


if __name__ == "__main__":
    unittest.main()
