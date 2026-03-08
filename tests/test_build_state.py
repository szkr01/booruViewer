from __future__ import annotations

import json
from tempfile import TemporaryDirectory
import unittest
from pathlib import Path

from app.build_state import BuildState, BuildStateStore, PendingRange


class BuildStateTests(unittest.TestCase):
    def test_round_trip_pending_ranges(self) -> None:
        with TemporaryDirectory() as tmpdir:
            store = BuildStateStore(Path(tmpdir) / "build_state.json")
            store.save(
                BuildState(
                    sync_failures=[5, 6],
                    pending_ranges=[
                        PendingRange(upper_id=1000, lower_id=900, cursor_id=950, source="latest", status="active")
                    ],
                    probe_resume_id=899,
                )
            )

            loaded = store.load()

        self.assertEqual(loaded.sync_failures, [5, 6])
        self.assertEqual(len(loaded.pending_ranges), 1)
        self.assertEqual(loaded.pending_ranges[0].upper_id, 1000)
        self.assertEqual(loaded.pending_ranges[0].lower_id, 900)
        self.assertEqual(loaded.pending_ranges[0].cursor_id, 950)
        self.assertEqual(loaded.pending_ranges[0].source, "latest")
        self.assertEqual(loaded.pending_ranges[0].status, "active")
        self.assertEqual(loaded.probe_resume_id, 899)

    def test_loads_legacy_active_gap_as_pending_range(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "build_state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "sync_failures": [1],
                        "active_gap_upper_id": 1200,
                        "active_gap_lower_id": 1100,
                        "active_gap_cursor_id": 1180,
                        "probe_resume_id": 1099,
                    }
                ),
                encoding="utf-8",
            )
            loaded = BuildStateStore(state_path).load()

        self.assertEqual(len(loaded.pending_ranges), 1)
        self.assertEqual(loaded.pending_ranges[0].upper_id, 1200)
        self.assertEqual(loaded.pending_ranges[0].lower_id, 1100)
        self.assertEqual(loaded.pending_ranges[0].cursor_id, 1180)
        self.assertEqual(loaded.pending_ranges[0].status, "active")
        self.assertEqual(loaded.probe_resume_id, 1099)

    def test_save_compacts_failures_and_pending_ranges(self) -> None:
        with TemporaryDirectory() as tmpdir:
            state_path = Path(tmpdir) / "build_state.json"
            store = BuildStateStore(state_path)
            state = BuildState(
                sync_failures=[8, 3, 8, 5, 3],
                pending_ranges=[
                    PendingRange(upper_id=1000, lower_id=900, cursor_id=1500, source="latest", status="active"),
                    PendingRange(upper_id=1000, lower_id=900, cursor_id=950, source="probe", status="pending"),
                    PendingRange(upper_id=0, lower_id=0, cursor_id=0),
                ],
                probe_resume_id=-10,
            )

            store.save(state)
            loaded = store.load()

        self.assertEqual(state.sync_failures, [3, 5, 8])
        self.assertEqual(loaded.sync_failures, [3, 5, 8])
        self.assertEqual(state.probe_resume_id, 0)
        self.assertEqual(loaded.probe_resume_id, 0)
        self.assertEqual(len(state.pending_ranges), 1)
        self.assertEqual(len(loaded.pending_ranges), 1)
        self.assertEqual(state.pending_ranges[0].upper_id, 1000)
        self.assertEqual(state.pending_ranges[0].lower_id, 900)
        self.assertEqual(state.pending_ranges[0].cursor_id, 1001)
        self.assertEqual(loaded.pending_ranges[0].cursor_id, 1001)


if __name__ == "__main__":
    unittest.main()
