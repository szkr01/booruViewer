from __future__ import annotations

import importlib
import sys
import types
import unittest

from app.build_state import BuildState, PendingRange


def _load_sync_posts_module():
    fake_ingest = types.ModuleType("app.ingest_posts")
    fake_ingest.AdaptiveDownloadController = type("AdaptiveDownloadController", (), {})
    fake_ingest.IngestRow = type("IngestRow", (), {})
    fake_ingest.RollingParquetWriter = type("RollingParquetWriter", (), {})
    fake_ingest.fetch_posts_page = lambda *args, **kwargs: []
    fake_ingest.make_client = lambda *args, **kwargs: None
    fake_ingest.process_posts_with_stats = lambda *args, **kwargs: []

    fake_tag_engine_mod = types.ModuleType("app.tag_engine")
    fake_tag_engine_mod.tag_engine = types.SimpleNamespace(load=lambda: None, device=types.SimpleNamespace(type="cuda"))

    original_ingest = sys.modules.get("app.ingest_posts")
    original_tag_engine = sys.modules.get("app.tag_engine")
    sys.modules["app.ingest_posts"] = fake_ingest
    sys.modules["app.tag_engine"] = fake_tag_engine_mod
    sys.modules.pop("app.sync_posts", None)
    try:
        return importlib.import_module("app.sync_posts")
    finally:
        if original_ingest is None:
            sys.modules.pop("app.ingest_posts", None)
        else:
            sys.modules["app.ingest_posts"] = original_ingest
        if original_tag_engine is None:
            sys.modules.pop("app.tag_engine", None)
        else:
            sys.modules["app.tag_engine"] = original_tag_engine


sync_posts = _load_sync_posts_module()


class SyncPostsPlannerTests(unittest.TestCase):
    def test_prioritize_latest_range_ahead_of_stale_probe(self) -> None:
        state = BuildState(
            pending_ranges=[
                PendingRange(upper_id=5784529, lower_id=1, cursor_id=3696360, source="probe", status="active")
            ],
            probe_resume_id=8704465,
        )

        changed = sync_posts._prioritize_latest_range(
            state,
            latest_head_id=11223105,
            db_max_id=10916872,
            gap_threshold=400,
        )

        self.assertTrue(changed)
        self.assertEqual(len(state.pending_ranges), 2)
        self.assertEqual(state.pending_ranges[0].source, "latest")
        self.assertEqual(state.pending_ranges[0].status, "active")
        self.assertEqual(state.pending_ranges[0].upper_id, 11223105)
        self.assertEqual(state.pending_ranges[0].lower_id, 10916873)
        self.assertEqual(state.pending_ranges[0].cursor_id, 11223106)
        self.assertEqual(state.pending_ranges[1].source, "probe")
        self.assertEqual(state.pending_ranges[1].status, "pending")
        self.assertEqual(state.pending_ranges[1].cursor_id, 3696360)

    def test_keep_existing_latest_progress_when_already_queued(self) -> None:
        state = BuildState(
            pending_ranges=[
                PendingRange(upper_id=5000, lower_id=1, cursor_id=3000, source="probe", status="active"),
                PendingRange(upper_id=12000, lower_id=11001, cursor_id=11800, source="probe", status="pending"),
            ]
        )

        changed = sync_posts._prioritize_latest_range(
            state,
            latest_head_id=12000,
            db_max_id=11000,
            gap_threshold=400,
        )

        self.assertTrue(changed)
        self.assertEqual(state.pending_ranges[0].upper_id, 12000)
        self.assertEqual(state.pending_ranges[0].lower_id, 11001)
        self.assertEqual(state.pending_ranges[0].cursor_id, 11800)
        self.assertEqual(state.pending_ranges[0].source, "latest")
        self.assertEqual(state.pending_ranges[0].status, "active")
        self.assertEqual(state.pending_ranges[1].upper_id, 5000)
        self.assertEqual(state.pending_ranges[1].status, "pending")


if __name__ == "__main__":
    unittest.main()
