from __future__ import annotations

import unittest
from unittest.mock import patch

from app.services.search_service import SearchService


class _FakeArray:
    def __init__(self, values):
        self._values = list(values)

    def tolist(self):
        return list(self._values)

    def astype(self, *_args, **_kwargs):
        return self

    def reshape(self, *_args, **_kwargs):
        return self


class _FakeVector:
    def astype(self, *_args, **_kwargs):
        return self

    def reshape(self, *_args, **_kwargs):
        return self


class _FakeSearchResult:
    def __init__(self, ids, scores) -> None:
        self.ids = _FakeArray(ids)
        self.scores = _FakeArray(scores)


def _row(post_id: int, rating: int = 0) -> dict[str, int]:
    return {
        "id": post_id,
        "rating": rating,
        "c1": 0x12,
        "c2": 0x34,
        "c3": 0x1234567890ABCDEF,
        "c4": 0x0FEDCBA098765432,
        "c5": 0,
        "vec_idx": 0,
    }


class SearchServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.service = SearchService()

    def test_recent_filters_rows_above_rating_threshold_before_return(self) -> None:
        with (
            patch("app.services.search_service.settings.rating_threshold", 1),
            patch(
                "app.services.search_service.db.get_recent",
                return_value=[
                    _row(10916846, rating=1),
                    _row(10916568, rating=2),
                    _row(10916567, rating=0),
                ],
            ),
        ):
            results = self.service.recent(limit=3, offset=0)

        self.assertEqual([entry.id for entry in results], ["10916846", "10916567"])
        self.assertEqual([entry.rating for entry in results], [1, 0])

    def test_vec_idx_mode_uses_vec_idx_mapping_only(self) -> None:
        with (
            patch("app.services.search_service.vector_store.is_ready", return_value=True),
            patch("app.services.search_service.vector_store.search_id_mode", "vec_idx"),
            patch("app.services.search_service.settings.rating_threshold", 1),
            patch(
                "app.services.search_service.vector_store.query_vector_by_post_id",
                return_value=_FakeVector(),
            ),
            patch(
                "app.services.search_service.vector_store.search",
                return_value=_FakeSearchResult(
                    ids=[5244708, 5244709],
                    scores=[0.9, 0.8],
                ),
            ),
            patch("app.services.search_service.tag_engine.str_to_tags", return_value=[]),
            patch(
                "app.services.search_service.db.get_posts_by_ids",
                side_effect=AssertionError("post_id lookup should not run in vec_idx mode"),
            ),
            patch(
                "app.services.search_service.db.get_posts_by_vec_idxs",
                return_value={
                    5244708: _row(10916846, rating=1),
                    5244709: _row(10916568, rating=0),
                },
            ),
        ):
            results, mode, reason = self.service.search_with_mode(
                query_text="10916846",
                query_images=None,
                image_weights=None,
                limit=2,
                offset=0,
            )

        self.assertEqual((mode, reason), ("vector", "faiss_match_vecidx"))
        self.assertEqual([entry.id for entry in results], ["10916846", "10916568"])
        self.assertEqual([round(entry.score, 2) for entry in results], [0.9, 0.8])

    def test_vector_results_filter_rows_above_rating_threshold_before_return(self) -> None:
        with (
            patch("app.services.search_service.vector_store.is_ready", return_value=True),
            patch("app.services.search_service.vector_store.search_id_mode", "post_id"),
            patch("app.services.search_service.settings.rating_threshold", 1),
            patch(
                "app.services.search_service.vector_store.query_vector_by_post_id",
                return_value=_FakeVector(),
            ),
            patch(
                "app.services.search_service.vector_store.search",
                return_value=_FakeSearchResult(
                    ids=[10916846, 10916568, 10916567],
                    scores=[0.95, 0.75, 0.7],
                ),
            ),
            patch("app.services.search_service.tag_engine.str_to_tags", return_value=[]),
            patch(
                "app.services.search_service.db.get_posts_by_ids",
                return_value={
                    10916846: _row(10916846, rating=1),
                    10916568: _row(10916568, rating=2),
                    10916567: _row(10916567, rating=0),
                },
            ),
        ):
            results, mode, reason = self.service.search_with_mode(
                query_text="10916846",
                query_images=None,
                image_weights=None,
                limit=3,
                offset=0,
            )

        self.assertEqual((mode, reason), ("vector", "faiss_match_idmap"))
        self.assertEqual([entry.id for entry in results], ["10916846", "10916567"])
        self.assertEqual([round(entry.score, 2) for entry in results], [0.95, 0.7])

    def test_post_id_mode_uses_post_id_mapping_only(self) -> None:
        with (
            patch("app.services.search_service.vector_store.is_ready", return_value=True),
            patch("app.services.search_service.vector_store.search_id_mode", "post_id"),
            patch(
                "app.services.search_service.vector_store.query_vector_by_post_id",
                return_value=_FakeVector(),
            ),
            patch(
                "app.services.search_service.vector_store.search",
                return_value=_FakeSearchResult(
                    ids=[10916846, 10916568],
                    scores=[0.95, 0.75],
                ),
            ),
            patch("app.services.search_service.tag_engine.str_to_tags", return_value=[]),
            patch(
                "app.services.search_service.db.get_posts_by_ids",
                return_value={
                    10916846: _row(10916846, rating=1),
                    10916568: _row(10916568, rating=0),
                },
            ),
            patch(
                "app.services.search_service.db.get_posts_by_vec_idxs",
                side_effect=AssertionError("vec_idx lookup should not run in post_id mode"),
            ),
        ):
            results, mode, reason = self.service.search_with_mode(
                query_text="10916846",
                query_images=None,
                image_weights=None,
                limit=2,
                offset=0,
            )

        self.assertEqual((mode, reason), ("vector", "faiss_match_idmap"))
        self.assertEqual([entry.id for entry in results], ["10916846", "10916568"])
        self.assertEqual([round(entry.score, 2) for entry in results], [0.95, 0.75])

    def test_vec_idx_mode_does_not_fallback_to_post_id_on_missing_rows(self) -> None:
        with (
            patch("app.services.search_service.vector_store.is_ready", return_value=True),
            patch("app.services.search_service.vector_store.search_id_mode", "vec_idx"),
            patch(
                "app.services.search_service.vector_store.query_vector_by_post_id",
                return_value=_FakeVector(),
            ),
            patch(
                "app.services.search_service.vector_store.search",
                return_value=_FakeSearchResult(
                    ids=[5244708],
                    scores=[0.9],
                ),
            ),
            patch("app.services.search_service.tag_engine.str_to_tags", return_value=[]),
            patch(
                "app.services.search_service.db.get_posts_by_ids",
                side_effect=AssertionError("post_id fallback should not run in vec_idx mode"),
            ),
            patch("app.services.search_service.db.get_posts_by_vec_idxs", return_value={}),
            patch.object(
                self.service,
                "recent",
                return_value=[],
            ) as recent_mock,
        ):
            results, mode, reason = self.service.search_with_mode(
                query_text="10916846",
                query_images=None,
                image_weights=None,
                limit=1,
                offset=0,
            )

        self.assertEqual(results, [])
        self.assertEqual((mode, reason), ("recent", "db_map_empty"))
        recent_mock.assert_called_once_with(limit=1, offset=0)


if __name__ == "__main__":
    unittest.main()
