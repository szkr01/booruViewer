from __future__ import annotations

import re
from typing import Iterable

import numpy as np
from PIL import Image

from ..config import settings
from ..database import db
from ..schemas import ImageEntry
from ..tag_engine import tag_engine
from ..url_utils import build_cdn_url, build_post_url
from ..vector_store import vector_store

ID_QUERY_RE = re.compile(r"(?:^|\s)id\s*:\s*(\d+)(?:$|\s)", re.IGNORECASE)


class SearchService:
    def _to_entries(self, rows: Iterable[dict], score_map: dict[int, float] | None = None) -> list[ImageEntry]:
        entries: list[ImageEntry] = []
        for row in rows:
            post_id = int(row["id"])
            entries.append(
                ImageEntry(
                    id=str(post_id),
                    url=build_post_url(post_id),
                    media_url=build_cdn_url(row),
                    rating=int(row["rating"]),
                    score=float(score_map.get(post_id, 0.0) if score_map else 0.0),
                )
            )
        return entries

    def _to_ranked_entries(self, ranked_rows: Iterable[tuple[dict, float]]) -> list[ImageEntry]:
        entries: list[ImageEntry] = []
        for row, score in ranked_rows:
            post_id = int(row["id"])
            entries.append(
                ImageEntry(
                    id=str(post_id),
                    url=build_post_url(post_id),
                    media_url=build_cdn_url(row),
                    rating=int(row["rating"]),
                    score=float(score),
                )
            )
        return entries

    def _extract_post_id_query(self, query_text: str | None) -> int | None:
        if not query_text:
            return None
        q = query_text.strip()
        if not q:
            return None
        if q.isdigit():
            return int(q)
        m = ID_QUERY_RE.search(q)
        if m:
            return int(m.group(1))
        return None

    def recent(self, limit: int, offset: int) -> list[ImageEntry]:
        rows = db.get_recent(limit=limit, offset=offset)
        return self._to_entries(rows)

    def search(
        self,
        query_text: str | None,
        query_images: list[Image.Image] | None,
        image_weights: list[float] | None,
        limit: int,
        offset: int,
    ) -> list[ImageEntry]:
        results, _, _ = self.search_with_mode(
            query_text=query_text,
            query_images=query_images,
            image_weights=image_weights,
            limit=limit,
            offset=offset,
        )
        return results

    def search_with_mode(
        self,
        query_text: str | None,
        query_images: list[Image.Image] | None,
        image_weights: list[float] | None,
        limit: int,
        offset: int,
    ) -> tuple[list[ImageEntry], str, str]:
        has_query = False
        query_vec = np.zeros((1, settings.embedding_dim), dtype=np.float32)

        # Query by uploaded image(s)
        if query_images:
            weights = image_weights if image_weights else [1.0] * len(query_images)
            for i, img in enumerate(query_images):
                feat = tag_engine.extract_image_feature(img)
                if feat is None or feat.size == 0:
                    continue
                norm = float(np.linalg.norm(feat))
                if norm > 1e-8:
                    feat = feat / norm
                w = float(weights[i]) if i < len(weights) else 1.0
                query_vec += feat.reshape(1, -1) * w
                has_query = True

        # Query by image id
        post_id = self._extract_post_id_query(query_text)
        if post_id is not None:
            ref_vec = vector_store.query_vector_by_post_id(post_id)
            if ref_vec is not None:
                query_vec += ref_vec.reshape(1, -1).astype(np.float32, copy=False)
                has_query = True

        # Query by tags
        if query_text:
            parsed_tags = tag_engine.str_to_tags(query_text)
            for tag_idx, weight in parsed_tags:
                feat = tag_engine.extract_tag_feature(tag_idx)
                if feat is None:
                    continue
                query_vec += feat.reshape(1, -1) * float(weight)
                has_query = True

            # If only unknown tags were specified, return default vector results (recent)
            if not parsed_tags and post_id is None and not query_images:
                return self.recent(limit=limit, offset=offset), "recent", "unknown_tags_only"

        if not has_query:
            return self.recent(limit=limit, offset=offset), "recent", "empty_query"

        if not vector_store.is_ready():
            return self.recent(limit=limit, offset=0), "recent", "index_not_ready"

        max_candidates = max(offset + limit, settings.recent_default_limit)
        sr = vector_store.search(query_vec=query_vec, k=max_candidates)

        pairs = [
            (int(pid), float(score))
            for pid, score in zip(sr.ids.tolist(), sr.scores.tolist())
            if int(pid) >= 0
        ]

        if not pairs:
            return self.recent(limit=limit, offset=0), "recent", "faiss_empty"

        page_pairs = pairs[offset: offset + limit]
        ids = [pid for pid, _ in page_pairs]

        if vector_store.search_id_mode == "post_id":
            row_map = db.get_posts_by_ids(ids)
            ranked_rows = [(row_map[id_value], score) for id_value, score in page_pairs if id_value in row_map]
            results = self._to_ranked_entries(ranked_rows)
            if results:
                return results, "vector", "faiss_match_idmap"
        else:
            vec_row_map = db.get_posts_by_vec_idxs(ids)
            ranked_rows = [(vec_row_map[id_value], score) for id_value, score in page_pairs if id_value in vec_row_map]
            results = self._to_ranked_entries(ranked_rows)
            if results:
                return results, "vector", "faiss_match_vecidx"

        return self.recent(limit=limit, offset=0), "recent", "db_map_empty"

    def get_cdn_url_by_post_id(self, post_id: int) -> str | None:
        row = db.get_post(post_id)
        if row is None:
            return None
        return build_cdn_url(row)


search_service = SearchService()
