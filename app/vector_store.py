from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import faiss
import numpy as np

from .config import settings
from .database import db


@dataclass
class SearchResult:
    ids: np.ndarray
    scores: np.ndarray


class VectorStore:
    def __init__(self) -> None:
        self.index = None
        self.vectors_raw: np.memmap | None = None

    def load(self) -> None:
        index_path = Path(settings.faiss_index_path)
        if index_path.exists():
            self.index = faiss.read_index(str(index_path))
            if hasattr(self.index, "nprobe"):
                self.index.nprobe = 32

        vec_path = Path(settings.vectors_raw_path)
        if vec_path.exists():
            max_vec_idx = db.max_vec_idx()
            if max_vec_idx >= 0:
                self.vectors_raw = np.memmap(
                    str(vec_path),
                    dtype=np.float16,
                    mode="r",
                    shape=(max_vec_idx + 1, settings.embedding_dim),
                )

    def is_ready(self) -> bool:
        return self.index is not None

    def search(self, query_vec: np.ndarray, k: int) -> SearchResult:
        if self.index is None:
            return SearchResult(ids=np.empty((0,), dtype=np.int64), scores=np.empty((0,), dtype=np.float32))

        q = query_vec.astype(np.float32, copy=False).reshape(1, -1)
        faiss.normalize_L2(q)
        scores, ids = self.index.search(q, k)
        return SearchResult(ids=ids[0], scores=scores[0])

    def query_vector_by_post_id(self, post_id: int) -> np.ndarray | None:
        vec_idx = db.get_vec_idx(post_id)
        if vec_idx is None:
            return None

        if self.vectors_raw is not None:
            if vec_idx < 0 or vec_idx >= self.vectors_raw.shape[0]:
                return None
            return np.array(self.vectors_raw[vec_idx], dtype=np.float32, copy=True)

        if self.index is not None:
            try:
                vec = self.index.reconstruct(int(post_id))
                return np.array(vec, dtype=np.float32, copy=True)
            except Exception:
                return None

        return None


vector_store = VectorStore()
