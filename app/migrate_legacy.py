from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import numpy as np

from .config import settings
from .database import db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate legacy cache into current DB/index format")
    parser.add_argument("--legacy-cache", type=str, default=None, help="Path containing search_ivfpq.index/metadata.npy/id_map.npy")
    parser.add_argument("--force", action="store_true", help="Overwrite existing db/index/vector files")
    parser.add_argument("--if-needed", action="store_true", help="Skip when db+index already exist")
    return parser.parse_args()


def _read_struct_fields(arr: np.ndarray, id_field_candidates: list[str], idx_field_candidates: list[str]) -> tuple[np.ndarray, np.ndarray]:
    if arr.dtype.names:
        id_field = next((f for f in id_field_candidates if f in arr.dtype.names), None)
        idx_field = next((f for f in idx_field_candidates if f in arr.dtype.names), None)
        if id_field is None or idx_field is None:
            raise ValueError(f"Unsupported dtype fields: {arr.dtype.names}")
        return arr[id_field].astype(np.int64), arr[idx_field].astype(np.int64)

    if arr.ndim == 2 and arr.shape[1] >= 2:
        return arr[:, 0].astype(np.int64), arr[:, 1].astype(np.int64)

    raise ValueError("Unsupported id_map format")


def _detect_legacy_cache_path(cli_path: str | None) -> Path | None:
    if cli_path:
        p = Path(cli_path)
        return p if p.exists() else None

    for d in settings.legacy_cache_dirs:
        if (d / "search_ivfpq.index").exists() and (d / "metadata.npy").exists() and (d / "id_map.npy").exists():
            return d

    return None


def _field(data: np.ndarray, *names: str, cast=np.int64, default: int = 0) -> np.ndarray:
    if data.dtype.names:
        for name in names:
            if name in data.dtype.names:
                return data[name].astype(cast)
    return np.full((len(data),), default, dtype=cast)

def _field_u64_as_i64_bits(data: np.ndarray, *names: str) -> np.ndarray:
    if data.dtype.names:
        for name in names:
            if name in data.dtype.names:
                return data[name].astype(np.uint64).view(np.int64)
    return np.zeros((len(data),), dtype=np.int64)


def _clean_outputs() -> None:
    for p in [settings.db_path, settings.faiss_index_path, settings.vectors_raw_path]:
        if Path(p).exists():
            Path(p).unlink()


def _has_output() -> bool:
    return Path(settings.db_path).exists() and Path(settings.faiss_index_path).exists() and db.count() > 0


def _convert_legacy_vectors(source_vectors: Path, expected_rows: int) -> None:
    expected_size = expected_rows * settings.embedding_dim * np.dtype(np.float16).itemsize
    source_size = source_vectors.stat().st_size

    # Old ILEMB may have mislabeled raw float16 vectors as ".npy".
    if source_size == expected_size:
        shutil.copy2(source_vectors, settings.vectors_raw_path)
        print(f"Copied raw vectors {expected_rows:,} rows")
        return

    vectors = np.load(source_vectors, mmap_mode="r")
    if vectors.ndim != 2:
        raise ValueError(f"Unsupported vectors_raw.npy shape: {vectors.shape}")
    if vectors.shape[0] != expected_rows:
        raise ValueError(
            f"vectors_raw.npy row count mismatch: vectors={vectors.shape[0]} metadata={expected_rows}"
        )
    if vectors.shape[1] != settings.embedding_dim:
        raise ValueError(
            f"vectors_raw.npy dim mismatch: vectors={vectors.shape[1]} expected={settings.embedding_dim}"
        )

    chunk_size = 50000
    with settings.vectors_raw_path.open("wb") as out_fp:
        for start in range(0, vectors.shape[0], chunk_size):
            end = min(start + chunk_size, vectors.shape[0])
            chunk = np.asarray(vectors[start:end], dtype=np.float16)
            out_fp.write(chunk.tobytes())
            print(f"Converted vectors {end:,}/{vectors.shape[0]:,}")


def migrate(cache_dir: Path) -> None:
    source_index = cache_dir / "search_ivfpq.index"
    source_meta = cache_dir / "metadata.npy"
    source_id_map = cache_dir / "id_map.npy"
    source_vectors = cache_dir / "vectors_raw.npy"

    if not source_index.exists() or not source_meta.exists() or not source_id_map.exists():
        raise FileNotFoundError("Legacy cache missing required files")

    settings.faiss_index_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_index, settings.faiss_index_path)
    print(f"Copied index -> {settings.faiss_index_path}")

    metadata = np.load(source_meta, mmap_mode="r")
    id_map = np.load(source_id_map, mmap_mode="r")

    if source_vectors.exists():
        _convert_legacy_vectors(source_vectors, expected_rows=len(metadata))
        print(f"Converted vectors -> {settings.vectors_raw_path}")

    id_map_ids, id_map_idxs = _read_struct_fields(id_map, ["id"], ["idx", "index"])
    sorter = np.argsort(id_map_ids)
    id_map_ids = id_map_ids[sorter]
    id_map_idxs = id_map_idxs[sorter]

    chunk_size = 50000
    for start in range(0, len(metadata), chunk_size):
        end = min(start + chunk_size, len(metadata))
        chunk = metadata[start:end]

        ids = _field(chunk, "id", cast=np.int64)
        ratings = _field(chunk, "rating", cast=np.int64)
        c1 = _field(chunk, "c1", "url_c1", cast=np.int64)
        c2 = _field(chunk, "c2", "url_c2", cast=np.int64)
        c3 = _field_u64_as_i64_bits(chunk, "c3", "url_c3")
        c4 = _field_u64_as_i64_bits(chunk, "c4", "url_c4")
        c5 = _field(chunk, "c5", "url_c5", cast=np.int64)

        pos = np.searchsorted(id_map_ids, ids)
        valid = (pos >= 0) & (pos < len(id_map_ids))
        valid &= id_map_ids[np.clip(pos, 0, len(id_map_ids) - 1)] == ids

        vec_idx = np.full_like(ids, -1, dtype=np.int64)
        vec_idx[valid] = id_map_idxs[pos[valid]]

        rows = [
            (int(i), int(r), int(v1), int(v2), int(v3), int(v4), int(v5), int(vi))
            for i, r, v1, v2, v3, v4, v5, vi in zip(ids, ratings, c1, c2, c3, c4, c5, vec_idx)
            if int(vi) >= 0
        ]
        if rows:
            db.upsert_posts(rows)

        print(f"Migrated {end:,}/{len(metadata):,}")

    print(f"Migration done. rows={db.count()}")


if __name__ == "__main__":
    args = parse_args()

    if args.if_needed and _has_output() and not args.force:
        print("Migration skipped: outputs already exist")
        raise SystemExit(0)

    if args.force:
        _clean_outputs()

    cache_dir = _detect_legacy_cache_path(args.legacy_cache)
    if cache_dir is None:
        print("No legacy cache found. Migration skipped.")
        raise SystemExit(0)

    print(f"Migrating from: {cache_dir}")
    migrate(cache_dir)
