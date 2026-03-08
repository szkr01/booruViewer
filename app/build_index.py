from __future__ import annotations

import argparse
import os
from pathlib import Path

import faiss
import numpy as np

from .config import settings
from .database import db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync parquet vectors into existing DB/index")
    parser.add_argument("--parquet-glob", type=str, default=None, help="Glob for parquet files")
    parser.add_argument("--init-from-parquet", action="store_true", help="Initialize a new index when missing")
    parser.add_argument("--delete-synced", action="store_true", help="Delete synced parquet files on success")
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=max(1, int(getattr(settings, "build_index_checkpoint_every", 20))),
        help="Write faiss checkpoint every N appended batches",
    )
    parser.add_argument(
        "--vector-flush-every",
        type=int,
        default=max(1, int(getattr(settings, "build_index_vector_flush_every", 16))),
        help="Flush vectors_raw every N appended batches",
    )
    parser.add_argument(
        "--vector-fsync",
        action="store_true",
        default=bool(getattr(settings, "build_index_vector_fsync", False)),
        help="Call fsync on vectors_raw flush (slower, more durable)",
    )
    parser.add_argument(
        "--id-mode",
        type=str,
        default="auto",
        choices=["auto", "post_id", "vec_idx"],
        help="ID space used in faiss index",
    )
    return parser.parse_args()


def _resolve_parquet_paths(parquet_glob: str | None) -> list[Path]:
    if not parquet_glob:
        return []
    return sorted(Path().glob(parquet_glob))

def _to_i64_bits(values) -> np.ndarray:
    # Keep uint64 bit pattern while storing in SQLite INTEGER (signed 64-bit).
    return np.array(values, dtype=np.uint64).view(np.int64)


def _iter_parquet_rows(parquet_paths: list[Path], batch_size: int = 50000):
    import pyarrow.parquet as pq

    required_cols = ["id", "rating", "url_c1", "url_c2", "url_c3", "url_c4", "url_c5", "emb"]

    for parquet_path in parquet_paths:
        pf = pq.ParquetFile(parquet_path)
        for batch in pf.iter_batches(batch_size=batch_size, columns=required_cols):
            d = batch.to_pydict()
            ids = np.array(d["id"], dtype=np.int64)
            ratings = np.array(d["rating"], dtype=np.int64)
            c1 = np.array(d["url_c1"], dtype=np.int64)
            c2 = np.array(d["url_c2"], dtype=np.int64)
            c3 = _to_i64_bits(d["url_c3"])
            c4 = _to_i64_bits(d["url_c4"])
            c5 = np.array(d["url_c5"], dtype=np.int64)
            embs = np.array(d["emb"], dtype=np.float32)

            mask = ratings <= settings.rating_threshold
            if not mask.any():
                continue

            yield (
                parquet_path,
                ids[mask],
                ratings[mask],
                c1[mask],
                c2[mask],
                c3[mask],
                c4[mask],
                c5[mask],
                embs[mask],
            )


def _train_index_from_parquet(parquet_paths: list[Path]) -> faiss.Index:
    train_samples: list[np.ndarray] = []
    sample_cap = 200000

    for _, _, _, _, _, _, _, _, embs in _iter_parquet_rows(parquet_paths):
        if embs.size == 0:
            continue
        train_samples.append(embs)
        if sum(x.shape[0] for x in train_samples) >= sample_cap:
            break

    if not train_samples:
        raise RuntimeError("No training vectors found in parquet")

    train = np.concatenate(train_samples, axis=0)[:sample_cap]
    dim = train.shape[1]
    if dim != settings.embedding_dim:
        raise ValueError(f"Embedding dim mismatch: expected {settings.embedding_dim}, got {dim}")

    nlist = 4096 if len(train) >= 4096 else max(128, len(train) // 64)
    m = 64 if dim % 64 == 0 else 32

    index = faiss.IndexIVFPQ(faiss.IndexFlatIP(dim), dim, nlist, m, 8)
    index.metric_type = faiss.METRIC_INNER_PRODUCT
    index.train(train)

    print(f"Trained new IVF-PQ index: nlist={nlist}, m={m}, samples={len(train)}")
    return index


def _detect_id_mode(index: faiss.Index) -> str:
    if db.count() == 0:
        return "vec_idx"

    if not Path(settings.vectors_raw_path).exists():
        return "vec_idx"

    with db.connection() as conn:
        row = conn.execute(
            "SELECT id, vec_idx FROM posts WHERE vec_idx >= 0 ORDER BY id DESC LIMIT 1"
        ).fetchone()
    if not row:
        return "vec_idx"

    post_id = int(row["id"])
    vec_idx = int(row["vec_idx"])
    if vec_idx < 0:
        return "vec_idx"

    try:
        vecs = np.memmap(
            str(settings.vectors_raw_path),
            dtype=np.float16,
            mode="r",
            shape=(db.max_vec_idx() + 1, settings.embedding_dim),
        )
        q = np.array(vecs[vec_idx], dtype=np.float32, copy=True).reshape(1, -1)
        faiss.normalize_L2(q)
        _, I = index.search(q, 1)
        rid = int(I[0][0])
    except Exception:
        return "vec_idx"

    if rid == post_id:
        return "post_id"
    if rid == vec_idx:
        return "vec_idx"
    return "vec_idx"


def _append_rows(
    index: faiss.Index,
    parquet_paths: list[Path],
    vectors_mode: str,
    id_mode: str,
    index_path: Path,
    checkpoint_every: int,
    vector_flush_every: int,
    vector_fsync: bool,
) -> tuple[int, int]:
    added = 0
    skipped_existing = 0
    current_vec_idx = db.max_vec_idx() + 1
    appended_batches = 0

    if id_mode == "vec_idx" and int(index.ntotal) != current_vec_idx:
        raise RuntimeError(
            f"Legacy vec_idx mode mismatch: index.ntotal={int(index.ntotal)} current_vec_idx={current_vec_idx}"
        )

    settings.vectors_raw_path.parent.mkdir(parents=True, exist_ok=True)
    with settings.vectors_raw_path.open(vectors_mode) as vec_fp:
        for parquet_path, ids, ratings, c1, c2, c3, c4, c5, embs in _iter_parquet_rows(parquet_paths):
            if len(ids) == 0:
                continue

            exists = db.existing_ids(ids.tolist())
            if exists:
                exists_arr = np.fromiter(exists, dtype=np.int64)
                mask_new = ~np.isin(ids, exists_arr)
                skipped_existing += int((~mask_new).sum())
            else:
                mask_new = np.ones(len(ids), dtype=bool)

            if not mask_new.any():
                continue

            ids = ids[mask_new]
            ratings = ratings[mask_new]
            c1 = c1[mask_new]
            c2 = c2[mask_new]
            c3 = c3[mask_new]
            c4 = c4[mask_new]
            c5 = c5[mask_new]
            embs = embs[mask_new]
            raw_embs = np.array(embs, dtype=np.float32, copy=True)
            if id_mode == "post_id":
                index_embs = np.array(raw_embs, dtype=np.float32, copy=True)
                faiss.normalize_L2(index_embs)
                add_ids = ids.astype(np.int64, copy=False)
                index.add_with_ids(index_embs, add_ids)
            else:
                index.add(raw_embs)
            vec_fp.write(raw_embs.astype(np.float16).tobytes())

            rows = [
                (
                    int(ids[i]),
                    int(ratings[i]),
                    int(c1[i]),
                    int(c2[i]),
                    int(c3[i]),
                    int(c4[i]),
                    int(c5[i]),
                    current_vec_idx + i,
                )
                for i in range(len(ids))
            ]
            db.upsert_posts(rows)
            appended_batches += 1
            if vector_flush_every > 0 and appended_batches % vector_flush_every == 0:
                vec_fp.flush()
                if vector_fsync:
                    try:
                        os.fsync(vec_fp.fileno())
                    except OSError:
                        pass
            if checkpoint_every > 0 and appended_batches % checkpoint_every == 0:
                faiss.write_index(index, str(index_path))
                print(f"Checkpoint saved: batches={appended_batches}, added={added + len(rows)}")

            current_vec_idx += len(rows)
            added += len(rows)
            print(f"Synced {parquet_path.name}: +{len(rows)}")

        vec_fp.flush()
        if vector_fsync:
            try:
                os.fsync(vec_fp.fileno())
            except OSError:
                pass

    return added, skipped_existing


def _report_coverage() -> None:
    with db.connection() as conn:
        row = conn.execute("SELECT MIN(id), MAX(id), COUNT(*) FROM posts").fetchone()

    if not row or row[0] is None:
        print("Coverage: empty")
        return

    min_id = int(row[0])
    max_id = int(row[1])
    count = int(row[2])
    span = max_id - min_id + 1
    gaps = max(0, span - count)
    print(f"Coverage: min_id={min_id}, max_id={max_id}, rows={count}, missing_in_span~={gaps}")


if __name__ == "__main__":
    args = parse_args()

    parquet_glob = args.parquet_glob or settings.parquet_glob
    parquet_paths = _resolve_parquet_paths(parquet_glob)
    if not parquet_paths:
        print("No parquet files found. Skip sync.")
        raise SystemExit(0)

    index_path = Path(settings.faiss_index_path)

    if index_path.exists():
        index = faiss.read_index(str(index_path))
        vectors_mode = "ab"
    else:
        if not args.init_from_parquet:
            raise RuntimeError("Faiss index is missing. Run migration or use --init-from-parquet")

        if db.count() > 0:
            raise RuntimeError("DB exists but index is missing. Run migration script first.")

        index = _train_index_from_parquet(parquet_paths)
        vectors_mode = "wb"

    if args.id_mode == "auto":
        id_mode = _detect_id_mode(index) if vectors_mode == "ab" else "vec_idx"
    else:
        id_mode = args.id_mode

    print(f"Index ID mode: {id_mode}")

    if vectors_mode == "ab" and db.count() > 0 and not Path(settings.vectors_raw_path).exists():
        raise RuntimeError("vectors_raw is missing. Run migration script before sync.")

    try:
        added, skipped_existing = _append_rows(
            index=index,
            parquet_paths=parquet_paths,
            vectors_mode=vectors_mode,
            id_mode=id_mode,
            index_path=index_path,
            checkpoint_every=max(1, int(args.checkpoint_every)),
            vector_flush_every=max(1, int(args.vector_flush_every)),
            vector_fsync=bool(args.vector_fsync),
        )
    except KeyboardInterrupt:
        faiss.write_index(index, str(index_path))
        print("Interrupted: latest index checkpoint has been saved.")
        raise SystemExit(130)

    faiss.write_index(index, str(index_path))

    print(f"Sync complete: added={added}, skipped_existing={skipped_existing}")
    _report_coverage()

    if args.delete_synced:
        for p in parquet_paths:
            try:
                p.unlink(missing_ok=True)
                print(f"Deleted synced parquet: {p}")
            except OSError as exc:
                print(f"Warning: failed to delete {p}: {exc}")
