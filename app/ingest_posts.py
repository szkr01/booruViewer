from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import time
import io
from email.utils import parsedate_to_datetime
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from time import perf_counter

import httpx
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from PIL import Image

from .config import settings
from .tag_engine import tag_engine

RATING_MAP = {"g": 0, "s": 1, "q": 2, "e": 3}
EXT_TO_CODE = {"jpg": 0, "png": 1, "jpeg": 2, "bmp": 3, "webp": 4}

_CDN_RE = re.compile(
    r"^https?://cdn\.donmai\.us/(?:original|sample|720x720|360x360|180x180)/([0-9a-f]{2})/([0-9a-f]{2})/([0-9a-f]{32})\.([a-zA-Z0-9]+)$"
)
_SUPPORTED_IMAGE_EXTS = {"jpg", "jpeg", "png", "webp", "bmp"}
_RETRY_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}
_BLOCK_STATUS = {403, 429}
_MIN_EMBED_EDGE = 448


@dataclass
class IngestRow:
    post_id: int
    rating: int
    c1: int
    c2: int
    c3: int
    c4: int
    c5: int
    emb: np.ndarray


@dataclass
class BuildRowStats:
    ok: bool
    bytes_downloaded: int
    download_sec: float
    decode_sec: float
    preprocess_sec: float
    forward_sec: float
    transfer_sec: float
    embed_sec: float
    total_sec: float
    embed_batch_size: int
    reason: str = "ok"


@dataclass
class DownloadedPost:
    post_id: int
    rating: int
    comp: tuple[int, int, int, int, int] | None
    image: Image.Image | None
    stats: BuildRowStats


class AdaptiveDownloadController:
    def __init__(self) -> None:
        self.min_workers = max(1, int(settings.ingest_download_workers_min))
        self.max_workers = max(self.min_workers, int(settings.ingest_download_workers_max))
        initial = int(settings.ingest_download_workers)
        self.current_workers = min(self.max_workers, max(self.min_workers, initial))
        self._window: list[BuildRowStats] = []
        self._interval = max(1, int(settings.ingest_download_autotune_interval))

    def observe(self, stats: BuildRowStats) -> None:
        self._window.append(stats)
        if len(self._window) < self._interval:
            return

        batch = self._window
        self._window = []
        total = max(1, len(batch))
        blocked = sum(1 for item in batch if item.reason in {"http_403", "http_429"})
        download_avg = sum(item.download_sec for item in batch) / total
        embed_avg = sum(item.embed_sec for item in batch) / total
        failure_rate = sum(1 for item in batch if not item.ok) / total

        target = self.current_workers
        if blocked > 0:
            target = max(self.min_workers, self.current_workers - max(1, blocked))
        elif failure_rate > 0.25:
            target = max(self.min_workers, self.current_workers - 1)
        elif download_avg > max(embed_avg * 1.25, 0.05):
            target = min(self.max_workers, self.current_workers + 1)

        if target != self.current_workers:
            print(
                "ingest autotune: "
                f"workers={self.current_workers}->{target} blocked={blocked}/{total} "
                f"download_avg_ms={download_avg*1000:.1f} embed_avg_ms={embed_avg*1000:.1f}"
            )
            self.current_workers = target


class RollingParquetWriter:
    def __init__(self, prefix: str) -> None:
        self.prefix = prefix
        self.max_rows = max(1, int(settings.ingest_roll_max_rows))
        self.max_bytes = max(1, int(settings.ingest_roll_max_mib)) * 1024 * 1024
        self._writer: pq.ParquetWriter | None = None
        self._tmp_path: Path | None = None
        self._opened_at: str | None = None
        self._file_index = 0
        self._active_index = 0
        self._rows_in_file = 0
        self._bytes_in_file = 0
        self._min_id: int | None = None
        self._max_id: int | None = None

    def write_rows(self, rows: list[IngestRow]) -> None:
        if not rows:
            return
        batch_rows = len(rows)
        batch_bytes = sum((r.emb.size * 4) + 48 for r in rows)
        if self._writer is None or self._should_rotate(batch_rows, batch_bytes):
            self.close()
            self._open_writer()
        table = rows_to_table(rows)
        assert self._writer is not None
        self._writer.write_table(table)
        self._rows_in_file += batch_rows
        self._bytes_in_file += batch_bytes
        ids = [r.post_id for r in rows]
        batch_min = min(ids)
        batch_max = max(ids)
        self._min_id = batch_min if self._min_id is None else min(self._min_id, batch_min)
        self._max_id = batch_max if self._max_id is None else max(self._max_id, batch_max)

    def close(self) -> None:
        if self._writer is None or self._tmp_path is None or self._opened_at is None:
            return
        self._writer.close()
        final_min = self._min_id if self._min_id is not None else 0
        final_max = self._max_id if self._max_id is not None else 0
        final_path = (
            settings.ingest_incoming_dir
            / f"{self.prefix}_{self._opened_at}_{self._active_index:04d}_{final_max}-{final_min}.parquet"
        )
        self._tmp_path.replace(final_path)
        print(f"Wrote parquet: {final_path}")
        self._writer = None
        self._tmp_path = None
        self._opened_at = None
        self._rows_in_file = 0
        self._bytes_in_file = 0
        self._min_id = None
        self._max_id = None

    def _should_rotate(self, next_rows: int, next_bytes: int) -> bool:
        if self._rows_in_file <= 0:
            return False
        return (self._rows_in_file + next_rows) > self.max_rows or (self._bytes_in_file + next_bytes) > self.max_bytes

    def _open_writer(self) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        tmp_path = settings.ingest_incoming_dir / f"{self.prefix}_{ts}_{self._file_index:04d}.parquet.part"
        settings.ingest_incoming_dir.mkdir(parents=True, exist_ok=True)
        self._writer = pq.ParquetWriter(tmp_path, schema=_row_schema())
        self._tmp_path = tmp_path
        self._opened_at = ts
        self._active_index = self._file_index
        self._file_index += 1


def _as_i64_bits_u64(value: int) -> int:
    return int(np.uint64(value).view(np.int64))


def parse_cdn_components(url: str) -> tuple[int, int, int, int, int] | None:
    m = _CDN_RE.match(url.strip())
    if not m:
        return None

    c1_hex, c2_hex, md5_hex, ext = m.group(1), m.group(2), m.group(3), m.group(4).lower()
    c1 = int(c1_hex, 16)
    c2 = int(c2_hex, 16)
    md5_u128 = int(md5_hex, 16)
    hi_u64 = (md5_u128 >> 64) & 0xFFFFFFFFFFFFFFFF
    lo_u64 = md5_u128 & 0xFFFFFFFFFFFFFFFF
    c3 = _as_i64_bits_u64(hi_u64)
    c4 = _as_i64_bits_u64(lo_u64)
    c5 = int(EXT_TO_CODE.get(ext, 0))
    return (c1, c2, c3, c4, c5)


def rating_to_int(raw: str) -> int:
    return int(RATING_MAP.get((raw or "").strip().lower(), 0))


def _ext_from_url(url: str) -> str:
    q = url.split("?", 1)[0].rstrip("/")
    if "." not in q:
        return ""
    return q.rsplit(".", 1)[-1].lower()


def _normalize_danbooru_url(url: str) -> str:
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return settings.danbooru_api_base.rstrip("/") + url
    return url


def _pick_variant(post: dict[str, Any], *, preferred_types: dict[str, int]) -> dict[str, Any] | None:
    media_asset = post.get("media_asset")
    if not isinstance(media_asset, dict):
        return None
    variants = media_asset.get("variants")
    if not isinstance(variants, list):
        return None

    cand: list[tuple[int, int, dict[str, Any]]] = []
    for item in variants:
        if not isinstance(item, dict):
            continue
        url = item.get("url")
        vtype = str(item.get("type", ""))
        ext = str(item.get("file_ext", "")).lower()
        width = int(item.get("width", 0) or 0)
        height = int(item.get("height", 0) or 0)
        if not isinstance(url, str) or not url:
            continue
        if ext and ext not in _SUPPORTED_IMAGE_EXTS:
            continue
        shortest_edge = min(width, height) if width > 0 and height > 0 else 0
        below_min_penalty = 1 if 0 < shortest_edge < _MIN_EMBED_EDGE else 0
        cand.append((below_min_penalty, preferred_types.get(vtype, 10), item))

    if not cand:
        return None
    cand.sort(key=lambda x: (x[0], x[1]))
    return cand[0][2]


def choose_image_url(post: dict[str, Any]) -> str | None:
    variant = _pick_variant(post, preferred_types={"sample": 0, "720x720": 1, "original": 2, "360x360": 3, "180x180": 4})
    if variant:
        url = variant.get("url")
        if isinstance(url, str) and url:
            return _normalize_danbooru_url(url)

    for key in ("preview_file_url", "large_file_url", "file_url"):
        v = post.get(key)
        if isinstance(v, str) and v:
            v = _normalize_danbooru_url(v)
            ext = _ext_from_url(v)
            if ext and ext not in _SUPPORTED_IMAGE_EXTS:
                continue
            return v
    return None


def choose_record_url(post: dict[str, Any]) -> str | None:
    media_asset = post.get("media_asset")
    if isinstance(media_asset, dict):
        variants = media_asset.get("variants")
        if isinstance(variants, list):
            for item in variants:
                if not isinstance(item, dict):
                    continue
                if str(item.get("type", "")) != "720x720":
                    continue
                url = item.get("url")
                if isinstance(url, str) and url:
                    return _normalize_danbooru_url(url)
    return None


def components_from_record_url(record_url: str) -> tuple[int, int, int, int, int] | None:
    return parse_cdn_components(record_url)


def fetch_posts_page(client: httpx.Client, page: str, limit: int) -> list[dict[str, Any]]:
    url = f"{settings.danbooru_api_base.rstrip('/')}/posts.json"
    resp = _get_with_retry(client, url, params={"page": page, "limit": str(limit)}, kind="api")
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        return []
    return [x for x in data if isinstance(x, dict)]


def build_row_from_post(client: httpx.Client, post: dict[str, Any]) -> IngestRow | None:
    row, _ = build_row_from_post_with_stats(client, post)
    return row


def build_row_from_post_with_stats(client: httpx.Client, post: dict[str, Any]) -> tuple[IngestRow | None, BuildRowStats]:
    downloaded = download_post_with_stats(client, post)
    if downloaded.image is None:
        return None, downloaded.stats
    return build_rows_from_downloaded_batch([downloaded])[0]


def _embed_downloaded_batch(downloaded_batch: list[DownloadedPost]) -> tuple[np.ndarray | None, float, float, float]:
    if not downloaded_batch:
        return None, 0.0, 0.0, 0.0
    images = [item.image for item in downloaded_batch if item.image is not None]
    if len(images) != len(downloaded_batch):
        return None, 0.0, 0.0, 0.0
    result = tag_engine.extract_image_features_with_stats(images)
    if result is None:
        return None, 0.0, 0.0, 0.0
    return result.features, result.preprocess_sec, result.forward_sec, result.transfer_sec


def build_rows_from_downloaded_batch(
    downloaded_batch: list[DownloadedPost],
) -> list[tuple[int, IngestRow | None, BuildRowStats]]:
    if not downloaded_batch:
        return []

    valid_batch = [item for item in downloaded_batch if item.image is not None and item.comp is not None]
    invalid_results: list[tuple[int, IngestRow | None, BuildRowStats]] = []
    for item in downloaded_batch:
        if item.image is None or item.comp is None:
            invalid_results.append((item.post_id, None, item.stats))

    feats, preprocess_sec, forward_sec, transfer_sec = _embed_downloaded_batch(valid_batch)
    batch_size = len(valid_batch)
    if batch_size <= 0:
        return invalid_results

    per_item_preprocess = preprocess_sec / batch_size
    per_item_forward = forward_sec / batch_size
    per_item_transfer = transfer_sec / batch_size
    per_item_embed = (preprocess_sec + forward_sec + transfer_sec) / batch_size
    results: list[tuple[int, IngestRow | None, BuildRowStats]] = []

    if feats is None or feats.shape[0] != batch_size:
        for item in valid_batch:
            results.append(
                (
                    item.post_id,
                    None,
                    BuildRowStats(
                        False,
                        item.stats.bytes_downloaded,
                        item.stats.download_sec,
                        item.stats.decode_sec,
                        per_item_preprocess,
                        per_item_forward,
                        per_item_transfer,
                        per_item_embed,
                        item.stats.total_sec + per_item_embed,
                        batch_size,
                        "embed_failed",
                    ),
                )
            )
        return invalid_results + results

    for item, feat in zip(valid_batch, feats, strict=True):
        emb = np.asarray(feat, dtype=np.float32).reshape(-1)
        if emb.shape[0] != settings.embedding_dim:
            results.append(
                (
                    item.post_id,
                    None,
                    BuildRowStats(
                        False,
                        item.stats.bytes_downloaded,
                        item.stats.download_sec,
                        item.stats.decode_sec,
                        per_item_preprocess,
                        per_item_forward,
                        per_item_transfer,
                        per_item_embed,
                        item.stats.total_sec + per_item_embed,
                        batch_size,
                        "embed_dim_mismatch",
                    ),
                )
            )
            continue

        c1, c2, c3, c4, c5 = item.comp
        row = IngestRow(post_id=item.post_id, rating=item.rating, c1=c1, c2=c2, c3=c3, c4=c4, c5=c5, emb=emb)
        results.append(
            (
                item.post_id,
                row,
                BuildRowStats(
                    True,
                    item.stats.bytes_downloaded,
                    item.stats.download_sec,
                    item.stats.decode_sec,
                    per_item_preprocess,
                    per_item_forward,
                    per_item_transfer,
                    per_item_embed,
                    item.stats.total_sec + per_item_embed,
                    batch_size,
                    "ok",
                ),
            )
        )
    return invalid_results + results


def download_post_with_stats(client: httpx.Client, post: dict[str, Any]) -> DownloadedPost:
    t0 = perf_counter()
    bytes_downloaded = 0
    download_sec = 0.0
    decode_sec = 0.0

    post_id = int(post.get("id", 0) or 0)
    if post_id <= 0:
        return DownloadedPost(post_id, 0, None, None, BuildRowStats(False, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "invalid_id"))
    if bool(post.get("is_deleted")):
        return DownloadedPost(post_id, 0, None, None, BuildRowStats(False, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "deleted"))

    source_url = choose_image_url(post)
    if not source_url:
        return DownloadedPost(post_id, 0, None, None, BuildRowStats(False, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "no_supported_url"))

    record_url = choose_record_url(post)
    if not record_url:
        return DownloadedPost(post_id, 0, None, None, BuildRowStats(False, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "no_record_url"))

    comp = components_from_record_url(record_url)
    if comp is None:
        return DownloadedPost(post_id, 0, None, None, BuildRowStats(False, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "component_parse_failed"))

    try:
        td0 = perf_counter()
        img_resp = _get_with_retry(client, source_url, timeout=30.0, kind="media")
        if img_resp.status_code >= 400:
            return DownloadedPost(
                post_id,
                0,
                None,
                None,
                BuildRowStats(
                    False,
                    0,
                    perf_counter() - td0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    perf_counter() - t0,
                    0,
                    f"http_{img_resp.status_code}",
                ),
            )
        download_sec = perf_counter() - td0
        bytes_downloaded = len(img_resp.content)

        tdec0 = perf_counter()
        img = Image.open(io.BytesIO(img_resp.content))
        img = img.convert("RGB")
        decode_sec = perf_counter() - tdec0
    except Exception:
        return DownloadedPost(
            post_id,
            0,
            None,
            None,
            BuildRowStats(False, bytes_downloaded, download_sec, decode_sec, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "download_or_decode_failed"),
        )

    return DownloadedPost(
        post_id=post_id,
        rating=rating_to_int(str(post.get("rating", "g"))),
        comp=comp,
        image=img,
        stats=BuildRowStats(True, bytes_downloaded, download_sec, decode_sec, 0.0, 0.0, 0.0, 0.0, perf_counter() - t0, 0, "downloaded"),
    )


def process_posts_with_stats(
    client: httpx.Client,
    posts: list[dict[str, Any]],
    controller: AdaptiveDownloadController | None = None,
) -> list[tuple[int, IngestRow | None, BuildRowStats]]:
    if not posts:
        return []
    downloader = controller or AdaptiveDownloadController()
    results: list[tuple[int, IngestRow | None, BuildRowStats]] = []
    pending_embed: list[DownloadedPost] = []
    first_pending_at: float | None = None
    embed_batch_size = max(1, int(getattr(settings, "ingest_embed_batch_size", 1)))
    embed_max_wait_sec = max(0.0, float(getattr(settings, "ingest_embed_max_wait_ms", 0.0))) / 1000.0

    workers = min(downloader.current_workers, max(1, len(posts)))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_map = {pool.submit(download_post_with_stats, client, post): post for post in posts}
        for future in as_completed(future_map):
            downloaded = future.result()
            if downloaded.image is None or downloaded.comp is None:
                downloader.observe(downloaded.stats)
                results.append((downloaded.post_id, None, downloaded.stats))
                continue

            if not pending_embed:
                first_pending_at = perf_counter()
            pending_embed.append(downloaded)
            should_flush = len(pending_embed) >= embed_batch_size
            if not should_flush and first_pending_at is not None and embed_max_wait_sec > 0:
                should_flush = (perf_counter() - first_pending_at) >= embed_max_wait_sec
            if should_flush:
                flushed = build_rows_from_downloaded_batch(list(pending_embed))
                pending_embed.clear()
                first_pending_at = None
                for item in flushed:
                    downloader.observe(item[2])
                results.extend(flushed)
    if pending_embed:
        flushed = build_rows_from_downloaded_batch(list(pending_embed))
        for item in flushed:
            downloader.observe(item[2])
        results.extend(flushed)
    return results


def write_rows_to_parquet(rows: list[IngestRow], target_path: Path) -> None:
    if not rows:
        return
    table = rows_to_table(rows)
    target_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, target_path)


def rows_to_table(rows: list[IngestRow]) -> pa.Table:
    c3_u64 = [int(np.array([r.c3], dtype=np.int64).view(np.uint64)[0]) for r in rows]
    c4_u64 = [int(np.array([r.c4], dtype=np.int64).view(np.uint64)[0]) for r in rows]
    data = {
        "id": pa.array([r.post_id for r in rows], type=pa.int64()),
        "rating": pa.array([r.rating for r in rows], type=pa.int64()),
        "url_c1": pa.array([r.c1 for r in rows], type=pa.int64()),
        "url_c2": pa.array([r.c2 for r in rows], type=pa.int64()),
        "url_c3": pa.array(c3_u64, type=pa.uint64()),
        "url_c4": pa.array(c4_u64, type=pa.uint64()),
        "url_c5": pa.array([r.c5 for r in rows], type=pa.int64()),
        "emb": pa.array([r.emb.tolist() for r in rows], type=pa.list_(pa.float32())),
    }
    return pa.Table.from_pydict(data, schema=_row_schema())


def _row_schema() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.int64()),
            ("rating", pa.int64()),
            ("url_c1", pa.int64()),
            ("url_c2", pa.int64()),
            ("url_c3", pa.uint64()),
            ("url_c4", pa.uint64()),
            ("url_c5", pa.int64()),
            ("emb", pa.list_(pa.float32())),
        ]
    )


def make_client() -> httpx.Client:
    return httpx.Client(
        timeout=30.0,
        follow_redirects=True,
        http2=True,
        limits=httpx.Limits(max_connections=64, max_keepalive_connections=32, keepalive_expiry=20.0),
        headers={"User-Agent": "booruViewer/0.1 (+https://danbooru.donmai.us/)"},
    )


def _sleep_if_needed(kind: str) -> None:
    wait = settings.ingest_sleep_sec if kind == "api" else 0.0
    if wait > 0:
        time.sleep(wait)


def _parse_retry_after_sec(value: str | None) -> float | None:
    if not value:
        return None
    raw = value.strip()
    if not raw:
        return None
    try:
        as_int = int(raw)
        if as_int >= 0:
            return float(as_int)
    except ValueError:
        pass
    try:
        dt = parsedate_to_datetime(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        sec = (dt - now).total_seconds()
        if sec > 0:
            return sec
    except Exception:
        return None
    return None


def _get_with_retry(
    client: httpx.Client,
    url: str,
    *,
    params: dict[str, str] | None = None,
    timeout: float | None = None,
    kind: str,
) -> httpx.Response:
    max_retries = max(0, int(getattr(settings, "ingest_http_max_retries", 4)))
    backoff_base = float(getattr(settings, "ingest_http_retry_base_sec", 0.6))
    backoff_factor = float(getattr(settings, "ingest_http_retry_backoff", 1.8))
    block_floor = float(getattr(settings, "ingest_http_block_cooldown_sec", 2.0))
    delay = max(0.05, backoff_base)

    for attempt in range(max_retries + 1):
        _sleep_if_needed(kind)
        try:
            resp = client.get(url, params=params, timeout=timeout)
        except httpx.HTTPError:
            if attempt >= max_retries:
                raise
            time.sleep(delay)
            delay = min(30.0, delay * max(1.0, backoff_factor))
            continue

        if resp.status_code < 400:
            return resp
        if resp.status_code not in _RETRY_STATUS and resp.status_code not in _BLOCK_STATUS:
            return resp
        if attempt >= max_retries:
            return resp

        retry_after = _parse_retry_after_sec(resp.headers.get("retry-after"))
        wait = max(delay, retry_after or 0.0)
        if resp.status_code in _BLOCK_STATUS:
            wait = max(wait, block_floor)
        time.sleep(wait)
        delay = min(60.0, wait * max(1.0, backoff_factor))

    raise RuntimeError("unreachable retry loop")
