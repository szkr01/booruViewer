from __future__ import annotations

import argparse
from collections import Counter
import signal
from time import perf_counter

from .build_state import build_state
from .config import settings
from .database import db
from .ingest_posts import (
    AdaptiveDownloadController,
    IngestRow,
    fetch_posts_page,
    make_client,
    process_posts_with_stats,
    RollingParquetWriter,
)
from .tag_engine import tag_engine


class _StopRequested(Exception):
    pass


_SKIP_REASONS = {"deleted", "no_supported_url"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Walk Danbooru posts from latest to older and emit parquet batches")
    parser.add_argument("--page-size", type=int, default=settings.ingest_page_size)
    parser.add_argument("--batch-size", type=int, default=settings.ingest_batch_size)
    parser.add_argument("--max-runtime-sec", type=float, default=settings.collector_phase_budget_sec)
    return parser.parse_args()


def _fetch_latest_head_id(client, page_size: int) -> int:
    posts = fetch_posts_page(client, page="1", limit=min(5, page_size))
    if not posts:
        return 0
    return max(int(post.get("id", 0) or 0) for post in posts)


def main() -> None:
    args = parse_args()
    page_size = max(1, min(200, int(args.page_size)))
    batch_size = max(1, int(args.batch_size))
    max_runtime_sec = max(0.0, float(args.max_runtime_sec))
    print(
        f"sync_posts start: page_size={page_size}, batch_size={batch_size}, "
        f"max_runtime_sec={max_runtime_sec:.1f}"
    )

    t_start = perf_counter()
    tag_engine.load()
    if settings.require_cuda_for_ingest and tag_engine.device.type != "cuda":
        raise RuntimeError(
            "CUDA is required for ingest, but current device is CPU. "
            "Check torch CUDA install and GPU visibility."
        )
    print(f"sync_posts device: {tag_engine.device}")
    state = build_state.load()

    total_seen = 0
    total_emitted = 0
    total_failed = 0
    total_skipped = 0
    failure_reasons: Counter[str] = Counter()
    skipped_reasons: Counter[str] = Counter()
    rows: list[IngestRow] = []
    writer = RollingParquetWriter("latest")
    download_controller = AdaptiveDownloadController()
    min_seen_id = 0
    pages = 0
    page_fetch_sec = 0.0
    bytes_downloaded = 0
    download_sec = 0.0
    decode_sec = 0.0
    embed_sec = 0.0
    processed_for_perf = 0
    metric_every = 25
    state_save_interval_sec = max(1.0, float(getattr(settings, "ingest_state_save_interval_sec", 300.0)))
    last_saved_at = perf_counter()
    stop_requested = False
    interrupted = False
    yielded_for_budget = False

    def _save_state_if_needed(*, force: bool = False) -> None:
        nonlocal last_saved_at
        now = perf_counter()
        by_time = (now - last_saved_at) >= state_save_interval_sec
        if force or by_time:
            build_state.save(state)
            last_saved_at = now

    def _request_stop(_sig: int, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True
        print("sync_posts: stop requested, flushing progress...")

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    try:
        with make_client() as client:
            latest_head_id = _fetch_latest_head_id(client, page_size)
            if latest_head_id <= 0:
                print("sync_posts skipped: latest head id unavailable")
                return
            cursor = int(state.sync_cursor_id or 0)
            if cursor <= 1 or cursor > (latest_head_id + 1):
                cursor = latest_head_id + 1
            min_seen_id = cursor
            print(
                f"sync_posts cursor_init: state_sync={state.sync_cursor_id}, "
                f"latest_head={latest_head_id}, cursor={cursor}"
            )
            while True:
                if stop_requested:
                    raise _StopRequested()
                if max_runtime_sec > 0 and (perf_counter() - t_start) >= max_runtime_sec:
                    yielded_for_budget = True
                    print(
                        "sync_posts: phase budget reached, yielding to next collector stage "
                        f"after {max_runtime_sec:.1f}s"
                    )
                    break
                page = f"b{cursor}"

                tf0 = perf_counter()
                posts = fetch_posts_page(client, page=page, limit=page_size)
                page_fetch_sec += perf_counter() - tf0
                pages += 1
                if not posts:
                    break

                page_ids = [int(p.get("id", 0) or 0) for p in posts if int(p.get("id", 0) or 0) > 0]
                if not page_ids:
                    break

                existing = db.existing_ids(page_ids)
                pending_posts = []
                for post in posts:
                    if stop_requested:
                        raise _StopRequested()
                    post_id = int(post.get("id", 0) or 0)
                    if post_id <= 0:
                        continue
                    min_seen_id = min(min_seen_id, post_id)
                    if post_id in existing:
                        continue
                    pending_posts.append(post)

                for post_id, row, st in process_posts_with_stats(client, pending_posts, download_controller):
                    bytes_downloaded += st.bytes_downloaded
                    download_sec += st.download_sec
                    decode_sec += st.decode_sec
                    embed_sec += st.embed_sec
                    processed_for_perf += 1
                    if row is None:
                        if st.reason in _SKIP_REASONS:
                            total_skipped += 1
                            skipped_reasons[st.reason] += 1
                        else:
                            total_failed += 1
                            failure_reasons[st.reason] += 1
                            state.sync_failures.append(post_id)
                    else:
                        rows.append(row)
                        total_emitted += 1

                    total_seen += 1
                    if max_runtime_sec > 0 and (perf_counter() - t_start) >= max_runtime_sec:
                        yielded_for_budget = True
                    if len(rows) >= batch_size:
                        writer.write_rows(rows)
                        rows.clear()
                        state.sync_cursor_id = max(1, min_seen_id)
                        state.sync_failures = sorted(set(state.sync_failures))[-5000:]
                        _save_state_if_needed()
                    if processed_for_perf % metric_every == 0:
                        elapsed = max(perf_counter() - t_start, 1e-9)
                        net_mb = bytes_downloaded / (1024.0 * 1024.0)
                        print(
                            "sync_posts perf: "
                            f"processed={processed_for_perf} emitted={total_emitted} skipped={total_skipped} failed={total_failed} "
                            f"overall={processed_for_perf/elapsed:.2f} img/s "
                            f"api_avg_ms={(page_fetch_sec/max(pages,1))*1000:.1f} "
                            f"dl_avg_ms={(download_sec/max(processed_for_perf,1))*1000:.1f} "
                            f"embed_avg_ms={(embed_sec/max(processed_for_perf,1))*1000:.1f} "
                            f"net={net_mb/max(download_sec,1e-9):.2f} MiB/s "
                            f"skip_reasons={dict(skipped_reasons.most_common(4))} "
                            f"fail_reasons={dict(failure_reasons.most_common(4))}"
                        )
                    if yielded_for_budget:
                        break
                if yielded_for_budget:
                    print(
                        "sync_posts: phase budget reached, stopping after current page "
                        f"at cursor={min_seen_id}"
                    )
                    break
                cursor = min_seen_id
                state.sync_cursor_id = max(1, cursor)
                state.sync_failures = sorted(set(state.sync_failures))[-5000:]
                _save_state_if_needed()
                if len(posts) < page_size:
                    break
    except _StopRequested:
        interrupted = True
        print("sync_posts: interrupted safely")

    if rows:
        writer.write_rows(rows)
    writer.close()

    state.sync_cursor_id = max(1, min_seen_id)
    state.sync_failures = sorted(set(state.sync_failures))[-5000:]
    _save_state_if_needed(force=True)

    print(
        f"Sync done: cursor={state.sync_cursor_id}, emitted={total_emitted}, "
        f"skipped={total_skipped}, failed={total_failed}, inspected={total_seen}, "
        f"skip_reasons={dict(skipped_reasons.most_common(8))}, "
        f"fail_reasons={dict(failure_reasons.most_common(8))}"
    )
    elapsed = max(perf_counter() - t_start, 1e-9)
    net_mb = bytes_downloaded / (1024.0 * 1024.0)
    print(
        "sync_posts summary: "
        f"elapsed={elapsed:.2f}s pages={pages} page_avg_ms={(page_fetch_sec/max(pages,1))*1000:.1f} "
        f"processed={processed_for_perf} emitted={total_emitted} skipped={total_skipped} failed={total_failed} "
        f"overall={processed_for_perf/elapsed:.2f} img/s "
        f"download_avg_ms={(download_sec/max(processed_for_perf,1))*1000:.1f} "
        f"decode_avg_ms={(decode_sec/max(processed_for_perf,1))*1000:.1f} "
        f"embed_avg_ms={(embed_sec/max(processed_for_perf,1))*1000:.1f} "
        f"net_rate={net_mb/max(download_sec,1e-9):.2f}MiB/s "
        f"device={tag_engine.device} "
        f"skip_reasons={dict(skipped_reasons.most_common(8))} "
        f"fail_reasons={dict(failure_reasons.most_common(8))}"
    )
    if interrupted:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
