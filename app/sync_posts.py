from __future__ import annotations

import argparse
from collections import Counter
import signal
from time import perf_counter
from typing import Iterator

from .build_state import BuildState, PendingRange, build_state
from .config import settings
from .database import db
from .gap_finder import MissingRange, iter_missing_ranges
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


def _iter_probe_ranges(latest_head_id: int, gap_threshold: int, probe_step: int) -> Iterator[MissingRange]:
    return iter_missing_ranges(
        latest_head_id=latest_head_id,
        gap_threshold=gap_threshold,
        probe_step=probe_step,
        prev_existing_id=db.prev_existing_id,
        next_existing_id=db.next_existing_id,
    )


def _normalize_pending_ranges(state: BuildState, latest_head_id: int) -> None:
    normalized: list[PendingRange] = []
    seen: set[tuple[int, int]] = set()
    for pending_range in state.pending_ranges:
        normalized_range = pending_range.normalized()
        if normalized_range is None:
            continue
        upper = min(normalized_range.upper_id, int(latest_head_id))
        lowered = PendingRange(
            upper_id=upper,
            lower_id=normalized_range.lower_id,
            cursor_id=min(normalized_range.cursor_id, upper + 1),
            source=normalized_range.source,
            status=normalized_range.status,
        ).normalized()
        if lowered is None:
            continue
        key = (lowered.upper_id, lowered.lower_id)
        if key in seen:
            continue
        seen.add(key)
        normalized.append(lowered)
    state.pending_ranges = normalized


def _range_key(pending_range: PendingRange) -> tuple[int, int]:
    return (int(pending_range.upper_id), int(pending_range.lower_id))


def _queue_has_range(state: BuildState, upper_id: int, lower_id: int) -> bool:
    key = (int(upper_id), int(lower_id))
    return any(_range_key(pending_range) == key for pending_range in state.pending_ranges)


def _enqueue_range(state: BuildState, pending_range: PendingRange) -> bool:
    normalized = pending_range.normalized()
    if normalized is None:
        return False
    if _queue_has_range(state, normalized.upper_id, normalized.lower_id):
        return False
    state.pending_ranges.append(normalized)
    return True


def _latest_gap_range(latest_head_id: int, db_max_id: int, gap_threshold: int) -> PendingRange | None:
    if (int(latest_head_id) - int(db_max_id)) < int(gap_threshold):
        return None
    return PendingRange(
        upper_id=int(latest_head_id),
        lower_id=max(1, int(db_max_id) + 1),
        cursor_id=int(latest_head_id) + 1,
        source="latest",
        status="active",
    ).normalized()


def _prioritize_latest_range(
    state: BuildState,
    *,
    latest_head_id: int,
    db_max_id: int,
    gap_threshold: int,
) -> bool:
    latest_range = _latest_gap_range(latest_head_id, db_max_id, gap_threshold)
    if latest_range is None:
        return False

    matched_existing: PendingRange | None = None
    remaining: list[PendingRange] = []
    for pending_range in state.pending_ranges:
        if matched_existing is None and _range_key(pending_range) == _range_key(latest_range):
            matched_existing = pending_range
            continue
        remaining.append(pending_range)

    chosen = matched_existing or latest_range
    chosen = PendingRange(
        upper_id=chosen.upper_id,
        lower_id=chosen.lower_id,
        cursor_id=chosen.cursor_id,
        source="latest",
        status="active",
    )
    reordered = [chosen]
    reordered.extend(
        PendingRange(
            upper_id=pending_range.upper_id,
            lower_id=pending_range.lower_id,
            cursor_id=pending_range.cursor_id,
            source=pending_range.source,
            status="pending",
        )
        for pending_range in remaining
    )

    changed = state.pending_ranges != reordered
    state.pending_ranges = reordered
    return changed


def _activate_next_range(state: BuildState) -> PendingRange | None:
    if not state.pending_ranges:
        return None
    active = state.pending_ranges[0]
    if active.status != "active":
        state.pending_ranges[0] = PendingRange(
            upper_id=active.upper_id,
            lower_id=active.lower_id,
            cursor_id=active.cursor_id,
            source=active.source,
            status="active",
        )
    return state.pending_ranges[0]


def _complete_active_range(state: BuildState) -> PendingRange | None:
    if not state.pending_ranges:
        return None
    completed = state.pending_ranges.pop(0)
    state.probe_resume_id = max(0, int(completed.lower_id) - 1)
    return completed


def _plan_pending_ranges(
    state: BuildState,
    *,
    latest_head_id: int,
    db_max_id: int,
    gap_threshold: int,
    probe_step: int,
    probe_limit: int = 1,
) -> int:
    planned = 0
    if _prioritize_latest_range(
        state,
        latest_head_id=latest_head_id,
        db_max_id=db_max_id,
        gap_threshold=gap_threshold,
    ):
        planned += 1

    if len(state.pending_ranges) > 0:
        return planned

    search_head = int(state.probe_resume_id or db_max_id)
    if search_head <= 0:
        state.probe_resume_id = 0
        return planned

    probe_count = 0
    for next_range in _iter_probe_ranges(
        latest_head_id=search_head,
        gap_threshold=gap_threshold,
        probe_step=probe_step,
    ):
        if _enqueue_range(
            state,
            PendingRange(
                upper_id=int(next_range.upper_id),
                lower_id=int(next_range.lower_id),
                cursor_id=int(next_range.upper_id) + 1,
                source="probe",
            ),
        ):
            planned += 1
            probe_count += 1
        if probe_count >= probe_limit:
            break

    if probe_count == 0 and planned == 0:
        state.probe_resume_id = 0
    return planned


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
    gap_threshold = max(1, int(settings.sync_gap_threshold))
    probe_step = max(1, int(settings.sync_probe_step))
    stats = db.get_stats()
    db_max_id = max(0, int(stats["max_id"]))

    total_seen = 0
    total_emitted = 0
    total_failed = 0
    total_skipped = 0
    failure_reasons: Counter[str] = Counter()
    skipped_reasons: Counter[str] = Counter()
    rows: list[IngestRow] = []
    writer = RollingParquetWriter("latest")
    download_controller = AdaptiveDownloadController()
    pages = 0
    page_fetch_sec = 0.0
    bytes_downloaded = 0
    download_sec = 0.0
    decode_sec = 0.0
    preprocess_sec = 0.0
    forward_sec = 0.0
    transfer_sec = 0.0
    embed_sec = 0.0
    embed_batch_total = 0
    processed_for_perf = 0
    metric_every = 25
    state_save_interval_sec = max(1.0, float(getattr(settings, "ingest_state_save_interval_sec", 300.0)))
    last_saved_at = perf_counter()
    stop_requested = False
    interrupted = False
    yielded_for_budget = False
    latest_head_id = 0
    last_cursor = 0
    planning_sec = 0.0

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
            _normalize_pending_ranges(state, latest_head_id)
            if state.probe_resume_id <= 0:
                state.probe_resume_id = db_max_id
            print(
                f"sync_posts cursor_init: latest_head={latest_head_id}, "
                f"gap_threshold={gap_threshold}, probe_step={probe_step}, "
                f"pending_ranges={len(state.pending_ranges)}, probe_resume={state.probe_resume_id}"
            )
            found_range = bool(state.pending_ranges)
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
                if _prioritize_latest_range(
                    state,
                    latest_head_id=latest_head_id,
                    db_max_id=db_max_id,
                    gap_threshold=gap_threshold,
                ):
                    _save_state_if_needed()
                    print(
                        "sync_posts range_prioritize: "
                        f"pending_ranges={len(state.pending_ranges)} "
                        f"latest_upper={latest_head_id} latest_lower={max(1, db_max_id + 1)}"
                    )
                    found_range = True
                if not state.pending_ranges:
                    tp0 = perf_counter()
                    planned = _plan_pending_ranges(
                        state,
                        latest_head_id=latest_head_id,
                        db_max_id=db_max_id,
                        gap_threshold=gap_threshold,
                        probe_step=probe_step,
                    )
                    planning_sec += perf_counter() - tp0
                    _save_state_if_needed()
                    if planned <= 0:
                        break
                    print(
                        "sync_posts range_plan: "
                        f"planned={planned} pending_ranges={len(state.pending_ranges)} probe_resume={state.probe_resume_id}"
                    )
                    found_range = True

                active_range = _activate_next_range(state)
                if active_range is None:
                    break
                cursor = int(active_range.cursor_id)
                print(
                    "sync_posts range_consume: "
                    f"upper={active_range.upper_id} lower={active_range.lower_id} "
                    f"cursor={cursor} size={active_range.upper_id - active_range.lower_id + 1} source={active_range.source}"
                )
                while cursor > active_range.lower_id:
                    if stop_requested:
                        raise _StopRequested()
                    if max_runtime_sec > 0 and (perf_counter() - t_start) >= max_runtime_sec:
                        yielded_for_budget = True
                        print(
                            "sync_posts: phase budget reached, yielding to next collector stage "
                            f"after {max_runtime_sec:.1f}s"
                        )
                        break

                    tf0 = perf_counter()
                    posts = fetch_posts_page(client, page=f"b{cursor}", limit=page_size)
                    page_fetch_sec += perf_counter() - tf0
                    pages += 1
                    if not posts:
                        break

                    page_ids = [int(p.get("id", 0) or 0) for p in posts if int(p.get("id", 0) or 0) > 0]
                    if not page_ids:
                        break
                    page_upper = max(page_ids)
                    page_lower = min(page_ids)

                    pending_posts = [
                        post
                        for post in posts
                        if active_range.lower_id <= int(post.get("id", 0) or 0) <= active_range.upper_id
                    ]
                    pending_ids = [int(post.get("id", 0) or 0) for post in pending_posts if int(post.get("id", 0) or 0) > 0]
                    pending_upper = max(pending_ids) if pending_ids else None
                    pending_lower = min(pending_ids) if pending_ids else None

                    print(
                        "sync_posts page_fetch: "
                        f"cursor={cursor} "
                        f"page_range={page_upper}..{page_lower} "
                        f"pending_range={pending_upper if pending_upper is not None else '-'}.."
                        f"{pending_lower if pending_lower is not None else '-'} "
                        f"pending_posts={len(pending_posts)}"
                    )

                    for post_id, row, st in process_posts_with_stats(client, pending_posts, download_controller):
                        bytes_downloaded += st.bytes_downloaded
                        download_sec += st.download_sec
                        decode_sec += st.decode_sec
                        preprocess_sec += st.preprocess_sec
                        forward_sec += st.forward_sec
                        transfer_sec += st.transfer_sec
                        embed_sec += st.embed_sec
                        embed_batch_total += st.embed_batch_size
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
                            _save_state_if_needed()
                        if processed_for_perf % metric_every == 0:
                            elapsed = max(perf_counter() - t_start, 1e-9)
                            net_mb = bytes_downloaded / (1024.0 * 1024.0)
                            print(
                                "sync_posts ingest_perf: "
                                f"processed={processed_for_perf} emitted={total_emitted} skipped={total_skipped} failed={total_failed} "
                                f"ingest={processed_for_perf/max(download_sec + preprocess_sec + forward_sec + transfer_sec + embed_sec, 1e-9):.2f} img/s "
                                f"overall={processed_for_perf/elapsed:.2f} img/s "
                                f"api_avg_ms={(page_fetch_sec/max(pages,1))*1000:.1f} "
                                f"dl_avg_ms={(download_sec/max(processed_for_perf,1))*1000:.1f} "
                                f"pre_avg_ms={(preprocess_sec/max(processed_for_perf,1))*1000:.1f} "
                                f"fwd_avg_ms={(forward_sec/max(processed_for_perf,1))*1000:.1f} "
                                f"xfer_avg_ms={(transfer_sec/max(processed_for_perf,1))*1000:.1f} "
                                f"embed_avg_ms={(embed_sec/max(processed_for_perf,1))*1000:.1f} "
                                f"embed_batch_avg={embed_batch_total/max(processed_for_perf,1):.2f} "
                                f"net={net_mb/max(download_sec,1e-9):.2f} MiB/s "
                                f"skip_reasons={dict(skipped_reasons.most_common(4))} "
                                f"fail_reasons={dict(failure_reasons.most_common(4))}"
                            )
                        if yielded_for_budget:
                            break
                    if yielded_for_budget:
                        print(
                            "sync_posts: phase budget reached, stopping after current page "
                            f"at cursor={cursor}"
                        )
                        break

                    cursor = min(page_ids)
                    last_cursor = cursor
                    state.pending_ranges[0] = PendingRange(
                        upper_id=active_range.upper_id,
                        lower_id=active_range.lower_id,
                        cursor_id=cursor,
                        source=active_range.source,
                        status="active",
                    )
                    _save_state_if_needed()
                    active_range = state.pending_ranges[0]
                    if cursor < active_range.lower_id or len(posts) < page_size:
                        break

                if yielded_for_budget:
                    break
                completed = _complete_active_range(state)
                if completed is None:
                    break
                print(
                    "sync_posts range_complete: "
                    f"upper={completed.upper_id} lower={completed.lower_id} next_probe={state.probe_resume_id}"
                )
                _save_state_if_needed()
                break
            if not found_range:
                print("sync_posts: no missing range matched threshold")
    except _StopRequested:
        interrupted = True
        print("sync_posts: interrupted safely")

    if rows:
        writer.write_rows(rows)
    writer.close()

    _save_state_if_needed(force=True)

    print(
        f"Sync done: latest_head={latest_head_id}, cursor={max(1, last_cursor)}, emitted={total_emitted}, "
        f"skipped={total_skipped}, failed={total_failed}, inspected={total_seen}, "
        f"skip_reasons={dict(skipped_reasons.most_common(8))}, "
        f"fail_reasons={dict(failure_reasons.most_common(8))}"
    )
    elapsed = max(perf_counter() - t_start, 1e-9)
    net_mb = bytes_downloaded / (1024.0 * 1024.0)
    print(
        "sync_posts summary: "
        f"elapsed={elapsed:.2f}s planning_sec={planning_sec:.2f}s pages={pages} page_avg_ms={(page_fetch_sec/max(pages,1))*1000:.1f} "
        f"processed={processed_for_perf} emitted={total_emitted} skipped={total_skipped} failed={total_failed} "
        f"overall={processed_for_perf/elapsed:.2f} img/s "
        f"download_avg_ms={(download_sec/max(processed_for_perf,1))*1000:.1f} "
        f"decode_avg_ms={(decode_sec/max(processed_for_perf,1))*1000:.1f} "
        f"preprocess_avg_ms={(preprocess_sec/max(processed_for_perf,1))*1000:.1f} "
        f"forward_avg_ms={(forward_sec/max(processed_for_perf,1))*1000:.1f} "
        f"transfer_avg_ms={(transfer_sec/max(processed_for_perf,1))*1000:.1f} "
        f"embed_avg_ms={(embed_sec/max(processed_for_perf,1))*1000:.1f} "
        f"embed_batch_avg={embed_batch_total/max(processed_for_perf,1):.2f} "
        f"net_rate={net_mb/max(download_sec,1e-9):.2f}MiB/s "
        f"device={tag_engine.device} "
        f"skip_reasons={dict(skipped_reasons.most_common(8))} "
        f"fail_reasons={dict(failure_reasons.most_common(8))}"
    )
    if interrupted:
        raise SystemExit(130)


if __name__ == "__main__":
    main()
