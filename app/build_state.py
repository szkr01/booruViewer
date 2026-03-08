from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import settings


@dataclass
class PendingRange:
    upper_id: int
    lower_id: int
    cursor_id: int
    source: str = "probe"
    status: str = "pending"

    def normalized(self) -> "PendingRange | None":
        upper = int(self.upper_id or 0)
        lower = int(self.lower_id or 0)
        if upper <= 0 or lower <= 0 or upper < lower:
            return None
        cursor = int(self.cursor_id or (upper + 1))
        cursor = min(max(cursor, lower + 1), upper + 1)
        source = str(self.source or "probe")
        if source not in {"latest", "probe"}:
            source = "probe"
        status = str(self.status or "pending")
        if status not in {"pending", "active"}:
            status = "pending"
        return PendingRange(
            upper_id=upper,
            lower_id=lower,
            cursor_id=cursor,
            source=source,
            status=status,
        )


@dataclass
class BuildState:
    sync_failures: list[int] | None = None
    pending_ranges: list[PendingRange] | None = None
    probe_resume_id: int = 0

    def __post_init__(self) -> None:
        if self.sync_failures is None:
            self.sync_failures = []
        if self.pending_ranges is None:
            self.pending_ranges = []

    def compact(self) -> None:
        self.sync_failures = sorted(set(int(x) for x in self.sync_failures))[-5000:]
        normalized: list[PendingRange] = []
        seen: set[tuple[int, int]] = set()
        for pending_range in self.pending_ranges:
            compacted = pending_range.normalized()
            if compacted is None:
                continue
            key = (compacted.upper_id, compacted.lower_id)
            if key in seen:
                continue
            seen.add(key)
            normalized.append(compacted)
        self.pending_ranges = normalized
        self.probe_resume_id = max(0, int(self.probe_resume_id or 0))


class BuildStateStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = path or settings.build_state_path

    def load(self) -> BuildState:
        if not self.path.exists():
            return BuildState()
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                return BuildState()
            sync_failures = self._to_int_list(raw.get("sync_failures", raw.get("latest_failures", [])))
            pending_ranges = self._load_pending_ranges(raw)
            return BuildState(
                sync_failures=sync_failures,
                pending_ranges=pending_ranges,
                probe_resume_id=int(raw.get("probe_resume_id", raw.get("sync_cursor_id", raw.get("latest_cursor_id", 0))) or 0),
            )
        except Exception:
            return BuildState()

    def save(self, state: BuildState) -> None:
        state.compact()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(asdict(state), ensure_ascii=True, indent=2)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(payload, encoding="utf-8")
        os.replace(tmp_path, self.path)

    def _to_int_list(self, value: object) -> list[int]:
        if not isinstance(value, list):
            return []
        result: list[int] = []
        for x in value:
            try:
                result.append(int(x))  # noqa: PERF401
            except Exception:
                continue
        return result

    def _load_pending_ranges(self, raw: dict[str, object]) -> list[PendingRange]:
        pending_raw = raw.get("pending_ranges")
        result: list[PendingRange] = []
        if isinstance(pending_raw, list):
            for item in pending_raw:
                if not isinstance(item, dict):
                    continue
                normalized = PendingRange(
                    upper_id=int(item.get("upper_id", 0) or 0),
                    lower_id=int(item.get("lower_id", 0) or 0),
                    cursor_id=int(item.get("cursor_id", 0) or 0),
                    source=str(item.get("source", "probe") or "probe"),
                    status=str(item.get("status", "pending") or "pending"),
                ).normalized()
                if normalized is not None:
                    result.append(normalized)
            return result

        legacy = PendingRange(
            upper_id=int(raw.get("active_gap_upper_id", 0) or 0),
            lower_id=int(raw.get("active_gap_lower_id", 0) or 0),
            cursor_id=int(raw.get("active_gap_cursor_id", 0) or 0),
            source="probe",
            status="active",
        ).normalized()
        if legacy is not None:
            result.append(legacy)
        return result


build_state = BuildStateStore()
