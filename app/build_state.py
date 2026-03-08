from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path

from .config import settings


@dataclass
class BuildState:
    sync_cursor_id: int = 0
    sync_failures: list[int] | None = None

    def __post_init__(self) -> None:
        if self.sync_failures is None:
            self.sync_failures = []


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
            return BuildState(
                sync_cursor_id=int(raw.get("sync_cursor_id", raw.get("latest_cursor_id", 0)) or 0),
                sync_failures=sync_failures,
            )
        except Exception:
            return BuildState()

    def save(self, state: BuildState) -> None:
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


build_state = BuildStateStore()
