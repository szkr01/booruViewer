from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterator


@dataclass(frozen=True)
class MissingRange:
    upper_id: int
    lower_id: int
    lower_existing_id: int | None
    upper_existing_id: int | None

    @property
    def size(self) -> int:
        return self.upper_id - self.lower_id + 1


def iter_missing_ranges(
    *,
    latest_head_id: int,
    gap_threshold: int,
    probe_step: int,
    prev_existing_id: Callable[[int], int | None],
    next_existing_id: Callable[[int], int | None],
    min_probe_id: int = 1,
) -> Iterator[MissingRange]:
    if latest_head_id < 1:
        return

    threshold = max(1, int(gap_threshold))
    step = max(1, min(int(probe_step), max(1, threshold - 1)))
    probe = max(1, int(latest_head_id))
    floor = max(1, int(min_probe_id))

    while probe >= floor:
        lower_existing = prev_existing_id(probe)
        if lower_existing is None:
            upper_id = probe
            lower_id = floor
            if (upper_id - lower_id + 1) >= threshold:
                yield MissingRange(
                    upper_id=upper_id,
                    lower_id=lower_id,
                    lower_existing_id=None,
                    upper_existing_id=None,
                )
            break

        if (probe - lower_existing) >= threshold:
            upper_existing = next_existing_id(probe + 1)
            upper_id = latest_head_id if upper_existing is None else (upper_existing - 1)
            lower_id = lower_existing + 1
            if upper_id >= lower_id and (upper_id - lower_id + 1) >= threshold:
                yield MissingRange(
                    upper_id=upper_id,
                    lower_id=lower_id,
                    lower_existing_id=lower_existing,
                    upper_existing_id=upper_existing,
                )
                probe = lower_existing - 1
                continue

        probe -= step
