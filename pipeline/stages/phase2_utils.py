from __future__ import annotations

from datetime import datetime


def assign_time_of_day_bucket(slot_start_at: datetime, boundaries: list[int]) -> str:
    """Assign a deterministic time-of-day bucket label using configured hour boundaries."""
    hours = sorted({h for h in boundaries if 0 <= h <= 24})
    if not hours or hours[0] != 0:
        hours = [0, *hours]
    if hours[-1] != 24:
        hours.append(24)

    hour = slot_start_at.hour
    for start, end in zip(hours, hours[1:]):
        if start <= hour < end:
            return f"{start:02d}-{end:02d}"
    return f"{hours[-2]:02d}-{hours[-1]:02d}"


def safe_rate(numerator: float, denominator: float) -> float:
    if denominator <= 0:
        return 0.0
    return float(numerator) / float(denominator)


def clamp(value: float, min_value: float, max_value: float) -> float:
    return max(min_value, min(max_value, value))
