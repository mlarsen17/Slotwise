from __future__ import annotations

from datetime import datetime
from typing import Iterable

import pandas as pd


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


def to_utc_ts(value: datetime) -> pd.Timestamp:
    """Convert a datetime to a UTC pandas timestamp."""
    return (
        pd.Timestamp(value).tz_convert("UTC")
        if pd.Timestamp(value).tzinfo
        else pd.Timestamp(value, tz="UTC")
    )


def filter_events_at_or_before(events: pd.DataFrame, effective_ts: datetime) -> pd.DataFrame:
    """Return only events whose event_at is at or before the snapshot timestamp."""
    if events.empty:
        return events.copy()
    out = events.copy()
    out["event_at"] = pd.to_datetime(out["event_at"], utc=True)
    return out[out["event_at"] <= to_utc_ts(effective_ts)].copy()


def trailing_window_mask(
    timestamps: pd.Series,
    effective_ts: datetime,
    days: int,
) -> pd.Series:
    """Boolean mask for [effective_ts - days, effective_ts] over timestamp series."""
    end = to_utc_ts(effective_ts)
    start = end - pd.Timedelta(days=days)
    ts = pd.to_datetime(timestamps, utc=True)
    return (ts >= start) & (ts <= end)


def unique_count(values: Iterable[object]) -> int:
    """Count unique non-null values from an iterable."""
    return int(pd.Series(list(values)).dropna().nunique())
