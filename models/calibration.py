from __future__ import annotations

import pandas as pd

from pipeline.stages.phase2_utils import clamp


def build_business_calibration(scoring: pd.DataFrame) -> pd.DataFrame:
    calibration = (
        scoring.groupby("business_id")["business_fill_trend"]
        .mean()
        .rename("local_fill")
        .reset_index()
    )
    global_fill = float(scoring["business_fill_trend"].mean()) if len(scoring) else 0.5
    if global_fill <= 0:
        global_fill = 0.5
    calibration["calibration_factor"] = calibration["local_fill"].apply(
        lambda x: clamp(float(x) / global_fill, 0.8, 1.2)
    )
    return calibration
