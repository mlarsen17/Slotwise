from __future__ import annotations

import pandas as pd


def sort_recommendations(
    recommendations: pd.DataFrame,
    *,
    sort_field: str,
    sort_desc: bool,
) -> pd.DataFrame:
    field = sort_field if sort_field in {"severity_score", "recommended_discount"} else "severity_score"
    return recommendations.sort_values(
        [field, "recommended_discount", "slot_id"],
        ascending=[not sort_desc, False, True],
        kind="mergesort",
    )
