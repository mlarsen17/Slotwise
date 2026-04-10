from __future__ import annotations


def discount_from_severity(severity: float, breakpoints: list[float], discounts: list[int]) -> int:
    if len(discounts) != len(breakpoints) + 1:
        msg = "discount_steps length must equal severity_breakpoints length + 1"
        raise ValueError(msg)
    for idx, threshold in enumerate(breakpoints):
        if severity <= threshold:
            return discounts[idx]
    return discounts[-1]


def select_discount(
    *,
    underbooked: bool,
    healthy_zero_only: bool,
    severity: float,
    breakpoints: list[float],
    discounts: list[int],
    eligible_actions: list[int],
) -> int:
    target = (
        0
        if (healthy_zero_only and not underbooked)
        else discount_from_severity(severity, breakpoints=breakpoints, discounts=discounts)
    )
    return max([v for v in eligible_actions if v <= target], default=min(eligible_actions))
