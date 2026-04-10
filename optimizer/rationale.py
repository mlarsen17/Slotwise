from __future__ import annotations


def generate_rationale_codes(
    *,
    underbooked: bool,
    hours_until_slot: float,
    provider_utilization_7d: float,
    chosen_discount: int,
) -> list[str]:
    codes: list[str] = []
    if underbooked:
        codes.append("booking_pace_below_baseline")
    if underbooked and float(hours_until_slot) <= 24:
        codes.append("short_lead_time_low_fill")
    if underbooked and float(provider_utilization_7d or 0.0) < 0.5:
        codes.append("provider_utilization_below_target")
    if not codes and chosen_discount > 0:
        codes.append("historically_underbooked_weekday_afternoon")
    if not codes:
        codes.append("healthy_slot_no_discount")
    return sorted(set(codes))
