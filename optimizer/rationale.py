from __future__ import annotations


def generate_rationale_codes(
    *,
    underbooked: bool,
    hours_until_slot: float,
    provider_utilization_7d: float,
    day_of_week: str,
    time_of_day_bucket: str,
    pace_deviation: float,
    chosen_discount: int,
) -> list[str]:
    codes: set[str] = set()
    if underbooked and pace_deviation < 0:
        codes.add("booking_pace_below_baseline")
    if underbooked and float(hours_until_slot) <= 24:
        codes.add("short_lead_time_low_fill")
    if underbooked and float(provider_utilization_7d or 0.0) < 0.5:
        codes.add("provider_utilization_below_target")
    if (
        underbooked
        and chosen_discount > 0
        and day_of_week in {"mon", "tue", "wed", "thu", "fri"}
        and time_of_day_bucket == "afternoon"
    ):
        codes.add("historically_underbooked_weekday_afternoon")

    if not codes:
        if underbooked:
            codes.add("underbooked_slot_flagged")
        elif chosen_discount > 0:
            codes.add("discount_applied")
        else:
            codes.add("healthy_slot_no_discount")

    return sorted(codes)
