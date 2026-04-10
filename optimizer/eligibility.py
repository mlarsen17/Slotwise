from __future__ import annotations


def enforce_lead_time_rule(
    discount: int, *, hours_until_slot: float, allowed_lead_time: int
) -> bool:
    return not (hours_until_slot > allowed_lead_time and discount > 0)


def enforce_excluded_service_rule(
    discount: int, *, service_id: str, excluded_services: list[str]
) -> bool:
    return not (service_id in excluded_services and discount > 0)


def apply_price_floor(
    discount: int, *, standard_price: float, floor_multiplier: float
) -> tuple[bool, float]:
    implied_price = float(standard_price) * (1 - discount / 100.0)
    allowed = implied_price >= float(standard_price) * floor_multiplier
    return allowed, implied_price


def build_eligible_action_set(
    *,
    action_ladder: list[int],
    max_discount_pct: int,
    service_id: str,
    excluded_services: list[str],
    hours_until_slot: float,
    allowed_lead_time: int,
    standard_price: float,
    floor_multiplier: float,
) -> list[int]:
    ladder = sorted(set(int(v) for v in action_ladder if 0 <= int(v) <= max_discount_pct))
    if 0 not in ladder:
        ladder.insert(0, 0)

    eligible: list[int] = []
    for discount in ladder:
        if not enforce_excluded_service_rule(
            discount, service_id=service_id, excluded_services=excluded_services
        ):
            continue
        if not enforce_lead_time_rule(
            discount,
            hours_until_slot=float(hours_until_slot),
            allowed_lead_time=allowed_lead_time,
        ):
            continue
        allowed, _ = apply_price_floor(
            discount,
            standard_price=float(standard_price),
            floor_multiplier=floor_multiplier,
        )
        if not allowed:
            continue
        eligible.append(discount)
    return eligible or [0]
