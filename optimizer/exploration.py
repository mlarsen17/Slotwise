from __future__ import annotations

import hashlib
import random


def deterministic_rng(seed: int, *parts: str) -> random.Random:
    digest = hashlib.sha256(f"{seed}|{'|'.join(parts)}".encode()).hexdigest()[:16]
    return random.Random(int(digest, 16))


def maybe_apply_exploration(
    *,
    random_seed: int,
    run_id: str,
    slot_id: str,
    exploration_share: float,
    underbooked: bool,
    chosen_discount: int,
    eligible_actions: list[int],
) -> tuple[int, bool, str, str]:
    if not underbooked or len(eligible_actions) <= 1:
        return (
            chosen_discount,
            False,
            "none",
            "underbooked_optimizer" if underbooked else "healthy_no_discount",
        )

    rng = deterministic_rng(random_seed, run_id, slot_id)
    if rng.random() >= exploration_share:
        return chosen_discount, False, "none", "underbooked_optimizer"

    exploratory_choice = rng.choice(
        [x for x in eligible_actions if x != chosen_discount] or eligible_actions
    )
    return exploratory_choice, True, "epsilon_greedy_deterministic", "exploration_override"
