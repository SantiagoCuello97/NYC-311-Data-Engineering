"""Pure planning for bounded incremental windows."""

from datetime import datetime, timedelta, timezone


def plan_next_window(
    checkpoint,
    now,
    window_minutes=60,
    safety_lag_minutes=10,
):
    """Return (lower, upper), or None if no window is eligible.

    All bounds are timezone-aware. Upper is exclusive.
    This planner does not access the API or database.
    """

    if checkpoint.tzinfo is None or now.tzinfo is None:
        raise ValueError("Timezone-aware timestamps required")

    if window_minutes <= 0:
        raise ValueError("Window duration must be positive")

    if safety_lag_minutes < 0:
        raise ValueError("Safety lag cannot be negative")

    lower = checkpoint.astimezone(timezone.utc)
    current = now.astimezone(timezone.utc)

    eligible_upper = current - timedelta(
        minutes=safety_lag_minutes
    )

    if eligible_upper <= lower:
        return None

    upper = min(
        lower + timedelta(minutes=window_minutes),
        eligible_upper,
    )

    return lower, upper