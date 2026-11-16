from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from window_planner import plan_next_window


def utc(hour, minute=0):
    return datetime(
        2026, 11, 15, hour, minute,
        tzinfo=timezone.utc,
    )


def test_regular_one_hour_window():
    result = plan_next_window(
        utc(10), utc(14)
    )
    assert result == (utc(10), utc(11))


def test_window_respects_safety_lag():
    result = plan_next_window(
        utc(10), utc(10, 35),
        window_minutes=60,
        safety_lag_minutes=10,
    )
    assert result == (utc(10), utc(10, 25))


def test_no_window_when_lag_not_elapsed():
    result = plan_next_window(
        utc(10), utc(10, 5),
        safety_lag_minutes=10,
    )
    assert result is None


def test_timezone_conversion_and_invalid_inputs():
    argentina = timezone(timedelta(hours=-3))

    checkpoint = utc(10).astimezone(argentina)

    assert plan_next_window(
        checkpoint, utc(12)
    ) == (utc(10), utc(11))

    with pytest.raises(ValueError):
        plan_next_window(
            utc(10).replace(tzinfo=None),
            utc(12),
        )

    with pytest.raises(ValueError):
        plan_next_window(
            utc(10), utc(12),
            window_minutes=0,
        )

    with pytest.raises(ValueError):
        plan_next_window(
            utc(10), utc(12),
            safety_lag_minutes=-1,
        )