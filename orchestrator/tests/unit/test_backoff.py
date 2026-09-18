"""Unit tests for exponential backoff."""

import pytest

from services.backoff import backoff_delay


class TestBackoffDelay:
    def test_doubles_up_to_the_cap(self):
        delays = [backoff_delay(n, initial=2, maximum=30) for n in range(1, 8)]

        assert delays == [2, 4, 8, 16, 30, 30, 30]

    def test_initial_at_or_above_cap_is_fixed_delay(self):
        """The old behaviour: every wait is the maximum."""
        assert [backoff_delay(n, 60, 60) for n in (1, 2, 5)] == [60, 60, 60]
        assert backoff_delay(1, initial=90, maximum=60) == 60

    def test_fractional_delays(self):
        assert backoff_delay(1, 0.5, 10) == 0.5
        assert backoff_delay(3, 0.5, 10) == 2.0

    def test_huge_attempt_does_not_overflow(self):
        assert backoff_delay(10_000, initial=2, maximum=30) == 30

    @pytest.mark.parametrize(
        "attempt, initial, maximum, match",
        [
            (0, 2, 30, "attempt"),
            (1, 0, 30, "initial"),
            (1, 2, 0, "maximum"),
            (1, -1, 30, "initial"),
        ],
    )
    def test_invalid_arguments(self, attempt, initial, maximum, match):
        with pytest.raises(ValueError, match=match):
            backoff_delay(attempt, initial, maximum)
