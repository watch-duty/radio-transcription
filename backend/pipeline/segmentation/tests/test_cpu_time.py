"""Tests for scoped thread CPU measurement helpers."""

from unittest.mock import patch

from backend.pipeline.segmentation import cpu_time


@patch.object(cpu_time, "read_thread_cpu_ns", return_value=8_500)
def test_elapsed_thread_cpu_us_uses_integer_microseconds(
    _mock_thread_cpu: object,
) -> None:
    """Converts nanoseconds to nonnegative integer microseconds."""
    assert cpu_time.elapsed_thread_cpu_us(2_000) == 6


@patch.object(cpu_time, "read_thread_cpu_ns", return_value=1_000)
def test_elapsed_thread_cpu_us_clamps_negative_values(
    _mock_thread_cpu: object,
) -> None:
    """Guards metric distributions against an invalid negative sample."""
    assert cpu_time.elapsed_thread_cpu_us(2_000) == 0
