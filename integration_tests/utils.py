"""Common testing utilities for integration tests."""

import time
from collections.abc import Callable

import pytest


def assert_eventually(
    condition_func: Callable[[], bool],
    timeout_sec: float = 5.0,
    error_msg: str = "Condition not met within timeout",
) -> None:
    """Repeatedly polls a condition function until it returns True."""
    end_time = time.time() + timeout_sec
    while time.time() < end_time:
        if condition_func():
            return
        time.sleep(0.5)
    pytest.fail(error_msg)
