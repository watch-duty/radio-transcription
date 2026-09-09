"""Helpers for scoped current-thread CPU measurements."""

import time

NANOSECONDS_PER_MICROSECOND = 1_000


def read_thread_cpu_ns() -> int:
    """Returns CPU time consumed by the current thread in nanoseconds."""
    return time.thread_time_ns()


def elapsed_thread_cpu_us(start_ns: int) -> int:
    """Returns nonnegative current-thread CPU elapsed since ``start_ns``."""
    elapsed_ns = read_thread_cpu_ns() - start_ns
    return max(0, elapsed_ns // NANOSECONDS_PER_MICROSECOND)
