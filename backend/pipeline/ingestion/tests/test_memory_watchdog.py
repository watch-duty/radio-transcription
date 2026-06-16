"""Unit tests for the extracted memory watchdog."""

from __future__ import annotations

import os
import threading
import unittest
from unittest import mock

from backend.pipeline.ingestion import memory_watchdog


def _make_settings(**overrides: object) -> mock.MagicMock:
    """Build settings with real watchdog values."""
    defaults: dict[str, object] = {
        "rss_watchdog_poll_interval_sec": 0.05,
        "rss_watchdog_pause_threshold": 0.70,
        "rss_watchdog_exit_threshold": 0.90,
        "rss_watchdog_pause_consecutive_samples": 3,
        "rss_watchdog_exit_consecutive_samples": 3,
        "rss_watchdog_warmup_sec": 0.0,
    }
    defaults.update(overrides)
    settings = mock.MagicMock()
    settings.configure_mock(**defaults)
    return settings


def _sequenced_stop(sample_count: int) -> mock.MagicMock:
    """Return a stop-event mock that lets exactly sample_count polls run."""
    thread_stop = mock.MagicMock()
    thread_stop.is_set.side_effect = [False] * sample_count + [True]
    thread_stop.wait.side_effect = [False] * sample_count
    return thread_stop


class TestMemoryWatchdogStart(unittest.IsolatedAsyncioTestCase):
    """Tests for watchdog lifecycle start and join behavior."""

    async def test_no_cgroup_limit_disables_without_thread(self) -> None:
        """A missing cgroup limit leaves the watchdog unpaused and join no-ops."""
        limit_reader = mock.Mock(return_value=None)
        watchdog = memory_watchdog.MemoryWatchdog(
            _make_settings(),
            threading.Event(),
            limit_reader=limit_reader,
        )

        watchdog.start(mock.MagicMock(), mock.MagicMock())
        await watchdog.join(timeout_sec=0.01)

        limit_reader.assert_called_once_with()
        self.assertFalse(watchdog.is_paused())

    async def test_second_start_raises_runtime_error(self) -> None:
        """start() is single-use so duplicate watchdog threads cannot appear."""
        watchdog = memory_watchdog.MemoryWatchdog(
            _make_settings(),
            threading.Event(),
            limit_reader=mock.Mock(return_value=None),
        )

        watchdog.start(mock.MagicMock(), mock.MagicMock())

        with self.assertRaises(RuntimeError):
            watchdog.start(mock.MagicMock(), mock.MagicMock())


class TestMemoryWatchdogDebounce(unittest.TestCase):
    """Pause-set debounce and resume hysteresis semantics."""

    def _drive_samples(
        self,
        usage_samples: list[int],
        *,
        limit_bytes: int = 1000,
    ) -> memory_watchdog.MemoryWatchdog:
        thread_stop = _sequenced_stop(len(usage_samples))
        watchdog = memory_watchdog.MemoryWatchdog(
            _make_settings(),
            thread_stop,
            usage_reader=mock.Mock(side_effect=usage_samples),
        )

        watchdog._run(limit_bytes, mock.MagicMock(), mock.MagicMock())

        return watchdog

    def test_two_high_then_one_low_does_not_pause(self) -> None:
        """Two samples at 70% then one at 65% resets the counter."""
        watchdog = self._drive_samples([700, 700, 650])

        self.assertFalse(watchdog.is_paused())

    def test_three_high_samples_pause_claims(self) -> None:
        """Three consecutive samples at 70% pause claims."""
        watchdog = self._drive_samples([700, 700, 700])

        self.assertTrue(watchdog.is_paused())

    def test_three_high_then_inside_band_stays_paused(self) -> None:
        """A sample inside the hysteresis band keeps claims paused."""
        watchdog = self._drive_samples([700, 700, 700, 650])

        self.assertTrue(watchdog.is_paused())

    def test_three_high_then_below_floor_resumes(self) -> None:
        """A sample below the 10pp hysteresis floor resumes claims."""
        watchdog = self._drive_samples([700, 700, 700, 590])

        self.assertFalse(watchdog.is_paused())


class TestMemoryWatchdogExitSemantics(unittest.TestCase):
    """Exit-trip semantics for the watchdog OS thread."""

    def _drive_samples_with_loop(
        self,
        usage_samples: list[int],
        *,
        limit_bytes: int = 1000,
    ) -> tuple[mock.MagicMock, mock.MagicMock, mock.MagicMock]:
        thread_stop = _sequenced_stop(len(usage_samples))
        loop = mock.MagicMock()
        shutdown = mock.MagicMock()
        watchdog = memory_watchdog.MemoryWatchdog(
            _make_settings(),
            thread_stop,
            usage_reader=mock.Mock(side_effect=usage_samples),
        )

        with mock.patch.object(os, "_exit") as mock_exit:
            watchdog._run(limit_bytes, loop, shutdown)

        return loop.call_soon_threadsafe, thread_stop.set, mock_exit

    def test_two_at_exit_then_one_low_does_not_trip(self) -> None:
        """Two samples at 95% then one at 70% does not trip shutdown."""
        call_soon_threadsafe, thread_stop_set, mock_exit = (
            self._drive_samples_with_loop([950, 950, 700])
        )

        call_soon_threadsafe.assert_not_called()
        thread_stop_set.assert_not_called()
        mock_exit.assert_not_called()

    def test_three_at_exit_threshold_trips_graceful_shutdown(self) -> None:
        """Three samples at 95% schedules shutdown and stops threads."""
        call_soon_threadsafe, thread_stop_set, mock_exit = (
            self._drive_samples_with_loop([950, 950, 950])
        )

        call_soon_threadsafe.assert_called_once()
        thread_stop_set.assert_called_once()
        mock_exit.assert_not_called()


class TestMemoryWatchdogWarmupGrace(unittest.TestCase):
    """Warmup grace semantics."""

    def _drive_samples_with_time(
        self,
        usage_samples: list[int],
        time_sequence: list[float],
    ) -> memory_watchdog.MemoryWatchdog:
        thread_stop = _sequenced_stop(len(usage_samples))
        watchdog = memory_watchdog.MemoryWatchdog(
            _make_settings(rss_watchdog_warmup_sec=60.0),
            thread_stop,
            usage_reader=mock.Mock(side_effect=usage_samples),
            monotonic=mock.Mock(side_effect=time_sequence),
        )

        watchdog._run(1000, mock.MagicMock(), mock.MagicMock())

        return watchdog

    def test_sample_during_warmup_does_not_increment(self) -> None:
        """A high sample during warmup does not pause claims."""
        watchdog = self._drive_samples_with_time([800], [0.0, 30.0])

        self.assertFalse(watchdog.is_paused())

    def test_sample_after_warmup_increments(self) -> None:
        """Three high post-warmup samples pause claims."""
        watchdog = self._drive_samples_with_time(
            [800, 800, 800],
            [0.0, 61.0, 62.0, 63.0],
        )

        self.assertTrue(watchdog.is_paused())


class TestMemoryWatchdogReadErrors(unittest.TestCase):
    """Usage-read error handling."""

    def test_usage_read_failure_defensively_pauses(self) -> None:
        """A failed cgroup usage read pauses claims without tripping shutdown."""
        thread_stop = _sequenced_stop(1)
        loop = mock.MagicMock()
        shutdown = mock.MagicMock()
        watchdog = memory_watchdog.MemoryWatchdog(
            _make_settings(),
            thread_stop,
            usage_reader=mock.Mock(return_value=None),
        )

        watchdog._run(1000, loop, shutdown)

        self.assertTrue(watchdog.is_paused())
        loop.call_soon_threadsafe.assert_not_called()
        thread_stop.set.assert_not_called()


if __name__ == "__main__":
    unittest.main()
