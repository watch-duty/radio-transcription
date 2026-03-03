from __future__ import annotations

import asyncio
import os
import signal
import unittest
from unittest import mock

from backend.pipeline.ingestion.shutdown import setup_shutdown_signals

# Small delay that guarantees the event loop polls I/O and dispatches
# any pending signal callbacks.  Same pattern used in test_heartbeat.py.
_SIGNAL_SETTLE = 0.05


class TestSetupShutdownSignals(unittest.IsolatedAsyncioTestCase):
    """Tests for the graceful-shutdown signal handler."""

    async def test_returns_unset_event(self) -> None:
        """The returned event must be an asyncio.Event that is initially unset."""
        loop = asyncio.get_running_loop()
        event = setup_shutdown_signals(loop)

        self.assertIsInstance(event, asyncio.Event)
        self.assertFalse(event.is_set())

    async def test_sigterm_sets_event(self) -> None:
        """Sending SIGTERM to the process must set the shutdown event."""
        loop = asyncio.get_running_loop()
        event = setup_shutdown_signals(loop)

        os.kill(os.getpid(), signal.SIGTERM)
        await asyncio.sleep(_SIGNAL_SETTLE)

        self.assertTrue(event.is_set())

    async def test_sigint_sets_event(self) -> None:
        """Sending SIGINT to the process must set the shutdown event."""
        loop = asyncio.get_running_loop()
        event = setup_shutdown_signals(loop)

        os.kill(os.getpid(), signal.SIGINT)
        await asyncio.sleep(_SIGNAL_SETTLE)

        self.assertTrue(event.is_set())

    async def test_idempotent_ignores_second_signal(self) -> None:
        """A second signal must be a no-op (event already set, no duplicate log)."""
        loop = asyncio.get_running_loop()

        with mock.patch("backend.pipeline.ingestion.shutdown.logger") as mock_logger:
            event = setup_shutdown_signals(loop)

            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.sleep(_SIGNAL_SETTLE)
            os.kill(os.getpid(), signal.SIGTERM)
            await asyncio.sleep(_SIGNAL_SETTLE)

        self.assertTrue(event.is_set())
        self.assertEqual(mock_logger.info.call_count, 1)

    async def test_both_handlers_registered(self) -> None:
        """Both SIGTERM and SIGINT must be registered via loop.add_signal_handler."""
        loop = asyncio.get_running_loop()

        with mock.patch.object(
            loop,
            "add_signal_handler",
            wraps=loop.add_signal_handler,
        ) as spy:
            setup_shutdown_signals(loop)

        registered_signals = {call.args[0] for call in spy.call_args_list}
        self.assertIn(signal.SIGTERM, registered_signals)
        self.assertIn(signal.SIGINT, registered_signals)


if __name__ == "__main__":
    unittest.main()
