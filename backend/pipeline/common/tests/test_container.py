import threading
import unittest
from unittest.mock import patch

from backend.pipeline.common.container import ForkAwareContainer


class DummyContainer(ForkAwareContainer):
    def __init__(self) -> None:
        super().__init__()
        self.reset_called_count = 0

    def reset_clients(self) -> None:
        self.reset_called_count += 1


class TestForkAwareContainer(unittest.TestCase):
    def test_no_fork_does_not_reset(self) -> None:
        container = DummyContainer()
        self.assertEqual(container.reset_called_count, 0)

        container.check_fork()
        self.assertEqual(container.reset_called_count, 0)

    def test_fork_resets_clients(self) -> None:
        container = DummyContainer()
        self.assertEqual(container.reset_called_count, 0)
        original_pid = container._pid

        # Simulate fork by mocking os.getpid to return a different PID
        with patch("os.getpid") as mock_getpid:
            new_pid = original_pid + 1
            mock_getpid.return_value = new_pid

            container.check_fork()
            self.assertEqual(container.reset_called_count, 1)
            self.assertEqual(container._pid, new_pid)

            # Call again, should not reset again since container._pid is updated
            container.check_fork()
            self.assertEqual(container.reset_called_count, 1)

    def test_thread_safety_lock_exists(self) -> None:
        container = DummyContainer()
        self.assertIsInstance(container._lock, type(threading.Lock()))


if __name__ == "__main__":
    unittest.main()
