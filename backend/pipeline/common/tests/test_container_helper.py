import threading
import unittest
from unittest.mock import patch

from backend.pipeline.common.container_helper import ForkDetector


class TestForkDetector(unittest.TestCase):
    def setUp(self) -> None:
        self.reset_called_count = 0

    def on_fork(self) -> None:
        self.reset_called_count += 1

    def test_no_fork_does_not_trigger_callback(self) -> None:
        detector = ForkDetector(self.on_fork)
        self.assertEqual(self.reset_called_count, 0)

        detector.check_fork()
        self.assertEqual(self.reset_called_count, 0)

    def test_fork_triggers_callback(self) -> None:
        detector = ForkDetector(self.on_fork)
        self.assertEqual(self.reset_called_count, 0)
        original_pid = detector._pid

        # Simulate fork by mocking os.getpid to return a different PID
        with patch("os.getpid") as mock_getpid:
            new_pid = original_pid + 1
            mock_getpid.return_value = new_pid

            detector.check_fork()
            self.assertEqual(self.reset_called_count, 1)
            self.assertEqual(detector._pid, new_pid)

            # Call again, should not trigger again since detector._pid is updated
            detector.check_fork()
            self.assertEqual(self.reset_called_count, 1)

    def test_thread_safety_lock_exists(self) -> None:
        detector = ForkDetector(self.on_fork)
        self.assertIsInstance(detector._lock, type(threading.Lock()))


if __name__ == "__main__":
    unittest.main()
