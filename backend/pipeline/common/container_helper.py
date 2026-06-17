import logging
import os
import threading
from collections.abc import Callable

logger = logging.getLogger(__name__)


class ForkDetector:
    """Helper that tracks process forks and executes a callback when one is detected."""

    def __init__(self, on_fork: Callable[[], None]) -> None:
        self._pid = os.getpid()
        self._lock = threading.Lock()
        self._on_fork = on_fork

    def check_fork(self) -> None:
        """Checks if the current process has been forked from the parent process.

        Uses a double-checked locking pattern to ensure thread safety when running on_fork.
        """
        current_pid = os.getpid()
        if self._pid != current_pid:
            with self._lock:
                if self._pid != current_pid:
                    logger.info(
                        "Fork detected (parent PID: %s, child PID: %s). "
                        "Executing fork reset callback.",
                        self._pid,
                        current_pid,
                    )
                    self._pid = current_pid
                    self._on_fork()
