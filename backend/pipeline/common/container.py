import logging
import os
import threading

logger = logging.getLogger(__name__)


class ForkAwareContainer:
    """Base class for service containers that detects process forks and resets clients safely."""

    def __init__(self) -> None:
        self._pid = os.getpid()
        self._lock = threading.Lock()

    def check_fork(self) -> None:
        """Checks if the current process has been forked from the parent process.

        Uses a double-checked locking pattern to ensure thread safety when resetting clients.
        """
        current_pid = os.getpid()
        if self._pid != current_pid:
            with self._lock:
                if self._pid != current_pid:
                    logger.info(
                        "Fork detected (parent PID: %s, child PID: %s). "
                        "Resetting container clients.",
                        self._pid,
                        current_pid,
                    )
                    self._pid = current_pid
                    self.reset_clients()

    def reset_clients(self) -> None:
        """Resets all cached client/processor instances in the subclass.

        Must be implemented by subclasses.
        """
        msg = "Subclasses must implement reset_clients"
        raise NotImplementedError(msg)
