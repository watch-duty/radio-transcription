import unittest

from backend.pipeline.common.clients.session_helper import (
    create_resilient_session,
)


class TestSessionHelper(unittest.TestCase):
    def test_create_resilient_session_with_custom_max_retries(self) -> None:
        # Execute
        session = create_resilient_session(max_retries=5)

        # Verify
        adapter = session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(adapter.max_retries.total, 5)

    def test_create_resilient_session_with_zero_max_retries(self) -> None:
        # Execute
        session = create_resilient_session(max_retries=0)

        # Verify
        adapter = session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(
                getattr(adapter.max_retries, "total", adapter.max_retries), 0
            )

    def test_create_resilient_session_with_custom_config(self) -> None:
        # Execute
        session = create_resilient_session(
            max_retries=2,
            backoff_factor=1.0,
            status_forcelist=[500, 503],
            raise_on_status=True,
        )

        # Verify
        adapter = session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(adapter.max_retries.total, 2)
            self.assertEqual(adapter.max_retries.backoff_factor, 1.0)
            self.assertEqual(adapter.max_retries.status_forcelist, [500, 503])
            self.assertTrue(adapter.max_retries.raise_on_status)

    def test_create_resilient_session_with_allowed_methods(self) -> None:
        # Execute
        session = create_resilient_session(allowed_methods=None)

        # Verify
        adapter = session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertIsNone(adapter.max_retries.allowed_methods)


if __name__ == "__main__":
    unittest.main()
