from __future__ import annotations

import unittest
import uuid
from unittest import mock

from backend.pipeline.storage.feed_store import FeedStore, LeasedFeed


def _make_cursor(
    *,
    fetchone_result: dict | tuple | None = None,
    rowcount: int = 0,
) -> mock.MagicMock:
    """Create a mock cursor with the given fetch/rowcount behaviour."""
    cursor = mock.MagicMock()
    cursor.fetchone.return_value = fetchone_result
    cursor.rowcount = rowcount
    return cursor


def _make_conn(cursor: mock.MagicMock) -> mock.MagicMock:
    """Create a mock connection whose ``cursor()`` returns *cursor*."""
    conn = mock.MagicMock()
    cursor.__enter__ = mock.Mock(return_value=cursor)
    cursor.__exit__ = mock.Mock(return_value=False)
    conn.cursor.return_value = cursor
    return conn


_FEED_ID = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
_FEED_ID_B = uuid.UUID("bbbbbbbb-cccc-dddd-eeee-ffffffffffff")
_WORKER_ID = uuid.UUID("11111111-2222-3333-4444-555555555555")

_LEASE_ROW: dict = {
    "id": _FEED_ID,
    "name": "My Feed",
    "source_type": "bcfy_feeds",
    "last_processed_filename": None,
    "stream_url": "http://stream.example.com/feed",
}


class TestLeaseFeed(unittest.TestCase):
    """Tests for FeedStore.lease_feed."""

    def test_returns_feed_when_available(self) -> None:
        """A leased feed is returned as a LeasedFeed dict."""
        cursor = _make_cursor(fetchone_result=_LEASE_ROW)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.lease_feed(_WORKER_ID)

        expected: LeasedFeed = {
            "id": _FEED_ID,
            "name": "My Feed",
            "source_type": "bcfy_feeds",
            "last_processed_filename": None,
            "stream_url": "http://stream.example.com/feed",
        }
        self.assertEqual(result, expected)

    def test_returns_none_when_no_feed_available(self) -> None:
        """None is returned when no feed can be leased."""
        cursor = _make_cursor(fetchone_result=None)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.lease_feed(_WORKER_ID)

        self.assertIsNone(result)

    def test_passes_worker_id_as_parameter(self) -> None:
        """The worker_id is passed as a string parameter to the query."""
        cursor = _make_cursor(fetchone_result=None)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.lease_feed(_WORKER_ID)

        args = cursor.execute.call_args
        self.assertEqual(args[0][1], (_WORKER_ID,))

    def test_commits_on_success(self) -> None:
        """The transaction is committed after a successful lease."""
        cursor = _make_cursor(fetchone_result=None)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.lease_feed(_WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__enter__.assert_called_once()
        tx_mock.__exit__.assert_called_once()

    def test_rolls_back_and_reraises_on_error(self) -> None:
        """The transaction is rolled back and the exception re-raised on failure."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.lease_feed(_WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__exit__.assert_called_once()
        args = tx_mock.__exit__.call_args[0]
        self.assertEqual(args[0], RuntimeError)

    def test_cursor_is_closed(self) -> None:
        """The cursor is always closed, even on error."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.lease_feed(_WORKER_ID)

        cursor.__exit__.assert_called_once()


class TestUpdateFeedProgress(unittest.TestCase):
    """Tests for FeedStore.update_feed_progress."""

    def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the fenced update succeeds."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.update_feed_progress(
            _FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg"
        )

        self.assertTrue(result)

    def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when no row matches (lease was lost)."""
        cursor = _make_cursor(rowcount=0)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.update_feed_progress(
            _FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg"
        )

        self.assertFalse(result)

    def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)
        gcs_path = "gs://bucket/path/file.ogg"

        store.update_feed_progress(_FEED_ID, _WORKER_ID, gcs_path)

        args = cursor.execute.call_args
        self.assertEqual(args[0][1], (gcs_path, _FEED_ID, _WORKER_ID))

    def test_commits_on_success(self) -> None:
        """The transaction is committed after a successful update."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.update_feed_progress(_FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg")

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__enter__.assert_called_once()
        tx_mock.__exit__.assert_called_once()

    def test_rolls_back_and_reraises_on_error(self) -> None:
        """The transaction is rolled back and the exception re-raised on failure."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.update_feed_progress(
                _FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg"
            )

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__exit__.assert_called_once()
        args = tx_mock.__exit__.call_args[0]
        self.assertEqual(args[0], RuntimeError)

    def test_cursor_is_closed(self) -> None:
        """The cursor is always closed, even on error."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.update_feed_progress(
                _FEED_ID, _WORKER_ID, "gs://bucket/path/file.ogg"
            )

        cursor.__exit__.assert_called_once()


class TestRenewHeartbeat(unittest.TestCase):
    """Tests for FeedStore.renew_heartbeat."""

    def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the heartbeat was renewed."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        self.assertTrue(result)

    def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was lost (fence violation)."""
        cursor = _make_cursor(rowcount=0)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        self.assertFalse(result)

    def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        args = cursor.execute.call_args
        self.assertEqual(args[0][1], (_FEED_ID, _WORKER_ID))

    def test_commits_on_success(self) -> None:
        """The transaction is committed after a successful renewal."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__enter__.assert_called_once()
        tx_mock.__exit__.assert_called_once()

    def test_rolls_back_and_reraises_on_error(self) -> None:
        """The transaction is rolled back and the exception re-raised on failure."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__exit__.assert_called_once()
        args = tx_mock.__exit__.call_args[0]
        self.assertEqual(args[0], RuntimeError)

    def test_cursor_is_closed(self) -> None:
        """The cursor is always closed, even on error."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.renew_heartbeat(_FEED_ID, _WORKER_ID)

        cursor.__exit__.assert_called_once()


class TestRenewHeartbeatsBatch(unittest.TestCase):
    """Tests for FeedStore.renew_heartbeats_batch."""

    def test_returns_renewed_ids(self) -> None:
        """Returned set contains the IDs from RETURNING rows."""
        cursor = _make_cursor()
        cursor.fetchall.return_value = [(_FEED_ID,), (_FEED_ID_B,)]
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.renew_heartbeats_batch([_FEED_ID, _FEED_ID_B], _WORKER_ID)

        self.assertEqual(result, {_FEED_ID, _FEED_ID_B})

    def test_returns_empty_set_for_no_matches(self) -> None:
        """Empty set is returned when no rows match (all leases lost)."""
        cursor = _make_cursor()
        cursor.fetchall.return_value = []
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.renew_heartbeats_batch([_FEED_ID], _WORKER_ID)

        self.assertEqual(result, set())

    def test_short_circuits_on_empty_input(self) -> None:
        """Empty feed_ids list returns empty set without executing a query."""
        conn = mock.MagicMock()
        store = FeedStore(conn)

        result = store.renew_heartbeats_batch([], _WORKER_ID)

        self.assertEqual(result, set())
        conn.transaction.assert_not_called()

    def test_passes_correct_parameters(self) -> None:
        """Parameters are passed as (feed_ids_list, worker_id)."""
        cursor = _make_cursor()
        cursor.fetchall.return_value = [(_FEED_ID,)]
        conn = _make_conn(cursor)
        store = FeedStore(conn)
        feed_ids = [_FEED_ID, _FEED_ID_B]

        store.renew_heartbeats_batch(feed_ids, _WORKER_ID)

        args = cursor.execute.call_args
        self.assertEqual(args[0][1], (feed_ids, _WORKER_ID))

    def test_commits_on_success(self) -> None:
        """The transaction is committed after a successful batch renewal."""
        cursor = _make_cursor()
        cursor.fetchall.return_value = [(_FEED_ID,)]
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.renew_heartbeats_batch([_FEED_ID], _WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__enter__.assert_called_once()
        tx_mock.__exit__.assert_called_once()

    def test_cursor_is_closed(self) -> None:
        """The cursor is always closed, even on error."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.renew_heartbeats_batch([_FEED_ID], _WORKER_ID)

        cursor.__exit__.assert_called_once()


class TestReportFeedFailure(unittest.TestCase):
    """Tests for FeedStore.report_feed_failure."""

    def test_returns_true_when_lease_held(self) -> None:
        """True is returned when the failure was recorded."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.report_feed_failure(_FEED_ID, _WORKER_ID)

        self.assertTrue(result)

    def test_returns_false_when_lease_lost(self) -> None:
        """False is returned when the lease was already lost."""
        cursor = _make_cursor(rowcount=0)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        result = store.report_feed_failure(_FEED_ID, _WORKER_ID)

        self.assertFalse(result)

    def test_passes_correct_parameters(self) -> None:
        """Parameters are passed in the correct order."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.report_feed_failure(_FEED_ID, _WORKER_ID)

        args = cursor.execute.call_args
        self.assertEqual(args[0][1], (_FEED_ID, _WORKER_ID))

    def test_commits_on_success(self) -> None:
        """The transaction is committed after recording the failure."""
        cursor = _make_cursor(rowcount=1)
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        store.report_feed_failure(_FEED_ID, _WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__enter__.assert_called_once()
        tx_mock.__exit__.assert_called_once()

    def test_rolls_back_and_reraises_on_error(self) -> None:
        """The transaction is rolled back and the exception re-raised on failure."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.report_feed_failure(_FEED_ID, _WORKER_ID)

        conn.transaction.assert_called_once()
        tx_mock = conn.transaction.return_value
        tx_mock.__exit__.assert_called_once()
        args = tx_mock.__exit__.call_args[0]
        self.assertEqual(args[0], RuntimeError)

    def test_cursor_is_closed(self) -> None:
        """The cursor is always closed, even on error."""
        cursor = _make_cursor()
        cursor.execute.side_effect = RuntimeError("db error")
        conn = _make_conn(cursor)
        store = FeedStore(conn)

        with self.assertRaises(RuntimeError):
            store.report_feed_failure(_FEED_ID, _WORKER_ID)

        cursor.__exit__.assert_called_once()


if __name__ == "__main__":
    unittest.main()
