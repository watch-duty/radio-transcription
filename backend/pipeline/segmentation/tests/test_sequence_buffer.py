import unittest

from backend.pipeline.segmentation.datatypes import OrderRestorerConfig
from backend.pipeline.segmentation.state.sequence_buffer import (
    BufferedChunk,
    SequenceBuffer,
)


class TestSequenceBuffer(unittest.TestCase):
    """Test suite for the framework-agnostic SequenceBuffer logic."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.config = OrderRestorerConfig(
            out_of_order_timeout_ms=5000,
            chunk_duration_ms=3000,
        )
        self.buffer = SequenceBuffer(self.config)

    def test_initial_chunk(self) -> None:
        """Verifies that the first chunk processed establishes the baseline expected Next timestamp without being buffered or flagged as late."""
        (
            expected_next_ts,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            current_ts_ms=1000,
            gcs_uri="gs://chunk1",
            expected_next_ts=None,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_ts, 4000)  # 1000 + 3000
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit, [BufferedChunk(timestamp_ms=1000, gcs_uri="gs://chunk1")]
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_perfect_sequence(self) -> None:
        """Verifies that subsequent chunks arriving cleanly with a timestamp matching the expected Next timestamp are emitted immediately without buffering."""
        (
            expected_next_ts,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            current_ts_ms=4000,
            gcs_uri="gs://chunk2",
            expected_next_ts=4000,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_ts, 7000)  # 4000 + 3000
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit, [BufferedChunk(timestamp_ms=4000, gcs_uri="gs://chunk2")]
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_future_chunk_is_buffered(self) -> None:
        """Verifies that chunks arriving with a timestamp greater than the expected Next timestamp are withheld in the buffer instead of being sequentially emitted."""
        (
            expected_next_ts,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            current_ts_ms=7000,
            gcs_uri="gs://chunk3",
            expected_next_ts=4000,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_ts, 4000)  # Unchanged
        self.assertEqual(len(buffered), 1)
        self.assertEqual(buffered[0].gcs_uri, "gs://chunk3")
        self.assertEqual(to_emit, [])
        self.assertFalse(was_late)
        self.assertTrue(was_buffered)

    def test_drain_ready_elements(self) -> None:
        """Verifies that processing a matching chunk recursively drains all contingently sequential elements currently held in the buffer."""
        initial_buffer = [
            BufferedChunk(timestamp_ms=7000, gcs_uri="gs://chunk3"),
            BufferedChunk(timestamp_ms=10000, gcs_uri="gs://chunk4"),
            BufferedChunk(timestamp_ms=16000, gcs_uri="gs://chunk6"),
        ]

        (
            expected_next_ts,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            current_ts_ms=4000,
            gcs_uri="gs://chunk2",
            expected_next_ts=4000,
            buffer_elements=initial_buffer,
        )

        self.assertEqual(
            expected_next_ts, 13000
        )  # Passed 4k -> 7k -> 10k -> outputs 13k
        self.assertEqual(len(buffered), 1)
        self.assertEqual(
            buffered[0].gcs_uri, "gs://chunk6"
        )  # Chunk 6 is still stranded awaiting Chunk 5
        self.assertEqual(
            to_emit,
            [
                BufferedChunk(timestamp_ms=4000, gcs_uri="gs://chunk2"),
                BufferedChunk(timestamp_ms=7000, gcs_uri="gs://chunk3"),
                BufferedChunk(timestamp_ms=10000, gcs_uri="gs://chunk4"),
            ],
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_late_chunk(self) -> None:
        """Verifies that a chunk arriving before the chronological expected Next timestamp is yielded individually and explicitly flagged as late."""
        (
            expected_next_ts,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            current_ts_ms=1000,  # Far in the past
            gcs_uri="gs://chunk-late",
            expected_next_ts=10000,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_ts, 10000)  # Unchanged
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit,
            [BufferedChunk(timestamp_ms=1000, gcs_uri="gs://chunk-late")],
        )  # Emitted for isolated rendering
        self.assertTrue(was_late)
        self.assertFalse(was_buffered)

    def test_epsilon_tolerance(self) -> None:
        """Verifies that floating-point truncation variances (e.g., within a 10ms epsilon delta) do not cause a chunk to be wrongly identified as late or out-of-order."""
        (
            expected_next_ts,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            current_ts_ms=3990,  # 10ms short of the expected 4000
            gcs_uri="gs://chunk-tolerance",
            expected_next_ts=4000,
            buffer_elements=[],
        )

        # The expected_next_ts mathematically adheres strictly to its own progression using chunk duration
        # regardless of the actual float-truncated timestamp of the accepted chunk!
        self.assertEqual(expected_next_ts, 6990)
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit,
            [BufferedChunk(timestamp_ms=3990, gcs_uri="gs://chunk-tolerance")],
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_max_emit_clamp(self) -> None:
        """Verifies that drain_ready_elements respects the max_emit clamp, leaving the rest in the buffer."""
        initial_buffer = [
            BufferedChunk(timestamp_ms=7000, gcs_uri="gs://chunk3"),
            BufferedChunk(timestamp_ms=10000, gcs_uri="gs://chunk4"),
            BufferedChunk(timestamp_ms=13000, gcs_uri="gs://chunk5"),
            BufferedChunk(timestamp_ms=16000, gcs_uri="gs://chunk6"),
        ]

        # Use the underlying drain_ready_elements directly to test the clamp
        (
            new_expected,
            new_buffer,
            to_emit,
        ) = self.buffer.drain_ready_elements(
            expected_next_ts=7000,
            buffer_elements=initial_buffer,
            epsilon_ms=10,
            max_emit=2,
        )

        self.assertEqual(len(to_emit), 2)
        self.assertEqual(to_emit[0].gcs_uri, "gs://chunk3")
        self.assertEqual(to_emit[1].gcs_uri, "gs://chunk4")

        self.assertEqual(new_expected, 13000)
        self.assertEqual(len(new_buffer), 2)
        self.assertEqual(new_buffer[0].gcs_uri, "gs://chunk5")
        self.assertEqual(new_buffer[1].gcs_uri, "gs://chunk6")
