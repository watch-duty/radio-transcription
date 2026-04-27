import unittest

from backend.pipeline.transcription.common.datatypes import OrderRestorerConfig
from backend.pipeline.transcription.state.sequence_buffer import (
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
            expected_next_seq,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            sequence_number=1,
            current_ts_ms=1000,
            gcs_uri="gs://chunk1",
            expected_next_seq=None,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_seq, 2)
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit,
            [
                BufferedChunk(
                    sequence_number=1, timestamp_ms=1000, gcs_uri="gs://chunk1"
                )
            ],
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_perfect_sequence(self) -> None:
        """Verifies that subsequent chunks arriving cleanly with a timestamp matching the expected Next timestamp are emitted immediately without buffering."""
        (
            expected_next_seq,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            sequence_number=2,
            current_ts_ms=4000,
            gcs_uri="gs://chunk2",
            expected_next_seq=2,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_seq, 3)
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit,
            [
                BufferedChunk(
                    sequence_number=2, timestamp_ms=4000, gcs_uri="gs://chunk2"
                )
            ],
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_future_chunk_is_buffered(self) -> None:
        """Verifies that chunks arriving with a timestamp greater than the expected Next timestamp are withheld in the buffer instead of being sequentially emitted."""
        (
            expected_next_seq,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            sequence_number=3,
            current_ts_ms=7000,
            gcs_uri="gs://chunk3",
            expected_next_seq=2,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_seq, 2)  # Unchanged
        self.assertEqual(len(buffered), 1)
        self.assertEqual(buffered[0].gcs_uri, "gs://chunk3")
        self.assertEqual(to_emit, [])
        self.assertFalse(was_late)
        self.assertTrue(was_buffered)

    def test_drain_ready_elements(self) -> None:
        """Verifies that processing a matching chunk recursively drains all contingently sequential elements currently held in the buffer."""
        initial_buffer = [
            BufferedChunk(
                sequence_number=3, timestamp_ms=7000, gcs_uri="gs://chunk3"
            ),
            BufferedChunk(
                sequence_number=4, timestamp_ms=10000, gcs_uri="gs://chunk4"
            ),
            BufferedChunk(
                sequence_number=6, timestamp_ms=16000, gcs_uri="gs://chunk6"
            ),
        ]

        (
            expected_next_seq,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            sequence_number=2,
            current_ts_ms=4000,
            gcs_uri="gs://chunk2",
            expected_next_seq=2,
            buffer_elements=initial_buffer,
        )

        self.assertEqual(
            expected_next_seq, 5
        )  # Passed 2 -> 3 -> 4 -> outputs 5
        self.assertEqual(len(buffered), 1)
        self.assertEqual(buffered[0].gcs_uri, "gs://chunk6")
        self.assertEqual(
            to_emit,
            [
                BufferedChunk(
                    sequence_number=2, timestamp_ms=4000, gcs_uri="gs://chunk2"
                ),
                BufferedChunk(
                    sequence_number=3, timestamp_ms=7000, gcs_uri="gs://chunk3"
                ),
                BufferedChunk(
                    sequence_number=4, timestamp_ms=10000, gcs_uri="gs://chunk4"
                ),
            ],
        )
        self.assertFalse(was_late)
        self.assertFalse(was_buffered)

    def test_late_chunk(self) -> None:
        """Verifies that a chunk arriving before the chronological expected Next timestamp is yielded individually and explicitly flagged as late."""
        (
            expected_next_seq,
            buffered,
            to_emit,
            was_late,
            was_buffered,
        ) = self.buffer.process_chunk(
            sequence_number=1,
            current_ts_ms=1000,
            gcs_uri="gs://chunk-late",
            expected_next_seq=10,
            buffer_elements=[],
        )

        self.assertEqual(expected_next_seq, 10)  # Unchanged
        self.assertEqual(buffered, [])
        self.assertEqual(
            to_emit,
            [
                BufferedChunk(
                    sequence_number=1,
                    timestamp_ms=1000,
                    gcs_uri="gs://chunk-late",
                )
            ],
        )
        self.assertTrue(was_late)
        self.assertFalse(was_buffered)
