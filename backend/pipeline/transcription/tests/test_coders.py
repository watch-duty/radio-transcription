"""Unit tests verifying deterministic encoding and decoding across custom Protobuf Coders."""

import unittest

from backend.pipeline.transcription.common import coders, datatypes


class CoderVerificationTest(unittest.TestCase):
    """Rigorously verifies Coder determinism, binary packing, and zero-loss unmarshaling."""

    def test_transmission_context_coder_idle_state(self) -> None:
        """Verifies that IdleFeedState instances are correctly encoded and decoded through oneof containers."""
        coder = coders.TransmissionContextCoder()
        idle = datatypes.IdleFeedState(
            order_timer_active=True,
            out_of_order_buffer=[
                datatypes.BufferedChunk(
                    timestamp_ms=1000, gcs_uri="gs://chunk1", traceparent="01"
                )
            ],
        )
        encoded = coder.encode(idle)
        self.assertIsInstance(encoded, bytes)
        self.assertTrue(coder.is_deterministic())

        decoded = coder.decode(encoded)
        self.assertIsInstance(decoded, datatypes.IdleFeedState)
        self.assertEqual(decoded.order_timer_active, True)
        self.assertEqual(len(decoded.out_of_order_buffer), 1)
        self.assertEqual(decoded.out_of_order_buffer[0].gcs_uri, "gs://chunk1")

    def test_transmission_context_coder_active_state(self) -> None:
        """Verifies full state payload preservation across ActiveStitchingState Coder boundaries."""
        coder = coders.TransmissionContextCoder()
        active = datatypes.ActiveStitchingState(
            session_id="mock-session-id",
            feed_metadata=datatypes.FeedMetadata(
                feed_name="Mock Feed", external_id="ext-123"
            ),
            last_end_time_ms=5000,
            stale_start_time_ms=1000,
            buffer_start_time_ms=1000,
            expected_next_chunk_start_ms=6000,
            start_audio_offset_ms=0,
            end_audio_offset_ms=5000,
            contributing_audio_uris=[
                "gs://bucket/1.flac",
                "gs://bucket/2.flac",
            ],
            missing_prior_context=False,
            missing_post_context=True,
            buffer_duration_ms=5000,
            order_timer_active=True,
            out_of_order_buffer=[],
            last_transmission_start_ms=1000,
            speech_segments=[datatypes.TimeRange(1000, 3000)],
            prior_audio_tail=b"raw_pcm_bytes_tail",
            traceparent="mock-traceparent-01",
            sample_rate=16000,
        )

        encoded = coder.encode(active)
        self.assertIsInstance(encoded, bytes)

        decoded = coder.decode(encoded)
        self.assertIsInstance(decoded, datatypes.ActiveStitchingState)
        self.assertEqual(decoded.session_id, "mock-session-id")
        self.assertEqual(decoded.feed_metadata.feed_name, "Mock Feed")
        self.assertEqual(decoded.last_end_time_ms, 5000)
        self.assertEqual(
            decoded.contributing_audio_uris,
            ["gs://bucket/1.flac", "gs://bucket/2.flac"],
        )
        self.assertEqual(decoded.missing_post_context, True)
        self.assertEqual(decoded.prior_audio_tail, b"raw_pcm_bytes_tail")
        self.assertEqual(decoded.traceparent, "mock-traceparent-01")

    def test_flush_request_coder(self) -> None:
        """Verifies exact binary serialization across FlushRequest Coder boundaries."""
        coder = coders.FlushRequestCoder()
        request = datatypes.FlushRequest(
            buffer=b"audio_pcm_buffer_bytes",
            feed_id="feed-123",
            session_id="session-123",
            contributing_audio_uris=["gs://bucket/audio.flac"],
            time_range=datatypes.TimeRange(0, 1000),
            transmission_id="tx-uuid-123",
            feed_metadata=datatypes.FeedMetadata("Feed", "Ext"),
            sample_rate=16000,
            missing_prior_context=False,
            missing_post_context=False,
            start_audio_offset_ms=10,
            end_audio_offset_ms=990,
            speech_segments=[datatypes.TimeRange(100, 900)],
            traceparent="traceparent-id",
        )

        encoded = coder.encode(request)
        self.assertTrue(coder.is_deterministic())

        decoded = coder.decode(encoded)
        self.assertIsInstance(decoded, datatypes.FlushRequest)
        self.assertEqual(decoded.buffer, b"audio_pcm_buffer_bytes")
        self.assertEqual(decoded.feed_id, "feed-123")
        self.assertEqual(decoded.start_audio_offset_ms, 10)
        self.assertEqual(len(decoded.speech_segments), 1)

    def test_chunk_metadata_coder(self) -> None:
        """Verifies ChunkMetadata binary encoding and decoding."""
        coder = coders.ChunkMetadataCoder()
        meta = datatypes.ChunkMetadata(
            gcs_uri="gs://bucket/chunk.flac",
            session_id="sess-01",
            duration_ms=1000,
            feed_metadata=datatypes.FeedMetadata("Name", "Ext"),
            traceparent="trace-01",
        )

        encoded = coder.encode(meta)
        self.assertTrue(coder.is_deterministic())

        decoded = coder.decode(encoded)
        self.assertIsInstance(decoded, datatypes.ChunkMetadata)
        self.assertEqual(decoded.gcs_uri, "gs://bucket/chunk.flac")
        self.assertEqual(decoded.duration_ms, 1000)
