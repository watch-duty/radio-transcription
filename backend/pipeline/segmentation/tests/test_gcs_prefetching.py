import concurrent.futures
import logging
import threading
import unittest

import numpy as np

from backend.pipeline.common import tracing_utils
from backend.pipeline.segmentation.datatypes import (
    AudioSignal,
    BufferedChunk,
    OrderRestorerConfig,
)
from backend.pipeline.segmentation.tests.test_stitcher_state import (
    get_test_stitch_config,
)
from backend.pipeline.segmentation.transforms.stitcher_engine import (
    StitcherEngine,
)


class GcsPrefetchingTest(unittest.TestCase):
    """Unit tests for concurrent GCS I/O pre-fetching in StitcherEngine adhering to concurrency_patterns.md."""

    def setUp(self) -> None:
        self.stitch_config = get_test_stitch_config()
        self.order_config = OrderRestorerConfig(out_of_order_timeout_ms=1000)
        self.engine = StitcherEngine(
            stitch_config=self.stitch_config,
            order_config=self.order_config,
        )
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        self.engine.executor = self.executor
        self.logger = logging.getLogger("test_prefetching")

    def tearDown(self) -> None:
        self.executor.shutdown(wait=True)

    def test_prefetch_audio_futures_returns_none_for_single_chunk(self) -> None:
        """Verifies that for <= 1 chunk, pre-fetching falls back to sequential execution to avoid thread overhead."""
        chunks = [
            BufferedChunk(gcs_uri="gs://bucket/1.flac", timestamp_ms=1000)
        ]
        futures_map = self.engine.prefetch_audio_futures(chunks, self.logger)
        self.assertIsNone(futures_map)

    def test_prefetch_audio_futures_executes_in_background_threads(
        self,
    ) -> None:
        """Verifies that for > 1 chunk, audio fetching executes in background threads (not main thread)."""
        execution_threads = set()
        main_thread_name = threading.current_thread().name

        def mock_fetch_and_decode(
            uri: str, trace_attrs: dict[str, str] | None = None
        ) -> AudioSignal:
            execution_threads.add(threading.current_thread().name)
            return AudioSignal(
                samples=np.zeros(1600, dtype=np.int16), sample_rate=16000
            )

        self.engine.processor.fetch_and_decode_audio = mock_fetch_and_decode  # type: ignore

        chunks = [
            BufferedChunk(gcs_uri="gs://bucket/1.flac", timestamp_ms=1000),
            BufferedChunk(gcs_uri="gs://bucket/2.flac", timestamp_ms=2000),
            BufferedChunk(gcs_uri="gs://bucket/3.flac", timestamp_ms=3000),
        ]

        futures_map = self.engine.prefetch_audio_futures(chunks, self.logger)
        self.assertIsNotNone(futures_map)
        self.assertEqual(len(futures_map), 3)  # type: ignore

        # Resolve futures
        for future in futures_map.values():  # type: ignore
            sig = future.result()
            self.assertEqual(len(sig.samples), 1600)
            self.assertEqual(sig.sample_rate, 16000)

        self.assertTrue(len(execution_threads) > 0)
        self.assertNotIn(main_thread_name, execution_threads)

    def test_prefetch_audio_futures_propagates_otel_context(self) -> None:
        """Verifies that pre-fetching in background threads inherits the active OpenTelemetry span context from the calling thread."""
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        with tracing_utils.with_tracer_context(
            traceparent, "parent_span", __name__
        ):
            captured_traceparents = {}

            def mock_fetch_and_decode(
                uri: str, trace_attrs: dict[str, str] | None = None
            ) -> AudioSignal:
                captured_traceparents[uri] = (
                    tracing_utils.get_current_traceparent()
                )
                return AudioSignal(
                    samples=np.zeros(1600, dtype=np.int16), sample_rate=16000
                )

            self.engine.processor.fetch_and_decode_audio = mock_fetch_and_decode  # type: ignore

            chunks = [
                BufferedChunk(gcs_uri="gs://bucket/1.flac", timestamp_ms=1000),
                BufferedChunk(gcs_uri="gs://bucket/2.flac", timestamp_ms=2000),
            ]

            futures_map = self.engine.prefetch_audio_futures(
                chunks, self.logger
            )
            self.assertIsNotNone(futures_map)

            for future in futures_map.values():  # type: ignore
                future.result()

            self.assertEqual(len(captured_traceparents), 2)
            for tp in captured_traceparents.values():
                self.assertTrue(
                    tp.startswith("00-4bf92f3577b34da6a3ce929d0e0e4736-"),
                    f"Expected trace ID 4bf92f3577b34da6a3ce929d0e0e4736 to propagate, got: {tp}",
                )


if __name__ == "__main__":
    unittest.main()
