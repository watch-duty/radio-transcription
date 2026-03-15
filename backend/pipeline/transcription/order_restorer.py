import logging
import json
from collections.abc import Generator
from dataclasses import dataclass

import apache_beam as beam
from apache_beam import window # type: ignore[import-untyped, unresolved-import]
from apache_beam.transforms.userstate import (
    BagStateSpec,
    ReadModifyWriteStateSpec,
    TimerSpec,
    on_timer,
)
from apache_beam.transforms.timeutil import TimeDomain

from backend.pipeline.shared_constants import CHUNK_DURATION_SECONDS
from backend.pipeline.transcription.constants import MS_PER_SECOND

logger = logging.getLogger(__name__)

@dataclass
class OrderRestorerConfig:
    out_of_order_timeout_ms: int = 5000
    chunk_duration_ms: int = CHUNK_DURATION_SECONDS * MS_PER_SECOND

class RestoreOrderFn(beam.DoFn):
    OOO_BUFFER_SPEC = BagStateSpec("ooo_buffer", beam.coders.BytesCoder())
    OOO_BUFFER_STATE = beam.DoFn.StateParam(OOO_BUFFER_SPEC)

    EXPECTED_NEXT_TS_SPEC = ReadModifyWriteStateSpec("expected_next_ts", beam.coders.VarIntCoder())
    EXPECTED_NEXT_TS_STATE = beam.DoFn.StateParam(EXPECTED_NEXT_TS_SPEC)

    TIMER_ACTIVE_SPEC = ReadModifyWriteStateSpec("timer_active", beam.coders.BooleanCoder())
    TIMER_ACTIVE_STATE = beam.DoFn.StateParam(TIMER_ACTIVE_SPEC)

    OOO_TIMER_SPEC = TimerSpec("ooo_timer", TimeDomain.REAL_TIME)
    OOO_TIMER = beam.DoFn.TimerParam(OOO_TIMER_SPEC)

    def __init__(self, config: OrderRestorerConfig):
        self.config = config

    def process( # type: ignore[override]
        self,
        element: tuple[str, str],
        timestamp: beam.utils.timestamp.Timestamp = beam.DoFn.TimestampParam,
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState = EXPECTED_NEXT_TS_STATE, # type: ignore
        ooo_buffer: beam.transforms.userstate.BagRuntimeState = OOO_BUFFER_STATE, # type: ignore
        timer_active: beam.transforms.userstate.ReadModifyWriteRuntimeState = TIMER_ACTIVE_STATE, # type: ignore
        ooo_timer: beam.transforms.userstate.RuntimeTimer = OOO_TIMER, # type: ignore
    ) -> Generator[tuple[str, str], None, None]:
        feed_id, gcs_path = element
        ts_ms = int(float(timestamp) * MS_PER_SECOND)

        expected = expected_next_ts.read()

        if expected is None or ts_ms == expected:
            # Happy path: In order or very first chunk.
            yield element
            self._advance_expected(ts_ms, expected_next_ts)
            yield from self._drain_ready_elements(
                feed_id, expected_next_ts, ooo_buffer, timer_active, ooo_timer
            )
        elif ts_ms < expected:
            # Late arrival for a gap we already accepted and moved past!
            # Since the state machine flushes on gaps, if we output this now,
            # it will be treated as a NEW transmission start, and then since
            # the next things are much later, it'll flush again. 
            # We can literally just output it. Since we accept the gap, it becomes 
            # its own tiny transmission.
            logger.info("Received late chunk older than expected: %s vs %s. Yielding despite the gap.", ts_ms, expected)
            yield element
        else:
            # Out of order: ts_ms > expected
            # Buffer it and start the processing timer if not tracking one yet.
            logger.info("Out of order! Expected %s, got %s. Buffering %s", expected, ts_ms, gcs_path)
            payload = json.dumps({"ts_ms": ts_ms, "gcs_path": gcs_path}).encode("utf-8")
            ooo_buffer.add(payload)

            is_timer_active = timer_active.read()
            if not is_timer_active:
                # Set a Processing Time timer to fire after timeout.
                # Currently time in Python Beam Timer is mostly ignored and uses time.time() internally for REAL_TIME
                # We can set the timer by passing an offset using Timestamp.
                # Actually, the API for REAL_TIME expects an absolute timestamp. But since it's hard to get runner's clock,
                # a common hack is to use 0 or offset. Wait, no, we shouldn't use time.time() if processing in DirectRunner.
                # Let's rely on TimeDomain.REAL_TIME + timeout seconds. 
                import time
                deadline_s = time.time() + (self.config.out_of_order_timeout_ms / 1000.0)
                ooo_timer.set(beam.utils.timestamp.Timestamp(deadline_s))
                timer_active.write(True)

    def _advance_expected(self, current_ts_ms: int, expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState) -> None:
        expected_next_ts.write(current_ts_ms + self.config.chunk_duration_ms)

    def _drain_ready_elements(
        self,
        feed_id: str,
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState,
        ooo_buffer: beam.transforms.userstate.BagRuntimeState,
        timer_active: beam.transforms.userstate.ReadModifyWriteRuntimeState,
        ooo_timer: beam.transforms.userstate.RuntimeTimer,
    ) -> Generator[tuple[str, str], None, None]:
        buffer_elements = list(ooo_buffer.read())
        if not buffer_elements:
            return

        parsed = [json.loads(b.decode("utf-8")) for b in buffer_elements]
        parsed.sort(key=lambda x: x["ts_ms"])

        expected = expected_next_ts.read()
        retained = []
        drained_any = False

        for item in parsed:
            if item["ts_ms"] == expected:
                # We found the one we were waiting for!
                yield (feed_id, item["gcs_path"])
                expected = item["ts_ms"] + self.config.chunk_duration_ms
                drained_any = True
            else:
                retained.append(item)

        if drained_any:
            expected_next_ts.write(expected)
            ooo_buffer.clear()
            for item in retained:
                ooo_buffer.add(json.dumps(item).encode("utf-8"))

        if not retained and timer_active.read():
            ooo_timer.clear()
            timer_active.clear()

    @on_timer(OOO_TIMER_SPEC)
    def handle_gap_timeout(
        self,
        key: str = beam.DoFn.KeyParam, # type: ignore
        expected_next_ts: beam.transforms.userstate.ReadModifyWriteRuntimeState = EXPECTED_NEXT_TS_STATE, # type: ignore
        ooo_buffer: beam.transforms.userstate.BagRuntimeState = OOO_BUFFER_STATE, # type: ignore
        timer_active: beam.transforms.userstate.ReadModifyWriteRuntimeState = TIMER_ACTIVE_STATE, # type: ignore
    ) -> Generator[tuple[str, str], None, None]:
        logger.info("Order restorer timeout fired for feed: %s. Accepting the Gap.", key)
        timer_active.clear()

        buffer_elements = list(ooo_buffer.read())
        if not buffer_elements:
            return

        parsed = [json.loads(b.decode("utf-8")) for b in buffer_elements]
        parsed.sort(key=lambda x: x["ts_ms"])

        # Yield EVERYTHING in chronological order.
        # This implicitly skips the missing sequence number and resets `expected` to the very end.
        expected = expected_next_ts.read()
        for item in parsed:
            # We enforce that all buffered items are strictly emitted.
            # We don't advance the global `expected_next_ts` until the end.
            yield (key, item["gcs_path"])
            expected = item["ts_ms"] + self.config.chunk_duration_ms

        expected_next_ts.write(expected)
        ooo_buffer.clear()
