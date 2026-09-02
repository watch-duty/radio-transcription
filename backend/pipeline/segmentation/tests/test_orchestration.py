import argparse
import inspect
import json
from pathlib import Path
from types import ModuleType

import apache_beam as beam
import pytest
from apache_beam.pipeline import AppliedPTransform, PipelineVisitor
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from backend.pipeline.segmentation.constants import (
    DEFAULT_PUBSUB_FALLBACK_DRAIN_TIMEOUT_MS,
)
from backend.pipeline.segmentation.datatypes import FlushRequest
from backend.pipeline.segmentation.options import SegmentationOptions
from backend.pipeline.segmentation.orchestration import get_pipeline
from backend.pipeline.segmentation.transforms import (
    stateful as stateful_module,
)
from backend.pipeline.segmentation.transforms import (
    stateless as stateless_module,
)
from backend.pipeline.segmentation.transforms.stateful import (
    TagSequenceNumberFn,
)


def test_pipeline_topology_typehints() -> None:
    """Builds the DAG to trigger Apache Beam's static type checker instantaneously."""
    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--staging_audio_bucket",
            "test-staging-bucket",
        ]
    )

    # Calling get_pipeline() dynamically maps the entire Apache Beam graph in memory.
    # If there are any TypeCheckErrors or missing links between transforms,
    # the Python SDK will immediately raise an exception right here!
    pipeline = get_pipeline(options)

    # We do NOT invoke pipeline.run(), because we just wanted to validate the topological typing.
    assert pipeline is not None


def test_sequence_tagger_is_fed_by_session_rekey() -> None:
    """The stateful sequence tagger MUST be fed by the feed_id#session_id re-key.

    Beam partitions stateful DoFn state by the *input* key. If KeyBySession is
    removed or reordered, TagSequenceNumberFn silently reverts to a per-feed
    counter while PubSubOrderRestorerFn stays partitioned per session, so every
    session blocks forever on sequence numbers routed to a different partition.
    That regression is invisible to DoFn-level unit tests, so it is pinned here
    against the real DAG.
    """
    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--staging_audio_bucket",
            "test-staging-bucket",
        ]
    )
    pipeline = get_pipeline(options)

    class _Collector(PipelineVisitor):
        def __init__(self) -> None:
            self.nodes: list[AppliedPTransform] = []

        def visit_transform(self, transform_node: AppliedPTransform) -> None:
            self.nodes.append(transform_node)

    collector = _Collector()
    pipeline.visit(collector)

    taggers = [
        n for n in collector.nodes if n.full_label.endswith("TagSequenceNumber")
    ]
    assert len(taggers) == 1, "expected exactly one TagSequenceNumber transform"

    (tagger_input,) = taggers[0].inputs
    producer = tagger_input.producer
    assert producer is not None, "TagSequenceNumber has no upstream producer"
    assert producer.full_label.endswith("KeyBySession"), (
        "TagSequenceNumber must consume the KeyBySession re-key so its state is "
        f"partitioned per feed_id#session_id, but it consumes '{producer.full_label}'"
    )


def test_pipeline_invalid_timeout() -> None:
    """Verifies failure when ooo timeout >= stale_timeout_ms."""
    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--staging_audio_bucket",
            "test-staging-bucket",
            "--out_of_order_timeout_ms",
            "80000",
            "--stale_timeout_ms",
            "70000",
        ]
    )

    with pytest.raises(
        ValueError,
        match=r"stale_timeout_ms .* must be strictly greater than out_of_order_timeout_ms",
    ):
        get_pipeline(options)


def test_metadata_json_parameters_parity() -> None:
    """Enforces registration of new programmatic options in metadata.json."""
    parser = argparse.ArgumentParser()
    SegmentationOptions._add_argparse_args(parser)

    declared_options = {
        action.dest
        for action in parser._actions
        if action.dest and action.dest != "help"
    }

    # Parse template specification
    meta_path = Path(__file__).parent.parent / "metadata.json"
    with open(meta_path) as f:
        metadata = json.load(f)

    metadata_parameters = {param["name"] for param in metadata["parameters"]}

    missing = declared_options - metadata_parameters
    assert not missing, (
        f"Config options defined in options.py missing from metadata.json spec: {missing}"
    )

    stale = metadata_parameters - declared_options
    assert not stale, (
        f"Entries in metadata.json have no corresponding option in options.py (stale?): {stale}"
    )


def test_pipeline_session_keying_sequence_isolation() -> None:
    """Verifies that KeyBySession | TagSequenceNumber generates independent sequence numbers (1, 2) per session."""
    req_a1 = FlushRequest(feed_id="feed-1", session_id="session-a")
    req_a2 = FlushRequest(feed_id="feed-1", session_id="session-a")
    req_b1 = FlushRequest(feed_id="feed-1", session_id="session-b")
    req_b2 = FlushRequest(feed_id="feed-1", session_id="session-b")

    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
        ]
    )

    with TestPipeline(options=options) as p:
        inputs = p | beam.Create(
            [
                ("key-1", req_a1),
                ("key-2", req_b1),
                ("key-3", req_a2),
                ("key-4", req_b2),
            ]
        )
        session_keyed = inputs | "KeyBySession" >> beam.Map(
            lambda kv: (f"{kv[1].feed_id}#{kv[1].session_id}", kv[1])
        )
        tagged = session_keyed | "TagSequenceNumber" >> beam.ParDo(
            TagSequenceNumberFn()
        )

        seq_results = tagged | beam.Map(lambda kv: (kv[0], kv[1][0]))

        assert_that(
            seq_results,
            equal_to(
                [
                    ("feed-1#session-a", 1),
                    ("feed-1#session-b", 1),
                    ("feed-1#session-a", 2),
                    ("feed-1#session-b", 2),
                ]
            ),
        )


def _restorer_from_pipeline(extra_flags: list[str] | None = None):
    """Builds the real DAG and returns the PubSubOrderRestorerFn instance in it."""
    options = SegmentationOptions(
        flags=[
            "--project",
            "test-project",
            "--input_subscription",
            "projects/test-project/subscriptions/audio-in",
            "--output_topic",
            "projects/test-project/topics/out",
            "--dlq_topic",
            "projects/test-project/topics/dlq",
            "--staging_audio_bucket",
            "test-staging-bucket",
            *(extra_flags or []),
        ]
    )
    pipeline = get_pipeline(options)

    class _Collector(PipelineVisitor):
        def __init__(self) -> None:
            self.nodes: list[AppliedPTransform] = []

        def visit_transform(self, transform_node: AppliedPTransform) -> None:
            self.nodes.append(transform_node)

    collector = _Collector()
    pipeline.visit(collector)
    restorers = [
        n
        for n in collector.nodes
        if n.full_label.endswith("RestorePubSubOrder")
    ]
    assert len(restorers) == 1, (
        "expected exactly one RestorePubSubOrder transform"
    )
    transform = restorers[0].transform
    assert transform is not None
    return transform.fn


def test_fallback_drain_timeout_defaults_from_constants() -> None:
    assert (
        _restorer_from_pipeline().timeout_ms
        == DEFAULT_PUBSUB_FALLBACK_DRAIN_TIMEOUT_MS
    )


def test_fallback_drain_timeout_is_overridable_at_launch() -> None:
    """Production is the only place upload skew is observable, so this must be
    tunable without a code change.
    """
    override = 180000
    assert override != DEFAULT_PUBSUB_FALLBACK_DRAIN_TIMEOUT_MS, (
        "the override must differ from the default, or this asserts nothing"
    )
    fn = _restorer_from_pipeline(
        ["--pubsub_fallback_drain_timeout_ms", str(override)]
    )
    assert fn.timeout_ms == override


def test_non_positive_fallback_drain_timeout_is_rejected() -> None:
    """Zero would force-drain out of order the instant any gap appeared."""
    with pytest.raises(
        ValueError,
        match=r"pubsub_fallback_drain_timeout_ms .* must be positive",
    ):
        _restorer_from_pipeline(["--pubsub_fallback_drain_timeout_ms", "0"])


def _beam_dofns_defined_in(module: ModuleType) -> list[type]:
    """Returns beam.DoFn subclasses defined in (not merely imported into) a module."""
    return [
        obj
        for _, obj in inspect.getmembers(module, inspect.isclass)
        if issubclass(obj, beam.DoFn)
        and obj is not beam.DoFn
        and obj.__module__ == module.__name__
    ]


def _declares_beam_state(dofn_cls: type) -> bool:
    """True if any method declares a StateParam or TimerParam default.

    Checked statically rather than through is_stateful_dofn so nothing has to be
    constructed, and -- more importantly -- so that a bare StateSpec passed where
    a StateParam belongs reads as "not stateful", which is the defect this is
    meant to catch.
    """
    for _, method in inspect.getmembers(dofn_cls, inspect.isfunction):
        for param in inspect.signature(method).parameters.values():
            if isinstance(
                param.default, beam.DoFn.StateParam | beam.DoFn.TimerParam
            ):
                return True
    return False


def test_dofns_live_in_the_module_matching_their_statefulness() -> None:
    """stateful.py and stateless.py split on a property Beam itself can check.

    This is not bookkeeping. A DoFn whose state is declared with a bare
    StateSpec instead of beam.DoFn.StateParam(...) is silently not stateful:
    Beam never wires the state up, every element raises AttributeError on first
    access, and unit tests that inject mocks for those parameters pass anyway.
    Such a DoFn sits in stateful.py while failing this check, so the mismatch
    between file and behaviour is what surfaces the bug.
    """
    stateful_dofns = _beam_dofns_defined_in(stateful_module)
    stateless_dofns = _beam_dofns_defined_in(stateless_module)
    assert stateful_dofns and stateless_dofns, "expected DoFns in both modules"

    for cls in stateful_dofns:
        assert _declares_beam_state(cls), (
            f"{cls.__name__} is in stateful.py but declares no StateParam or "
            "TimerParam. Either it belongs in stateless.py, or its specs are "
            "not wrapped in beam.DoFn.StateParam()/TimerParam() -- in which "
            "case Beam will not treat it as stateful at runtime."
        )

    for cls in stateless_dofns:
        assert not _declares_beam_state(cls), (
            f"{cls.__name__} is in stateless.py but declares Beam state or "
            "timers, so Beam will serialize its processing per key. It belongs "
            "in stateful.py."
        )
