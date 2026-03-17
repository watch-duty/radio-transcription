import unittest

from backend.pipeline.detection.detector_factory import DetectorFactory
from backend.pipeline.detection.sound_event_signal_combiner import (
    SoundEventSignalCombiner,
)
from backend.pipeline.detection.types import DetectionResult


class _FakeDetector:
    """Stub detector that records constructor kwargs."""

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @property
    def detector_type(self) -> str:
        return "fake"

    def detect(self, samples):
        return DetectionResult(speech_regions=(), detector_type="fake")


class _AnotherFakeDetector:
    """Second stub for multi-detector tests."""

    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs

    @property
    def detector_type(self) -> str:
        return "another"

    def detect(self, samples):
        return DetectionResult(speech_regions=(), detector_type="another")


class TestRegister(unittest.TestCase):
    """Tests for DetectorFactory.register / unregister."""

    def setUp(self) -> None:
        self._saved_registry = DetectorFactory._registry.copy()

    def tearDown(self) -> None:
        DetectorFactory._registry.clear()
        DetectorFactory._registry.update(self._saved_registry)

    def test_register_and_lookup(self) -> None:
        DetectorFactory.register("fake", _FakeDetector)
        self.assertIn("fake", DetectorFactory._registry)
        self.assertIs(DetectorFactory._registry["fake"], _FakeDetector)

    def test_register_duplicate_raises(self) -> None:
        DetectorFactory.register("fake", _FakeDetector)
        with self.assertRaises(ValueError) as ctx:
            DetectorFactory.register("fake", _AnotherFakeDetector)
        self.assertIn("already registered", str(ctx.exception))

    def test_unregister_removes_entry(self) -> None:
        DetectorFactory.register("fake", _FakeDetector)
        DetectorFactory.unregister("fake")
        self.assertNotIn("fake", DetectorFactory._registry)

    def test_unregister_nonexistent_does_not_raise(self) -> None:
        DetectorFactory.unregister("nonexistent")


class TestCreateEnsemble(unittest.TestCase):
    """Tests for DetectorFactory.create_ensemble."""

    def setUp(self) -> None:
        self._saved_registry = DetectorFactory._registry.copy()
        DetectorFactory.register("fake", _FakeDetector)
        DetectorFactory.register("another", _AnotherFakeDetector)

    def tearDown(self) -> None:
        DetectorFactory._registry.clear()
        DetectorFactory._registry.update(self._saved_registry)

    def test_returns_detectors_and_combiner(self) -> None:
        config = {"detectors": [{"type": "fake"}]}
        detectors, combiner = DetectorFactory.create_ensemble(config)
        self.assertEqual(len(detectors), 1)
        self.assertIsInstance(combiner, SoundEventSignalCombiner)

    def test_kwargs_forwarded(self) -> None:
        config = {"detectors": [{"type": "fake", "threshold": 0.4, "mode": "fast"}]}
        detectors, _ = DetectorFactory.create_ensemble(config)
        detector = detectors[0]
        assert isinstance(detector, _FakeDetector)
        self.assertEqual(detector.kwargs, {"threshold": 0.4, "mode": "fast"})

    def test_unknown_detector_type_raises(self) -> None:
        config = {"detectors": [{"type": "unknown"}]}
        with self.assertRaises(KeyError) as ctx:
            DetectorFactory.create_ensemble(config)
        self.assertIn("unknown", str(ctx.exception))

    def test_missing_type_key_raises(self) -> None:
        config = {"detectors": [{"threshold": 0.4}]}
        with self.assertRaises(KeyError) as ctx:
            DetectorFactory.create_ensemble(config)
        self.assertIn("type", str(ctx.exception))

    def test_empty_detectors_list(self) -> None:
        config = {"detectors": []}
        detectors, combiner = DetectorFactory.create_ensemble(config)
        self.assertEqual(detectors, [])
        self.assertIsInstance(combiner, SoundEventSignalCombiner)

    def test_missing_detectors_key(self) -> None:
        config = {}
        detectors, combiner = DetectorFactory.create_ensemble(config)
        self.assertEqual(detectors, [])
        self.assertIsInstance(combiner, SoundEventSignalCombiner)

    def test_does_not_mutate_config(self) -> None:
        entry = {"type": "fake", "threshold": 0.4}
        config = {"detectors": [entry]}
        DetectorFactory.create_ensemble(config)
        self.assertIn("type", entry)

    def test_multiple_detectors(self) -> None:
        config = {"detectors": [{"type": "fake"}, {"type": "another"}]}
        detectors, _ = DetectorFactory.create_ensemble(config)
        self.assertEqual(len(detectors), 2)


if __name__ == "__main__":
    unittest.main()
