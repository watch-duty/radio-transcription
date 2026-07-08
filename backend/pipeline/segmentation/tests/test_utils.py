"""Tests for segmentation utility functions."""

import unittest

from backend.pipeline.schema_types.segmented_audio_pb2 import SegmentedAudio
from backend.pipeline.segmentation import utils as trans_utils


class SegmentationUtilsTest(unittest.TestCase):
    def test_is_speech_classification(self) -> None:
        self.assertTrue(
            trans_utils.is_speech_classification(
                SegmentedAudio.AUDIO_CLASSIFICATION_SPEECH
            )
        )
        self.assertFalse(
            trans_utils.is_speech_classification(
                SegmentedAudio.AUDIO_CLASSIFICATION_OTHER
            )
        )
        self.assertFalse(
            trans_utils.is_speech_classification(
                SegmentedAudio.AUDIO_CLASSIFICATION_UNSPECIFIED
            )
        )


if __name__ == "__main__":
    unittest.main()
