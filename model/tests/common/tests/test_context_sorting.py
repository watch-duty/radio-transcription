import re
import unittest
from pathlib import Path
from collections import defaultdict

def get_parent_recording(entry):
    uri = entry.get("original_audio_uri") or entry.get("audio_uri")
    if uri:
        return uri
    filepath = entry.get("audio_filepath", "")
    filename = Path(filepath).name
    match = re.match(r"^(.*?)(?:_\d{8}_\d{2})?(?:__seg|-row-)", filename)
    if match:
        return match.group(1)
    return ""

def group_and_sort_manifest_entries(entries):
    channels = defaultdict(list)
    for entry in entries:
        filepath = entry["audio_filepath"]
        filename = Path(filepath).name

        # Grouping key priority: source_group -> example_id -> filename regex -> parent path
        channel_id = entry.get("source_group")
        if not channel_id:
            eid = entry.get("example_id")
            if eid and "-row-" not in str(eid):
                channel_id = eid
        if not channel_id:
            match = re.match(
                r"^(.*?)(?:_\d{8}_\d{2})?(?:__seg|-row-)", filename
            )
            if match:
                channel_id = match.group(1)
            else:
                channel_id = Path(filepath).parent.name

        entry["example_id"] = channel_id
        channels[channel_id].append(entry)

    # Sort chronologically
    for cid in channels:
        channels[cid].sort(
            key=lambda x: (
                get_parent_recording(x),
                x.get("original_offset") if x.get("original_offset") is not None else x.get("offset", 0),
                x.get("start_time", 0),
            )
        )
    return channels


class TestContextSortingAndGrouping(unittest.TestCase):
    def test_sft_dataset_grouping_and_sorting(self):
        # SFT manifests have offset=0.0, derived audio files inside derived/,
        # source_group identifies the channel, and original_offset represents chronology.
        entries = [
            {
                "audio_filepath": "gs://bucket/sft/v1/audio/derived/echo-row-3-abc.flac",
                "offset": 0.0,
                "source_group": "echo:wa_cathlamet/Wahkiakum_Fire_Disp",
                "original_offset": 10.5,
                "original_audio_uri": "gs://bucket/raw/Wahkiakum_Fire_Disp_20250326_07.wav",
                "text": "third segment",
            },
            {
                "audio_filepath": "gs://bucket/sft/v1/audio/derived/echo-row-1-xyz.flac",
                "offset": 0.0,
                "source_group": "echo:wa_cathlamet/Wahkiakum_Fire_Disp",
                "original_offset": 1.5,
                "original_audio_uri": "gs://bucket/raw/Wahkiakum_Fire_Disp_20250326_07.wav",
                "text": "first segment",
            },
            {
                "audio_filepath": "gs://bucket/sft/v1/audio/derived/echo-row-2-def.flac",
                "offset": 0.0,
                "source_group": "echo:wa_cathlamet/Wahkiakum_Fire_Disp",
                "original_offset": 5.0,
                "original_audio_uri": "gs://bucket/raw/Wahkiakum_Fire_Disp_20250326_07.wav",
                "text": "second segment",
            },
            {
                "audio_filepath": "gs://bucket/sft/v1/audio/derived/echo-row-4-ghi.flac",
                "offset": 0.0,
                "source_group": "echo:or_astria/Astria_Police_Disp",
                "original_offset": 2.0,
                "original_audio_uri": "gs://bucket/raw/Astria_Police_Disp_20250326_07.wav",
                "text": "different channel segment",
            },
        ]

        channels = group_and_sort_manifest_entries(entries)

        # Assert correct channels were isolated
        self.assertEqual(set(channels.keys()), {
            "echo:wa_cathlamet/Wahkiakum_Fire_Disp",
            "echo:or_astria/Astria_Police_Disp",
        })

        # Assert correct segment count per channel
        self.assertEqual(len(channels["echo:wa_cathlamet/Wahkiakum_Fire_Disp"]), 3)
        self.assertEqual(len(channels["echo:or_astria/Astria_Police_Disp"]), 1)

        # Assert chronological sorting within channel
        sorted_texts = [x["text"] for x in channels["echo:wa_cathlamet/Wahkiakum_Fire_Disp"]]
        self.assertEqual(sorted_texts, ["first segment", "second segment", "third segment"])

    def test_legacy_dataset_grouping_and_sorting(self):
        # Legacy manifests use filename parsing for channel_id,
        # with offset and start_time indicating chronology.
        entries = [
            {
                "audio_filepath": "gs://bucket/audio/wa_cathlamet/Wahkiakum_Fire_Disp_20250326_07__segA.wav",
                "offset": 10.5,
                "start_time": 0.0,
                "text": "third segment",
            },
            {
                "audio_filepath": "gs://bucket/audio/wa_cathlamet/Wahkiakum_Fire_Disp_20250326_07__segB.wav",
                "offset": 1.5,
                "start_time": 0.0,
                "text": "first segment",
            },
            {
                "audio_filepath": "gs://bucket/audio/wa_cathlamet/Wahkiakum_Fire_Disp_20250326_07__segC.wav",
                "offset": 5.0,
                "start_time": 0.0,
                "text": "second segment",
            },
        ]

        channels = group_and_sort_manifest_entries(entries)

        # Channel name is extracted from the filename pattern
        channel_name = "Wahkiakum_Fire_Disp"
        self.assertEqual(list(channels.keys()), [channel_name])

        # Assert chronological sorting within channel
        sorted_texts = [x["text"] for x in channels[channel_name]]
        self.assertEqual(sorted_texts, ["first segment", "second segment", "third segment"])
