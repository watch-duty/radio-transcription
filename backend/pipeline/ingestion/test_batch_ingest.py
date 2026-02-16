import json
import unittest

import apache_beam as beam
import requests
import responses
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from backend.pipeline.ingestion.batch_ingest import (
    fetch_url_content,
    get_metadata_fields,
)


class TestAudioPipeline(unittest.TestCase):
    def setUp(self) -> None:
        self.manifest_url = "https://example.com/manifest.txt"
        self.long_audio_url = "https://example.com/" + ("a" * 116) + ".mp3"
        self.short_audio_url = "https://example.com/short.mp3"
        self.sample_manifest_content = f"{self.long_audio_url}\n{self.short_audio_url}"

    @responses.activate
    def test_fetch_url_content_success(self) -> None:
        """Test successful manifest fetching."""
        responses.add(
            responses.GET,
            self.manifest_url,
            body=self.sample_manifest_content,
            status=200,
        )

        result = fetch_url_content(self.manifest_url)
        self.assertEqual(result, self.sample_manifest_content)

    @responses.activate
    def test_fetch_url_content_http_error(self) -> None:
        """Test handling of 404 errors during manifest fetch."""
        responses.add(responses.GET, self.manifest_url, status=404)

        with self.assertRaises(requests.exceptions.HTTPError):
            fetch_url_content(self.manifest_url)

    def test_fetch_url_invalid_scheme(self) -> None:
        """Test that invalid URLs raise ValueError."""
        with self.assertRaises(ValueError):
            fetch_url_content("ftp://invalid-url.com")

    @responses.activate
    def test_fetch_url_content_connection_error(self) -> None:
        """Test handling of network-level connection failures."""
        # Simulate a ConnectionError during the request
        responses.add(
            responses.GET,
            self.manifest_url,
            body=requests.exceptions.ConnectionError("Max retries exceeded with url"),
        )

        with self.assertRaises(requests.exceptions.ConnectionError):
            fetch_url_content(self.manifest_url)

    @responses.activate
    def test_fetch_url_content_unexpected_error(self) -> None:
        """Test handling of generic/unexpected exceptions."""
        # Simulate an unexpected exception type (e.g., RequestException)
        responses.add(
            responses.GET,
            self.manifest_url,
            body=requests.exceptions.RequestException("Something went wrong"),
        )

        with self.assertRaises(requests.exceptions.RequestException):
            fetch_url_content(self.manifest_url)

    @responses.activate
    def test_get_metadata_fields_success(self) -> None:
        """Test metadata extraction from a mock audio file."""
        fake_audio_data = b"fake-audio-binary-content"
        responses.add(
            responses.GET, self.long_audio_url, body=fake_audio_data, status=200
        )

        expected = {
            "file_path": self.long_audio_url,
            "byte_length": len(fake_audio_data),
            "source": "Echo",
        }

        result = get_metadata_fields(self.long_audio_url)
        self.assertEqual(result, expected)

    @responses.activate
    def test_get_metadata_fields_error(self) -> None:
        """Test metadata extraction returns empty dict when HTTP 404 occurs."""
        fake_audio_data = b"fake-audio-binary-content"
        responses.add(
            responses.GET, self.long_audio_url, body=fake_audio_data, status=404
        )

        expected = {"file_path": self.long_audio_url}

        result = get_metadata_fields(self.long_audio_url)
        self.assertEqual(result, expected)

    @responses.activate
    def test_pipeline_logic_with_metadata_error(self) -> None:
        """
        Tests pipeline behavior when metadata extraction fails and returns an empty dict.
        Verifies whether empty metadata dicts are propagated through the pipeline.
        """
        manifest_content = self.sample_manifest_content
        # Mock the manifest fetch
        responses.add(
            responses.GET, self.manifest_url, body=manifest_content, status=200
        )
        # Mock the audio file fetch to fail, causing get_metadata_fields to return {}
        responses.add(responses.GET, self.long_audio_url, body=b"error", status=404)
        # filtered out due to short length, but we mock it anyway
        responses.add(responses.GET, self.short_audio_url, body=b"12345678", status=404)
        # Given the current pipeline steps, an empty dict will be serialized to b'{}'
        expected_output = [
            json.dumps({"file_path": self.long_audio_url}).encode("utf-8")
        ]
        with TestPipeline() as p:
            output = (
                p
                | "Start" >> beam.Create([self.manifest_url])
                | "Fetch" >> beam.Map(fetch_url_content)
                | "Split" >> beam.FlatMap(lambda content: content.splitlines())
                | "Filter" >> beam.Filter(lambda line: len(line) > 115)
                | "Metadata" >> beam.Map(get_metadata_fields)
                | "JSON" >> beam.Map(lambda data: json.dumps(data).encode("utf-8"))
            )
            assert_that(output, equal_to(expected_output))

    @responses.activate
    def test_pipeline_logic(self) -> None:
        """
        Tests the core transformation logic of the pipeline using TestPipeline.
        This excludes the Pub/Sub IO to keep the test hermetic.
        """
        manifest_content = self.sample_manifest_content
        fake_audio_data = b"12345"  # 5 bytes

        # Mock the manifest fetch
        responses.add(
            responses.GET, self.manifest_url, body=manifest_content, status=200
        )
        # Mock the audio file fetch
        responses.add(
            responses.GET, self.long_audio_url, body=fake_audio_data, status=200
        )
        # filtered out due to short length, but we mock it anyway
        responses.add(responses.GET, self.short_audio_url, body=b"12345678", status=404)

        expected_output = [
            json.dumps(
                {"file_path": self.long_audio_url, "byte_length": 5, "source": "Echo"}
            ).encode("utf-8")
        ]

        with TestPipeline() as p:
            output = (
                p
                | "Start" >> beam.Create([self.manifest_url])
                | "Fetch" >> beam.Map(fetch_url_content)
                | "Split" >> beam.FlatMap(lambda content: content.splitlines())
                | "Filter" >> beam.Filter(lambda line: len(line) > 115)
                | "Metadata" >> beam.Map(get_metadata_fields)
                | "JSON" >> beam.Map(lambda data: json.dumps(data).encode("utf-8"))
            )

            assert_that(output, equal_to(expected_output))


if __name__ == "__main__":
    unittest.main()
