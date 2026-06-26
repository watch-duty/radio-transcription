import asyncio
import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from backend.pipeline.common.clients.audio_segments_client import (
    AsyncAudioSegmentsClient,
    AudioSegmentsClient,
    GCPMetadataAsyncAuth,
    GCPMetadataAuth,
    _async_audience_locks,
    _async_token_cache,
)
from backend.services.audio_segments.models import AnnotationType


class TestAudioSegmentsClient(unittest.TestCase):
    def setUp(self) -> None:
        self.api_url = "http://test-api.com"
        self.client = AudioSegmentsClient(self.api_url)
        self.mock_session = MagicMock()
        self.client.session = self.mock_session

        self.segment_payload = {
            "id": "segment-id-123",
            "feed_id": "feed-id-456",
            "classification": "SPEECH",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "end_timestamp": "2026-01-01T00:01:00Z",
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
        }

    def test_init_with_custom_max_retries(self) -> None:
        # Execute
        client = AudioSegmentsClient(self.api_url, max_retries=5)

        # Verify
        adapter = client.session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(adapter.max_retries.total, 5)
            self.assertEqual(
                adapter.max_retries.status_forcelist, [429, 500, 502, 503, 504]
            )

    def test_init_with_zero_max_retries(self) -> None:
        # Execute
        client = AudioSegmentsClient(self.api_url, max_retries=0)

        # Verify
        adapter = client.session.adapters.get("http://")
        self.assertIsNotNone(adapter)
        if adapter is not None:
            self.assertEqual(
                getattr(adapter.max_retries, "total", adapter.max_retries), 0
            )

    def test_add_audio_segment_annotation_success(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_session.post.return_value = mock_response

        annotation_data = {"decisions": ["ALERT"], "errors": []}

        # Execute
        self.client.add_audio_segment_annotation(
            audio_segment_id="segment-id-123",
            annotation_type=AnnotationType.EVALUATION,
            data=annotation_data,
        )

        # Verify
        self.mock_session.post.assert_called_once()
        args, kwargs = self.mock_session.post.call_args
        self.assertEqual(
            args[0],
            "http://test-api.com/v1/audio_segments/segment-id-123/annotations",
        )
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"]["type"], "EVALUATION")
        self.assertEqual(kwargs["json"]["data"], annotation_data)

    @patch("backend.pipeline.common.clients.audio_segments_client.is_gcp_env")
    def test_client_initializes_with_auth_in_gcp(self, mock_is_gcp_env) -> None:
        mock_is_gcp_env.return_value = True
        client = AudioSegmentsClient("http://test-api.com")
        auth = client.session.auth
        self.assertIsNotNone(auth)
        self.assertIsInstance(auth, GCPMetadataAuth)
        if isinstance(auth, GCPMetadataAuth):
            self.assertEqual(auth.audience, "http://test-api.com")

    @patch("backend.pipeline.common.clients.audio_segments_client.get_id_token")
    def test_gcp_metadata_auth_adds_header(self, mock_get_id_token) -> None:
        mock_get_id_token.return_value = "fake-token"
        auth = GCPMetadataAuth(audience="http://audience")
        request = MagicMock()
        request.headers = {}

        result = auth(request)

        self.assertIsNotNone(result.headers)
        if result.headers is not None:
            self.assertEqual(
                result.headers["Authorization"], "Bearer fake-token"
            )
        mock_get_id_token.assert_called_once_with("http://audience")

    def test_add_audio_segment_annotation_propagates_exception(self) -> None:
        # Setup
        self.mock_session.post.side_effect = Exception("Network error")

        # Execute & Verify
        with self.assertRaises(Exception):
            self.client.add_audio_segment_annotation(
                audio_segment_id="segment-id-123",
                annotation_type=AnnotationType.EVALUATION,
                data={"key": "val"},
            )

    def test_add_audio_segment_success(self) -> None:
        # Setup
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_session.post.return_value = mock_response

        # Execute
        self.client.add_audio_segment(self.segment_payload)

        # Verify
        self.mock_session.post.assert_called_once()
        args, kwargs = self.mock_session.post.call_args
        self.assertEqual(args[0], "http://test-api.com/v1/audio_segments")
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"], self.segment_payload)

    def test_add_audio_segment_propagates_exception(self) -> None:
        # Setup
        self.mock_session.post.side_effect = Exception("Network error")

        # Execute & Verify
        with self.assertRaises(Exception):
            self.client.add_audio_segment(self.segment_payload)


class TestAsyncAudioSegmentsClient(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        _async_token_cache.clear()
        _async_audience_locks.clear()

        self.api_url = "http://test-api.com"
        self.client = AsyncAudioSegmentsClient(self.api_url)
        self.post_patcher = patch(
            "httpx.AsyncClient.post", new_callable=AsyncMock
        )
        self.mock_post = self.post_patcher.start()

        self.segment_payload = {
            "id": "segment-id-123",
            "feed_id": "feed-id-456",
            "classification": "SPEECH",
            "start_timestamp": "2026-01-01T00:00:00Z",
            "end_timestamp": "2026-01-01T00:01:00Z",
            "missing_prior_context": False,
            "missing_post_context": False,
            "source_audio_uris": ["gs://bucket/audio1.ogg"],
        }

    async def asyncTearDown(self) -> None:
        self.post_patcher.stop()

    async def test_add_audio_segment_annotation_success(self) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_post.return_value = mock_response

        annotation_data = {"decisions": ["ALERT"], "errors": []}

        await self.client.add_audio_segment_annotation(
            audio_segment_id="segment-id-123",
            annotation_type=AnnotationType.EVALUATION,
            data=annotation_data,
        )

        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        self.assertEqual(
            args[0],
            "http://test-api.com/v1/audio_segments/segment-id-123/annotations",
        )
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"]["type"], "EVALUATION")
        self.assertEqual(kwargs["json"]["data"], annotation_data)

    @patch("backend.pipeline.common.clients.audio_segments_client.is_gcp_env")
    def test_async_client_initializes_with_auth_in_gcp(
        self, mock_is_gcp_env
    ) -> None:
        mock_is_gcp_env.return_value = True
        client = AsyncAudioSegmentsClient("http://test-api.com")
        auth = client.client.auth
        self.assertIsNotNone(auth)
        self.assertIsInstance(auth, GCPMetadataAsyncAuth)
        if isinstance(auth, GCPMetadataAsyncAuth):
            self.assertEqual(auth.audience, "http://test-api.com")

    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_gcp_metadata_async_auth_adds_header(self, mock_get) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "fake-token"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        auth = GCPMetadataAsyncAuth(audience="http://audience")
        request = MagicMock()
        request.headers = {}

        flow = auth.async_auth_flow(request)
        async for req in flow:
            self.assertIsNotNone(req.headers)
            if req.headers is not None:
                self.assertEqual(
                    req.headers["Authorization"], "Bearer fake-token"
                )

        mock_get.assert_called_once()
        args, kwargs = mock_get.call_args
        self.assertEqual(
            args[0],
            "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity",
        )
        self.assertEqual(kwargs["params"]["audience"], "http://audience")

    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_gcp_metadata_async_auth_caches_token(self, mock_get) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "fake-token"
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        auth = GCPMetadataAsyncAuth(audience="http://audience")

        # First request: should call fetch
        req1 = MagicMock(headers={})
        async for req in auth.async_auth_flow(req1):
            self.assertEqual(req.headers["Authorization"], "Bearer fake-token")

        self.assertEqual(mock_get.call_count, 1)

        # Second request: should use cache
        req2 = MagicMock(headers={})
        async for req in auth.async_auth_flow(req2):
            self.assertEqual(req.headers["Authorization"], "Bearer fake-token")

        self.assertEqual(mock_get.call_count, 1)

    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_gcp_metadata_async_auth_expires_and_refreshes(
        self, mock_get
    ) -> None:
        mock_response1 = MagicMock(text="fake-token-1")
        mock_response1.raise_for_status.return_value = None
        mock_response2 = MagicMock(text="fake-token-2")
        mock_response2.raise_for_status.return_value = None
        mock_get.side_effect = [mock_response1, mock_response2]

        auth = GCPMetadataAsyncAuth(audience="http://audience")

        # Fetch first token
        req1 = MagicMock(headers={})
        async for req in auth.async_auth_flow(req1):
            self.assertEqual(
                req.headers["Authorization"], "Bearer fake-token-1"
            )

        # Mock time moving forward past TTL (2700s)
        with patch("time.monotonic", return_value=time.monotonic() + 2701):
            req2 = MagicMock(headers={})
            async for req in auth.async_auth_flow(req2):
                self.assertEqual(
                    req.headers["Authorization"], "Bearer fake-token-2"
                )

        self.assertEqual(mock_get.call_count, 2)

    @patch("httpx.AsyncClient.get", new_callable=AsyncMock)
    async def test_concurrent_async_auth_requests_are_serialized(
        self, mock_get
    ) -> None:
        # Simulate a slow network fetch
        async def slow_get(*args, **kwargs):
            await asyncio.sleep(0.1)
            mock_response = MagicMock(text="slow-token")
            mock_response.raise_for_status.return_value = None
            return mock_response

        mock_get.side_effect = slow_get

        auth = GCPMetadataAsyncAuth(audience="http://audience")

        # Start 5 concurrent tasks requesting auth flow
        async def run_flow(task_id: int):
            req = MagicMock(headers={})
            async for r in auth.async_auth_flow(req):
                return r.headers["Authorization"]

        results = await asyncio.gather(*[run_flow(i) for i in range(5)])

        self.assertEqual(set(results), {"Bearer slow-token"})
        self.assertEqual(mock_get.call_count, 1)

    async def test_add_audio_segment_annotation_propagates_exception(
        self,
    ) -> None:
        self.mock_post.side_effect = Exception("Network error")

        with self.assertRaises(Exception):
            await self.client.add_audio_segment_annotation(
                audio_segment_id="segment-id-123",
                annotation_type=AnnotationType.EVALUATION,
                data={"key": "val"},
            )

    async def test_add_audio_segment_success(self) -> None:
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        self.mock_post.return_value = mock_response

        await self.client.add_audio_segment(self.segment_payload)

        self.mock_post.assert_called_once()
        args, kwargs = self.mock_post.call_args
        self.assertEqual(args[0], "http://test-api.com/v1/audio_segments")
        self.assertIn("json", kwargs)
        self.assertEqual(kwargs["json"], self.segment_payload)

    async def test_add_audio_segment_propagates_exception(self) -> None:
        self.mock_post.side_effect = Exception("Network error")

        with self.assertRaises(Exception):
            await self.client.add_audio_segment(self.segment_payload)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_add_audio_segment_annotation_retries_on_transient_error(
        self, mock_sleep
    ) -> None:
        self.mock_post.side_effect = httpx.ConnectError("Connection failed")

        with self.assertRaises(httpx.ConnectError):
            await self.client.add_audio_segment_annotation(
                audio_segment_id="segment-id-123",
                annotation_type=AnnotationType.EVALUATION,
                data={"key": "val"},
            )

        self.assertEqual(self.mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)

    @patch("asyncio.sleep", new_callable=AsyncMock)
    async def test_add_audio_segment_retries_on_transient_error(
        self, mock_sleep
    ) -> None:
        self.mock_post.side_effect = httpx.HTTPStatusError(
            "Too Many Requests",
            request=MagicMock(spec=httpx.Request),
            response=MagicMock(status_code=429, spec=httpx.Response),
        )

        with self.assertRaises(httpx.HTTPStatusError):
            await self.client.add_audio_segment(self.segment_payload)

        self.assertEqual(self.mock_post.call_count, 3)
        self.assertEqual(mock_sleep.call_count, 2)


if __name__ == "__main__":
    unittest.main()
