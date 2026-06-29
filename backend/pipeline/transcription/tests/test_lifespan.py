import asyncio
import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from unittest import mock

from fastapi import FastAPI

from backend.pipeline.transcription import main as transcription_main


class TestLifespanExecutor(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()
        self.original_concurrency = os.environ.get("CONTAINER_CONCURRENCY")
        if "CONTAINER_CONCURRENCY" in os.environ:
            del os.environ["CONTAINER_CONCURRENCY"]

    def tearDown(self) -> None:
        super().tearDown()
        if self.original_concurrency is not None:
            os.environ["CONTAINER_CONCURRENCY"] = self.original_concurrency
        elif "CONTAINER_CONCURRENCY" in os.environ:
            del os.environ["CONTAINER_CONCURRENCY"]

    @mock.patch(
        "backend.pipeline.transcription.main.TranscriptionServiceContainer"
    )
    async def test_lifespan_configures_default_executor(
        self, mock_container_class: mock.MagicMock
    ) -> None:
        # Mock TranscriptionServiceContainer to prevent it from performing real eager warmup or raising errors
        mock_container = mock.MagicMock()
        mock_container.processor = None
        mock_container_class.return_value = mock_container

        app = FastAPI()
        loop: Any = asyncio.get_running_loop()

        # Save existing default executor to restore it later
        old_executor = loop._default_executor

        try:
            async with transcription_main.lifespan(app):
                # Verify that a custom default executor is now set on the loop
                executor = loop._default_executor
                self.assertIsNotNone(executor)
                assert isinstance(executor, ThreadPoolExecutor)
                # By default, CONTAINER_CONCURRENCY is not set, so it should default to 128
                self.assertEqual(executor._max_workers, 128)
                self.assertEqual(
                    executor._thread_name_prefix, "asyncio_default_executor"
                )

            # Verify that the executor is shut down after lifespan context exits
            # ThreadPoolExecutor._shutdown is set to True when shut down
            self.assertTrue(executor._shutdown)
        finally:
            # Restore the original executor
            if old_executor is None:
                loop._default_executor = None
            else:
                loop.set_default_executor(old_executor)

    @mock.patch(
        "backend.pipeline.transcription.main.TranscriptionServiceContainer"
    )
    async def test_lifespan_configures_custom_concurrency(
        self, mock_container_class: mock.MagicMock
    ) -> None:
        mock_container = mock.MagicMock()
        mock_container.processor = None
        mock_container_class.return_value = mock_container

        os.environ["CONTAINER_CONCURRENCY"] = "64"
        app = FastAPI()
        loop: Any = asyncio.get_running_loop()

        old_executor = loop._default_executor

        try:
            async with transcription_main.lifespan(app):
                executor = loop._default_executor
                self.assertIsNotNone(executor)
                assert isinstance(executor, ThreadPoolExecutor)
                self.assertEqual(executor._max_workers, 64)
        finally:
            if old_executor is None:
                loop._default_executor = None
            else:
                loop.set_default_executor(old_executor)

    @mock.patch(
        "backend.pipeline.transcription.main.TranscriptionServiceContainer"
    )
    async def test_lifespan_fallback_on_invalid_concurrency(
        self, mock_container_class: mock.MagicMock
    ) -> None:
        mock_container = mock.MagicMock()
        mock_container.processor = None
        mock_container_class.return_value = mock_container

        os.environ["CONTAINER_CONCURRENCY"] = "invalid_number"
        app = FastAPI()
        loop: Any = asyncio.get_running_loop()

        old_executor = loop._default_executor

        try:
            async with transcription_main.lifespan(app):
                executor = loop._default_executor
                self.assertIsNotNone(executor)
                assert isinstance(executor, ThreadPoolExecutor)
                self.assertEqual(executor._max_workers, 128)
        finally:
            if old_executor is None:
                loop._default_executor = None
            else:
                loop.set_default_executor(old_executor)

    @mock.patch(
        "backend.pipeline.transcription.main.TranscriptionServiceContainer"
    )
    async def test_lifespan_fallback_on_negative_or_zero_concurrency(
        self, mock_container_class: mock.MagicMock
    ) -> None:
        mock_container = mock.MagicMock()
        mock_container.processor = None
        mock_container_class.return_value = mock_container

        for invalid_val in ["0", "-10", "-1"]:
            os.environ["CONTAINER_CONCURRENCY"] = invalid_val
            app = FastAPI()
            loop: Any = asyncio.get_running_loop()

            old_executor = loop._default_executor

            try:
                async with transcription_main.lifespan(app):
                    executor = loop._default_executor
                    self.assertIsNotNone(executor)
                    assert isinstance(executor, ThreadPoolExecutor)
                    self.assertEqual(executor._max_workers, 128)
            finally:
                if old_executor is None:
                    loop._default_executor = None
                else:
                    loop.set_default_executor(old_executor)
