"""Unit tests for the typed exception hierarchy (Phase 2 foundation)."""
from __future__ import annotations

import pytest

from backend.pipeline.ingestion.exceptions import FeedError, PipelineError, SourceError


class TestSourceError:
    def test_reason_attribute(self) -> None:
        err = SourceError("auth_failed")
        assert err.reason == "auth_failed"

    def test_reason_matches_first_arg(self) -> None:
        err = SourceError("source_unreachable")
        assert err.reason == "source_unreachable"

    def test_is_feed_error_subclass(self) -> None:
        assert isinstance(SourceError("x"), FeedError)

    def test_is_exception_subclass(self) -> None:
        assert isinstance(SourceError("x"), Exception)

    def test_chained_cause_preserved(self) -> None:
        original = ValueError("original error")
        try:
            raise SourceError("auth_failed") from original
        except SourceError as err:
            assert err.__cause__ is original

    def test_str_representation(self) -> None:
        err = SourceError("auth_failed")
        assert "auth_failed" in str(err)


class TestPipelineError:
    def test_reason_attribute(self) -> None:
        err = PipelineError("publish_schema_validation")
        assert err.reason == "publish_schema_validation"

    def test_reason_matches_first_arg(self) -> None:
        err = PipelineError("gcs_upload")
        assert err.reason == "gcs_upload"

    def test_is_feed_error_subclass(self) -> None:
        assert isinstance(PipelineError("x"), FeedError)

    def test_is_exception_subclass(self) -> None:
        assert isinstance(PipelineError("x"), Exception)

    def test_chained_cause_preserved(self) -> None:
        original = RuntimeError("publish failed")
        try:
            raise PipelineError("publish_other") from original
        except PipelineError as err:
            assert err.__cause__ is original

    def test_str_representation(self) -> None:
        err = PipelineError("publish_schema_validation")
        assert "publish_schema_validation" in str(err)


class TestFeedErrorHierarchy:
    def test_source_error_not_pipeline_error(self) -> None:
        assert not isinstance(SourceError("x"), PipelineError)

    def test_pipeline_error_not_source_error(self) -> None:
        assert not isinstance(PipelineError("x"), SourceError)

    def test_feed_error_is_exception(self) -> None:
        assert issubclass(FeedError, Exception)

    def test_can_catch_both_as_feed_error(self) -> None:
        errors = [SourceError("auth_failed"), PipelineError("gcs_upload")]
        for err in errors:
            assert isinstance(err, FeedError)

    def test_exports_all_three(self) -> None:
        from backend.pipeline.ingestion import exceptions

        assert hasattr(exceptions, "FeedError")
        assert hasattr(exceptions, "SourceError")
        assert hasattr(exceptions, "PipelineError")
