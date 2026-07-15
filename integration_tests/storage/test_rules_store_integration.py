from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import asyncpg

import pytest

from backend.pipeline.common.rules.models import (
    EvaluationType,
    KeywordConditions,
    LogicalOperator,
    RuleCreate,
    RuleMetadata,
    RuleUpdate,
    Scope,
    ScopeLevel,
    Tag,
)
from backend.pipeline.storage.rules_store import RulesStore


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> RulesStore:
    """Provides a RulesStore instance with a clean database."""
    await db_pool.execute("TRUNCATE rules CASCADE")
    return RulesStore(db_pool)


def _create_sample_rule_in(
    name: str = "Test Rule", tags: list[Tag] | None = None
) -> RuleCreate:
    return RuleCreate(
        rule_name=name,
        description="A test rule",
        is_active=True,
        scope=Scope(level=ScopeLevel.FEED_SPECIFIC, target_feeds=["feed-1"]),
        conditions=KeywordConditions(
            evaluation_type=EvaluationType.KEYWORD_MATCH,
            keywords=["fire", "smoke"],
            operator=LogicalOperator.ANY,
        ),
        tags=tags or [],
        metadata=RuleMetadata(created_by="test-user@example.com"),
    )


async def test_create_and_get_rule(store: RulesStore) -> None:
    """Verify adding and retrieving a rule."""
    rule_in = _create_sample_rule_in("Fire Alert")

    # 1. Create
    created = await store.create_rule(rule_in)
    assert created.rule_name == "Fire Alert"
    assert created.rule_id is not None
    assert created.metadata is not None
    assert created.metadata.created_at is not None

    # 2. Get
    fetched = await store.get_rule(created.rule_id)
    assert fetched is not None
    assert fetched.rule_id == created.rule_id
    assert fetched.rule_name == "Fire Alert"
    assert fetched.scope.target_feeds == ["feed-1"]
    assert isinstance(fetched.conditions, KeywordConditions)
    assert fetched.conditions.keywords == ["fire", "smoke"]
    # Rules created without tags round-trip as an empty list
    assert created.tags == []
    assert fetched.tags == []


async def test_list_rules(store: RulesStore) -> None:
    """Verify listing multiple rules."""
    await store.create_rule(_create_sample_rule_in("Rule 1"))
    await store.create_rule(_create_sample_rule_in("Rule 2"))

    rules = await store.list_rules()
    assert len(rules) == 2
    names = {r.rule_name for r in rules}
    assert "Rule 1" in names
    assert "Rule 2" in names


async def test_list_rules_with_ids(store: RulesStore) -> None:
    """Verify listing rules filtered by IDs."""
    r1 = await store.create_rule(_create_sample_rule_in("Rule 1"))
    r2 = await store.create_rule(_create_sample_rule_in("Rule 2"))
    await store.create_rule(_create_sample_rule_in("Rule 3"))

    rules = await store.list_rules(rule_ids=[r1.rule_id, r2.rule_id])
    assert len(rules) == 2
    ids = {r.rule_id for r in rules}
    assert r1.rule_id in ids
    assert r2.rule_id in ids

    names = {r.rule_name for r in rules}
    assert "Rule 1" in names
    assert "Rule 2" in names
    assert "Rule 3" not in names


async def test_update_rule(store: RulesStore) -> None:
    """Verify partial updates to a rule."""
    created = await store.create_rule(_create_sample_rule_in("Old Name"))

    update_in = RuleUpdate(
        rule_name="New Name",
        is_active=False,
        conditions=KeywordConditions(
            evaluation_type=EvaluationType.KEYWORD_MATCH, keywords=["water"]
        ),
    )

    updated = await store.update_rule(created.rule_id, update_in)
    assert updated is not None
    assert updated.rule_name == "New Name"
    assert not updated.is_active
    assert isinstance(updated.conditions, KeywordConditions)
    assert updated.conditions.keywords == ["water"]

    # Scope should remain unchanged (COALESCE logic)
    assert updated.scope.target_feeds == ["feed-1"]


async def test_rule_tags_round_trip(store: RulesStore) -> None:
    """Verify tags persist on create, survive fetch, and can be updated."""
    created = await store.create_rule(
        _create_sample_rule_in(
            "Tagged Rule", tags=[Tag(key="geo_event_type", value="flooding")]
        )
    )
    assert created.tags == [Tag(key="geo_event_type", value="flooding")]

    fetched = await store.get_rule(created.rule_id)
    assert fetched is not None
    assert fetched.tags == [Tag(key="geo_event_type", value="flooding")]

    updated = await store.update_rule(
        created.rule_id,
        RuleUpdate(tags=[Tag(key="geo_event_type", value="wildfire")]),
    )
    assert updated is not None
    assert updated.tags == [Tag(key="geo_event_type", value="wildfire")]
    # Other fields remain unchanged by a tags-only update
    assert updated.rule_name == "Tagged Rule"
    assert updated.scope.target_feeds == ["feed-1"]


async def test_delete_rule(store: RulesStore) -> None:
    """Verify rule deletion."""
    created = await store.create_rule(_create_sample_rule_in())

    # Delete
    success = await store.delete_rule(created.rule_id)
    assert success is True

    # Verify gone
    fetched = await store.get_rule(created.rule_id)
    assert fetched is None

    # Delete non-existent
    success_again = await store.delete_rule(str(uuid.uuid4()))
    assert success_again is False


async def test_invalid_uuid_returns_none_or_false(store: RulesStore) -> None:
    """Verify handling of malformed UUID strings."""
    assert await store.get_rule("not-a-uuid") is None
    assert await store.update_rule("not-a-uuid", RuleUpdate()) is None
    assert await store.delete_rule("not-a-uuid") is False
