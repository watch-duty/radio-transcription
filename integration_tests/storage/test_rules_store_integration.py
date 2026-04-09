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
    RuleGroupCreate,
    RuleGroupUpdate,
    RuleMetadata,
    RuleUpdate,
    Scope,
    ScopeLevel,
)
from backend.pipeline.storage.rules_store import RulesStore


@pytest.fixture
async def store(db_pool: asyncpg.Pool) -> RulesStore:
    """Provides a RulesStore instance with a clean database."""
    await db_pool.execute("TRUNCATE rules, rule_groups CASCADE")
    return RulesStore(db_pool)


def _create_sample_rule_in(name: str = "Test Rule") -> RuleCreate:
    return RuleCreate(
        rule_name=name,
        description="A test rule",
        conditions=KeywordConditions(
            evaluation_type=EvaluationType.KEYWORD_MATCH,
            keywords=["fire", "smoke"],
            operator=LogicalOperator.ANY,
        ),
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
    assert isinstance(fetched.conditions, KeywordConditions)
    assert fetched.conditions.keywords == ["fire", "smoke"]


async def test_list_rules(store: RulesStore) -> None:
    """Verify listing multiple rules."""
    await store.create_rule(_create_sample_rule_in("Rule 1"))
    await store.create_rule(_create_sample_rule_in("Rule 2"))

    rules = await store.list_rules()
    assert len(rules) == 2
    names = {r.rule_name for r in rules}
    assert "Rule 1" in names
    assert "Rule 2" in names


async def test_update_rule(store: RulesStore) -> None:
    """Verify partial updates to a rule."""
    created = await store.create_rule(_create_sample_rule_in("Old Name"))

    update_in = RuleUpdate(
        rule_name="New Name",
        conditions=KeywordConditions(
            evaluation_type=EvaluationType.KEYWORD_MATCH, keywords=["water"]
        ),
    )

    updated = await store.update_rule(created.rule_id, update_in)
    assert updated is not None
    assert updated.rule_name == "New Name"
    assert isinstance(updated.conditions, KeywordConditions)
    assert updated.conditions.keywords == ["water"]


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


def _create_sample_group_in(
    name: str = "Test Group", child_ids: list[str] | None = None
) -> RuleGroupCreate:
    if child_ids is None:
        child_ids = []
    return RuleGroupCreate(
        rule_name=name,
        description="A test group",
        is_active=True,
        scope=Scope(level=ScopeLevel.GLOBAL),
        operator=LogicalOperator.ANY,
        child_rule_ids=child_ids,
        metadata=RuleMetadata(created_by="test-user@example.com"),
    )


async def test_create_and_get_group(store: RulesStore) -> None:
    """Verify adding and retrieving a rule group."""
    group_in = _create_sample_group_in("Fire Group")

    # 1. Create
    created = await store.create_rule_group(group_in)
    assert created.rule_name == "Fire Group"
    assert created.rule_id is not None

    # 2. Get
    fetched = await store.get_rule_group(created.rule_id)
    assert fetched is not None
    assert fetched.rule_id == created.rule_id
    assert fetched.rule_name == "Fire Group"


async def test_list_rule_groups(store: RulesStore) -> None:
    """Verify listing multiple groups."""
    await store.create_rule_group(_create_sample_group_in("Group 1"))
    await store.create_rule_group(_create_sample_group_in("Group 2"))

    groups = await store.list_rule_groups()
    assert len(groups) == 2
    names = {g.rule_name for g in groups}
    assert "Group 1" in names
    assert "Group 2" in names


async def test_update_rule_group(store: RulesStore) -> None:
    """Verify partial updates to a rule group."""
    created = await store.create_rule_group(_create_sample_group_in("Old Name"))

    update_in = RuleGroupUpdate(
        rule_name="New Name",
        is_active=False,
    )

    updated = await store.update_rule_group(created.rule_id, update_in)
    assert updated is not None
    assert updated.rule_name == "New Name"
    assert not updated.is_active


async def test_delete_rule_group(store: RulesStore) -> None:
    """Verify rule group deletion."""
    created = await store.create_rule_group(_create_sample_group_in())

    # Delete
    success = await store.delete_rule_group(created.rule_id)
    assert success is True

    # Verify gone
    fetched = await store.get_rule_group(created.rule_id)
    assert fetched is None


async def test_get_resolved_rule_group(store: RulesStore) -> None:
    """Verify fetching a group with resolved child rules."""
    # 1. Create child rule
    rule = await store.create_rule(_create_sample_rule_in("Child Rule"))

    # 2. Create group referencing the rule
    group = await store.create_rule_group(
        _create_sample_group_in("Parent Group", [rule.rule_id])
    )

    # 3. Get resolved
    resolved = await store.get_resolved_rule_group(group.rule_id)
    assert resolved is not None
    assert resolved.rule_id == group.rule_id
    assert len(resolved.child_rules) == 1
    assert resolved.child_rules[0].rule_id == rule.rule_id
    assert resolved.child_rules[0].rule_name == "Child Rule"


async def test_list_resolved_rule_groups(store: RulesStore) -> None:
    """Verify listing groups with resolved child rules."""
    rule = await store.create_rule(_create_sample_rule_in("Child Rule"))
    await store.create_rule_group(
        _create_sample_group_in("Group 1", [rule.rule_id])
    )
    await store.create_rule_group(_create_sample_group_in("Group 2", []))

    resolved_groups = await store.list_resolved_rule_groups()
    assert len(resolved_groups) == 2

    # Find the one with child rules
    g1 = next(g for g in resolved_groups if g.rule_name == "Group 1")
    assert len(g1.child_rules) == 1
    assert g1.child_rules[0].rule_id == rule.rule_id
