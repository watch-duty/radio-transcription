from __future__ import annotations

import asyncio
import unittest
import uuid
from pathlib import Path
from datetime import datetime, timezone

import asyncpg
import docker
from testcontainers.postgres import PostgresContainer

from backend.pipeline.storage.connection import create_pool
from backend.pipeline.storage.rules_store import RulesStore
from backend.pipeline.common.rules.models import (
    RuleCreate,
    Scope,
    KeywordConditions,
    RuleUpdate,
    ScopeLevel,
    LogicalOperator,
    EvaluationType,
)

_REPO_ROOT = Path(__file__).resolve().parents[4]
_SQL_DIR = _REPO_ROOT / "terraform" / "modules" / "alloydb" / "sql" / "ingestion"


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
    except Exception:
        return False
    return True


@unittest.skipUnless(_docker_available(), "Docker is not available")
class TestRulesStoreIntegration(unittest.IsolatedAsyncioTestCase):
    """Integration tests for RulesStore against AlloyDB Omni."""

    container: PostgresContainer
    pool: asyncpg.Pool

    @classmethod
    def setUpClass(cls) -> None:
        """Start AlloyDB Omni container and apply schema."""
        cls.container = PostgresContainer(
            image="google/alloydbomni:15",
            username="postgres",
            password="postgres",
            dbname="postgres",
            driver=None,
        )
        cls.container.start()

        cls._host = cls.container.get_container_host_ip()
        cls._port = int(cls.container.get_exposed_port(5432))

        async def _setup_schema() -> None:
            conn = await asyncpg.connect(
                host=cls._host,
                port=cls._port,
                user="postgres",
                password="postgres",
                database="postgres",
            )
            # Apply schema files in order
            sql_files = sorted(_SQL_DIR.glob("*.sql"))
            for sql_file in sql_files:
                await conn.execute(sql_file.read_text())
            await conn.close()

        asyncio.run(_setup_schema())

    @classmethod
    def tearDownClass(cls) -> None:
        """Stop container."""
        cls.container.stop()

    async def asyncSetUp(self) -> None:
        """Create pool, truncate rules, and set up store."""
        self.pool = await create_pool(
            host=self._host,
            port=self._port,
            user="postgres",
            password="postgres",
            db_name="postgres",
            min_size=2,
            max_size=5,
        )
        await self.pool.execute("TRUNCATE rules CASCADE")
        self.store = RulesStore(self.pool)

    async def asyncTearDown(self) -> None:
        """Close pool."""
        await self.pool.close()

    def _create_sample_rule_in(self, name: str = "Test Rule") -> RuleCreate:
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
        )

    async def test_create_and_get_rule(self) -> None:
        """Verify adding and retrieving a rule."""
        rule_in = self._create_sample_rule_in("Fire Alert")

        # 1. Create
        created = await self.store.create_rule(rule_in)
        self.assertEqual(created.rule_name, "Fire Alert")
        self.assertIsNotNone(created.rule_id)
        self.assertIsNotNone(created.metadata.created_at)

        # 2. Get
        fetched = await self.store.get_rule(created.rule_id)
        self.assertIsNotNone(fetched)
        self.assertEqual(fetched.rule_id, created.rule_id)
        self.assertEqual(fetched.rule_name, "Fire Alert")
        self.assertEqual(fetched.scope.target_feeds, ["feed-1"])
        self.assertEqual(fetched.conditions.keywords, ["fire", "smoke"])

    async def test_list_rules(self) -> None:
        """Verify listing multiple rules."""
        await self.store.create_rule(self._create_sample_rule_in("Rule 1"))
        await self.store.create_rule(self._create_sample_rule_in("Rule 2"))

        rules = await self.store.list_rules()
        self.assertEqual(len(rules), 2)
        names = {r.rule_name for r in rules}
        self.assertIn("Rule 1", names)
        self.assertIn("Rule 2", names)

    async def test_update_rule(self) -> None:
        """Verify partial updates to a rule."""
        created = await self.store.create_rule(self._create_sample_rule_in("Old Name"))

        update_in = RuleUpdate(
            rule_name="New Name",
            is_active=False,
            conditions=KeywordConditions(
                evaluation_type=EvaluationType.KEYWORD_MATCH, keywords=["water"]
            ),
        )

        updated = await self.store.update_rule(created.rule_id, update_in)
        self.assertIsNotNone(updated)
        self.assertEqual(updated.rule_name, "New Name")
        self.assertFalse(updated.is_active)
        self.assertEqual(updated.conditions.keywords, ["water"])

        # Scope should remain unchanged (COALESCE logic)
        self.assertEqual(updated.scope.target_feeds, ["feed-1"])

    async def test_delete_rule(self) -> None:
        """Verify rule deletion."""
        created = await self.store.create_rule(self._create_sample_rule_in())

        # Delete
        success = await self.store.delete_rule(created.rule_id)
        self.assertTrue(success)

        # Verify gone
        fetched = await self.store.get_rule(created.rule_id)
        self.assertIsNone(fetched)

        # Delete non-existent
        success_again = await self.store.delete_rule(str(uuid.uuid4()))
        self.assertFalse(success_again)

    async def test_invalid_uuid_returns_none_or_false(self) -> None:
        """Verify handling of malformed UUID strings."""
        self.assertIsNone(await self.store.get_rule("not-a-uuid"))
        self.assertIsNone(await self.store.update_rule("not-a-uuid", RuleUpdate()))
        self.assertFalse(await self.store.delete_rule("not-a-uuid"))


if __name__ == "__main__":
    unittest.main()
