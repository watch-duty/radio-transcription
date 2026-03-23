from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate
from backend.pipeline.storage.connection import close_pool, create_pool_from_env
from backend.pipeline.storage.rules_store import RulesStore

from .service import init_rules_service, rules_service


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the lifecycle of the AlloyDB connection pool."""
    use_mock = os.environ.get("USE_MOCK_RULES", "false").lower() == "true"
    
    if not use_mock:
        pool = await create_pool_from_env()
        store = RulesStore(pool)
        init_rules_service(store)
        yield
        await close_pool(pool)
    else:
        yield


app = FastAPI(
    title="Rules Management Service",
    description="API for creating, reading, updating, and deleting transcription rules.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)


@app.post(
    "/v1/rules",
    response_model=Rule,
    status_code=status.HTTP_201_CREATED,
    tags=["rules"],
)
async def create_rule(rule_in: RuleCreate) -> Rule:
    """Create a new transcription rule."""
    return await rules_service.create_rule(rule_in)


@app.get(
    "/v1/rules",
    response_model=list[Rule],
    tags=["rules"],
)
async def list_rules() -> list[Rule]:
    """List all transcription rules."""
    return await rules_service.list_rules()


@app.get(
    "/v1/rules/{rule_id}",
    response_model=Rule,
    tags=["rules"],
)
async def get_rule(rule_id: str) -> Rule:
    """Fetch a specific transcription rule by ID."""
    rule = await rules_service.get_rule(rule_id)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule {rule_id} not found",
        )
    return rule


@app.put(
    "/v1/rules/{rule_id}",
    response_model=Rule,
    tags=["rules"],
)
async def update_rule(rule_id: str, rule_in: RuleUpdate) -> Rule:
    """Fully update an existing transcription rule."""
    rule = await rules_service.update_rule(rule_id, rule_in)
    if not rule:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule {rule_id} not found",
        )
    return rule


@app.delete(
    "/v1/rules/{rule_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["rules"],
)
async def delete_rule(rule_id: str) -> None:
    """Delete a transcription rule."""
    success = await rules_service.delete_rule(rule_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule {rule_id} not found",
        )
