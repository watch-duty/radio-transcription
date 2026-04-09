from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, Annotated, Any

if TYPE_CHECKING:
    from collections.abc import AsyncIterator

from fastapi import Depends, FastAPI, HTTPException, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.rules.models import (
    ResolvedRuleGroup,
    Rule,
    RuleCreate,
    RuleGroup,
    RuleGroupCreate,
    RuleGroupUpdate,
    RuleUpdate,
)
from backend.pipeline.storage.connection import (
    close_pool,
    create_pool_with_retry,
)
from backend.pipeline.storage.rules_store import RulesStore

from .service import AlloyRulesService, BaseRulesService


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_with_retry()
    store = RulesStore(pool)
    app.state.rules_service = AlloyRulesService(store)
    yield
    await close_pool(pool)


app = FastAPI(
    title="Rules Management Service",
    description="API for creating, reading, updating, and deleting transcription rules and rule groups.",
    version="2.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)


def get_rules_service(request: Request) -> BaseRulesService:
    """Dependency that retrieves the rules service from the application state."""
    return request.app.state.rules_service


# --- Rule Endpoints ---


@app.post(
    "/v1/rules",
    response_model=Rule,
    status_code=status.HTTP_201_CREATED,
    tags=["rules"],
)
async def create_rule(
    rule_in: RuleCreate,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
    user: Annotated[dict[str, Any], Depends(verify_oidc_token)],
) -> Rule:
    """Create a new transcription rule."""
    rule_in.metadata.created_by = user.get("email")
    return await service.create_rule(rule_in)


@app.get(
    "/v1/rules",
    response_model=list[Rule],
    tags=["rules"],
)
async def list_rules(
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> list[Rule]:
    """List all transcription rules."""
    return await service.list_rules()


@app.get(
    "/v1/rules/{rule_id}",
    response_model=Rule,
    tags=["rules"],
)
async def get_rule(
    rule_id: str,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> Rule:
    """Fetch a specific transcription rule by ID."""
    rule = await service.get_rule(rule_id)
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
async def update_rule(
    rule_id: str,
    rule_in: RuleUpdate,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> Rule:
    """Update an existing transcription rule."""
    rule = await service.update_rule(rule_id, rule_in)
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
async def delete_rule(
    rule_id: str,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> None:
    """Delete a transcription rule."""
    success = await service.delete_rule(rule_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule {rule_id} not found",
        )


# --- Rule Group Endpoints ---


@app.post(
    "/v1/groups",
    response_model=RuleGroup,
    status_code=status.HTTP_201_CREATED,
    tags=["groups"],
)
async def create_rule_group(
    group_in: RuleGroupCreate,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
    user: Annotated[dict[str, Any], Depends(verify_oidc_token)],
) -> RuleGroup:
    """Create a new rule group."""
    group_in.metadata.created_by = user.get("email")
    return await service.create_rule_group(group_in)


@app.get(
    "/v1/groups",
    response_model=list[RuleGroup],
    tags=["groups"],
)
async def list_rule_groups(
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> list[RuleGroup]:
    """List all rule groups."""
    return await service.list_rule_groups()


@app.get(
    "/v1/groups/{group_id}",
    response_model=RuleGroup,
    tags=["groups"],
)
async def get_rule_group(
    group_id: str,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> RuleGroup:
    """Fetch a specific rule group by ID."""
    group = await service.get_rule_group(group_id)
    if not group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Group {group_id} not found",
        )
    return group


@app.put(
    "/v1/groups/{group_id}",
    response_model=RuleGroup,
    tags=["groups"],
)
async def update_rule_group(
    group_id: str,
    group_in: RuleGroupUpdate,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> RuleGroup:
    """Update an existing rule group."""
    group = await service.update_rule_group(group_id, group_in)
    if not group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Group {group_id} not found",
        )
    return group


@app.delete(
    "/v1/groups/{group_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    tags=["groups"],
)
async def delete_rule_group(
    group_id: str,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> None:
    """Delete a rule group."""
    success = await service.delete_rule_group(group_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Group {group_id} not found",
        )


@app.get(
    "/v1/groups/resolved",
    response_model=list[ResolvedRuleGroup],
    tags=["groups"],
)
async def list_resolved_rule_groups(
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> list[ResolvedRuleGroup]:
    """List all rule groups with their child rules resolved."""
    return await service.list_resolved_rule_groups()


@app.get(
    "/v1/groups/{group_id}/resolved",
    response_model=ResolvedRuleGroup,
    tags=["groups"],
)
async def get_resolved_rule_group(
    group_id: str,
    service: Annotated[BaseRulesService, Depends(get_rules_service)],
) -> ResolvedRuleGroup:
    """Fetch a rule group with its child rules resolved."""
    resolved_group = await service.get_resolved_rule_group(group_id)
    if not resolved_group:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Group {group_id} not found",
        )
    return resolved_group
