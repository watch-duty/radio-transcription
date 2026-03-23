from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Request, status

from backend.pipeline.common.auth import verify_oidc_token
from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate
from backend.pipeline.storage.connection import close_pool, create_pool_from_settings
from backend.pipeline.storage.rules_store import RulesStore

from .service import AlloyRulesService, BaseRulesService


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the lifecycle of the AlloyDB connection pool."""
    pool = await create_pool_from_settings()
    store = RulesStore(pool)
    app.state.rules_service = AlloyRulesService(store)
    yield
    await close_pool(pool)


app = FastAPI(
    title="Rules Management Service",
    description="API for creating, reading, updating, and deleting transcription rules.",
    version="1.0.0",
    lifespan=lifespan,
    dependencies=[Depends(verify_oidc_token)],
)


def get_rules_service(request: Request) -> BaseRulesService:
    """Dependency that retrieves the rules service from the application state."""
    return request.app.state.rules_service


@app.post(
    "/v1/rules",
    response_model=Rule,
    status_code=status.HTTP_201_CREATED,
    tags=["rules"],
)
async def create_rule(
    rule_in: RuleCreate, service: BaseRulesService = Depends(get_rules_service)
) -> Rule:
    """Create a new transcription rule."""
    return await service.create_rule(rule_in)


@app.get(
    "/v1/rules",
    response_model=list[Rule],
    tags=["rules"],
)
async def list_rules(
    service: BaseRulesService = Depends(get_rules_service),
) -> list[Rule]:
    """List all transcription rules."""
    return await service.list_rules()


@app.get(
    "/v1/rules/{rule_id}",
    response_model=Rule,
    tags=["rules"],
)
async def get_rule(
    rule_id: str, service: BaseRulesService = Depends(get_rules_service)
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
    service: BaseRulesService = Depends(get_rules_service),
) -> Rule:
    """Fully update an existing transcription rule."""
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
    rule_id: str, service: BaseRulesService = Depends(get_rules_service)
) -> None:
    """Delete a transcription rule."""
    success = await service.delete_rule(rule_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule {rule_id} not found",
        )
