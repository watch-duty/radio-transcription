from __future__ import annotations

from fastapi import FastAPI, HTTPException, status

from backend.pipeline.common.rules.models import Rule, RuleCreate, RuleUpdate

from .service import rules_service

app = FastAPI(
    title="Rules Management Service",
    description="API for creating, reading, updating, and deleting transcription rules.",
    version="1.0.0",
)


@app.post(
    "/v1/rules",
    response_model=Rule,
    status_code=status.HTTP_201_CREATED,
    tags=["rules"],
)
async def create_rule(rule_in: RuleCreate) -> Rule:
    """Create a new transcription rule."""
    return rules_service.create_rule(rule_in)


@app.get(
    "/v1/rules",
    response_model=list[Rule],
    tags=["rules"],
)
async def list_rules() -> list[Rule]:
    """List all transcription rules."""
    return rules_service.list_rules()


@app.get(
    "/v1/rules/{rule_id}",
    response_model=Rule,
    tags=["rules"],
)
async def get_rule(rule_id: str) -> Rule:
    """Fetch a specific transcription rule by ID."""
    rule = rules_service.get_rule(rule_id)
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
    rule = rules_service.update_rule(rule_id, rule_in)
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
    success = rules_service.delete_rule(rule_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Rule {rule_id} not found",
        )
