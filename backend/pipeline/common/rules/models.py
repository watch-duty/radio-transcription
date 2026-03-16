from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class ScopeLevel(StrEnum):
    FEED_SPECIFIC = "FEED_SPECIFIC"
    GLOBAL = "GLOBAL"


class EvaluationType(StrEnum):
    KEYWORD_MATCH = "KEYWORD_MATCH"
    REGEX_MATCH = "REGEX_MATCH"
    RULE_GROUP = "RULE_GROUP"


class LogicalOperator(StrEnum):
    ANY = "ANY"
    ALL = "ALL"


class Scope(BaseModel):
    level: ScopeLevel
    target_feeds: list[str] = Field(default_factory=list)


class KeywordConditions(BaseModel):
    evaluation_type: Literal[EvaluationType.KEYWORD_MATCH] = EvaluationType.KEYWORD_MATCH
    operator: LogicalOperator = LogicalOperator.ANY
    keywords: list[str]
    case_sensitive: bool = False


class RegexConditions(BaseModel):
    evaluation_type: Literal[EvaluationType.REGEX_MATCH] = EvaluationType.REGEX_MATCH
    expression: str
    flags: str = "i"


class GroupConditions(BaseModel):
    evaluation_type: Literal[EvaluationType.RULE_GROUP] = EvaluationType.RULE_GROUP
    operator: LogicalOperator = LogicalOperator.ANY
    child_rule_ids: list[str]


RuleConditions = Annotated[
    Union[KeywordConditions, RegexConditions, GroupConditions],
    Field(discriminator="evaluation_type"),
]


class RuleMetadata(BaseModel):
    created_by: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RuleBase(BaseModel):
    rule_name: str
    description: str | None = None
    is_active: bool = True
    scope: Scope
    conditions: RuleConditions
    metadata: RuleMetadata | None = None


class RuleCreate(RuleBase):
    pass


class RuleUpdate(BaseModel):
    rule_name: str | None = None
    description: str | None = None
    is_active: bool | None = None
    scope: Scope | None = None
    conditions: RuleConditions | None = None
    metadata: RuleMetadata | None = None


class Rule(RuleBase):
    rule_id: str

    model_config = ConfigDict(from_attributes=True)
