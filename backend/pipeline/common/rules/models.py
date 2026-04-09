from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field


class ScopeLevel(StrEnum):
    """Defines the scope of a rule, whether it's global or feed-specific."""

    FEED_SPECIFIC = "FEED_SPECIFIC"
    GLOBAL = "GLOBAL"


class EvaluationType(StrEnum):
    """Specifies the type of evaluation to be performed for a rule."""

    KEYWORD_MATCH = "KEYWORD_MATCH"
    REGEX_MATCH = "REGEX_MATCH"


class LogicalOperator(StrEnum):
    """Defines the logical operator to be used when evaluating multiple conditions."""

    ANY = "ANY"
    ALL = "ALL"


class Scope(BaseModel):
    """Represents the scope of a rule, including its level and target feeds."""

    level: ScopeLevel
    target_feeds: list[str] = Field(default_factory=list)


class KeywordConditions(BaseModel):
    """Defines the conditions for a keyword-based rule."""

    evaluation_type: Literal[EvaluationType.KEYWORD_MATCH] = (
        EvaluationType.KEYWORD_MATCH
    )
    operator: LogicalOperator = LogicalOperator.ANY
    keywords: list[str]
    case_sensitive: bool = False


class RegexConditions(BaseModel):
    """Defines the conditions for a regex-based rule."""

    evaluation_type: Literal[EvaluationType.REGEX_MATCH] = (
        EvaluationType.REGEX_MATCH
    )
    expression: str
    flags: str = "i"


RuleConditions = Annotated[
    Union[KeywordConditions, RegexConditions],
    Field(discriminator="evaluation_type"),
]


class RuleMetadata(BaseModel):
    """Contains metadata about a rule, such as creation and update timestamps."""

    created_by: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RuleBase(BaseModel):
    """Base model for a rule."""

    rule_name: str
    description: str | None = None
    conditions: RuleConditions
    metadata: RuleMetadata = Field(default_factory=RuleMetadata)


class RuleCreate(RuleBase):
    """Model for creating a new rule."""


class RuleUpdate(BaseModel):
    """Model for updating an existing rule."""

    rule_name: str | None = None
    description: str | None = None
    conditions: RuleConditions | None = None


class Rule(RuleBase):
    """Represents a rule as stored in the database."""

    rule_id: str

    model_config = ConfigDict(from_attributes=True)


class RuleGroupBase(BaseModel):
    """Base model for a rule group."""

    rule_name: str
    description: str | None = None
    is_active: bool = True
    scope: Scope
    operator: LogicalOperator = LogicalOperator.ANY
    child_rule_ids: list[str]
    metadata: RuleMetadata = Field(default_factory=RuleMetadata)


class RuleGroupCreate(RuleGroupBase):
    """Model for creating a new rule group."""


class RuleGroupUpdate(BaseModel):
    """Model for updating an existing rule group."""

    rule_name: str | None = None
    description: str | None = None
    is_active: bool | None = None
    scope: Scope | None = None
    operator: LogicalOperator | None = None
    child_rule_ids: list[str] | None = None


class RuleGroup(RuleGroupBase):
    """Represents a rule group as stored in the database."""

    rule_id: str

    model_config = ConfigDict(from_attributes=True)


class ResolvedRuleGroup(RuleGroup):
    """Represents a rule group with its child rules fully resolved."""

    child_rules: list[Rule]
