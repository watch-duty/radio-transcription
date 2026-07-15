import type { Tag } from './feeds.js';

export type ScopeLevel = 'FEED_SPECIFIC' | 'GLOBAL';
export type EvaluationType = 'KEYWORD_MATCH' | 'REGEX_MATCH' | 'RULE_GROUP';
export type LogicalOperator = 'ANY' | 'ALL';

export interface Scope {
  level: ScopeLevel;
  targetFeeds: string[];
}

export interface KeywordConditions {
  evaluationType: 'KEYWORD_MATCH';
  operator: LogicalOperator;
  keywords: string[];
  caseSensitive: boolean;
}

export interface RegexConditions {
  evaluationType: 'REGEX_MATCH';
  expression: string;
  flags: string;
}

export interface GroupConditions {
  evaluationType: 'RULE_GROUP';
  operator: LogicalOperator;
  childRuleIds: string[];
}

export type RuleConditions =
  | KeywordConditions
  | RegexConditions
  | GroupConditions;

export interface RuleMetadata {
  createdBy?: string;
  createdAt: string;
  updatedAt: string;
}

export interface Rule {
  ruleId: string;
  ruleName: string;
  description?: string;
  isActive: boolean;
  scope: Scope;
  conditions: RuleConditions;
  tags?: Tag[];
  metadata: RuleMetadata;
}

export interface RuleCreate {
  ruleName: string;
  description?: string;
  isActive?: boolean;
  scope: Scope;
  conditions: RuleConditions;
  tags?: Tag[];
}

export interface RuleUpdate {
  ruleName?: string;
  description?: string;
  isActive?: boolean;
  scope?: Scope;
  conditions?: RuleConditions;
  tags?: Tag[];
}
