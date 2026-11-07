import type { Tag } from './feeds.js';
import type { TextMatchSpan } from './audio.js';

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

export interface DryRunMatchExample {
  audioSegmentId: string;
  feedId: string;
  text: string;
  matchedSpans: TextMatchSpan[];
}

export interface DryRunResponse {
  hitCount: number;
  totalEvaluated: number;
  examples: DryRunMatchExample[];
}

export interface DryRunRequest {
  rule: RuleCreate;
  maxExamples?: number;
  feedIds?: string[];
  daysLookback?: number;
}

export interface RuleAuditEvent {
  id: string;
  ruleId: string;
  action: string;
  actorId: string;
  occurredAt: string;
  ruleRevision: number;
  beforeValues: Record<string, unknown>;
  afterValues: Record<string, unknown>;
}

export interface PaginatedRuleAuditEvents {
  auditEvents: RuleAuditEvent[];
  nextToken: string | null;
  total: number;
}
