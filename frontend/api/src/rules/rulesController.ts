import { GoogleAuth } from 'google-auth-library';
import {
  Body,
  Controller,
  Delete,
  Extension,
  Get,
  Path,
  Post,
  Put,
  Res,
  Response,
  Route,
  Security,
  Tags,
  TsoaResponse,
} from 'tsoa';

import { RULES_API_URL } from '../config.js';

// Types for Frontend (CamelCase)
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
  metadata: RuleMetadata;
}

export interface RuleCreate {
  ruleName: string;
  description?: string;
  isActive?: boolean;
  scope: Scope;
  conditions: RuleConditions;
}

export interface RuleUpdate {
  ruleName?: string;
  description?: string;
  isActive?: boolean;
  scope?: Scope;
  conditions?: RuleConditions;
}

// Types for Backend (SnakeCase)
interface ScopeResponse {
  level: ScopeLevel;
  target_feeds: string[];
}

interface KeywordConditionsResponse {
  evaluation_type: 'KEYWORD_MATCH';
  operator: LogicalOperator;
  keywords: string[];
  case_sensitive: boolean;
}

interface RegexConditionsResponse {
  evaluation_type: 'REGEX_MATCH';
  expression: string;
  flags: string;
}

interface GroupConditionsResponse {
  evaluation_type: 'RULE_GROUP';
  operator: LogicalOperator;
  child_rule_ids: string[];
}

type RuleConditionsResponse =
  | KeywordConditionsResponse
  | RegexConditionsResponse
  | GroupConditionsResponse;

interface RuleMetadataResponse {
  created_by?: string;
  created_at: string;
  updated_at: string;
}

interface RuleResponse {
  rule_id: string;
  rule_name: string;
  description?: string;
  is_active: boolean;
  scope: ScopeResponse;
  conditions: RuleConditionsResponse;
  metadata: RuleMetadataResponse;
}

function convertConditions(
  conditions: RuleConditionsResponse
): RuleConditions {
  switch (conditions.evaluation_type) {
    case 'KEYWORD_MATCH':
      return {
        evaluationType: conditions.evaluation_type,
        operator: conditions.operator,
        keywords: conditions.keywords,
        caseSensitive: conditions.case_sensitive,
      };
    case 'REGEX_MATCH':
      return {
        evaluationType: conditions.evaluation_type,
        expression: conditions.expression,
        flags: conditions.flags,
      };
    case 'RULE_GROUP':
      return {
        evaluationType: conditions.evaluation_type,
        operator: conditions.operator,
        childRuleIds: conditions.child_rule_ids,
      };
  }
}

function convertRuleResponse(response: RuleResponse): Rule {
  return {
    ruleId: response.rule_id,
    ruleName: response.rule_name,
    description: response.description,
    isActive: response.is_active,
    scope: {
      level: response.scope.level,
      targetFeeds: response.scope.target_feeds,
    },
    conditions: convertConditions(response.conditions),
    metadata: {
      createdBy: response.metadata.created_by,
      createdAt: response.metadata.created_at,
      updatedAt: response.metadata.updated_at,
    },
  };
}

function convertRuleCreate(create: RuleCreate): any {
  const conditions: any = {
    evaluation_type: create.conditions.evaluationType,
  };
  if (create.conditions.evaluationType === 'KEYWORD_MATCH') {
    conditions.operator = create.conditions.operator;
    conditions.keywords = create.conditions.keywords;
    conditions.case_sensitive = create.conditions.caseSensitive;
  } else if (create.conditions.evaluationType === 'REGEX_MATCH') {
    conditions.expression = create.conditions.expression;
    conditions.flags = create.conditions.flags;
  } else if (create.conditions.evaluationType === 'RULE_GROUP') {
    conditions.operator = create.conditions.operator;
    conditions.child_rule_ids = create.conditions.childRuleIds;
  }

  return {
    rule_name: create.ruleName,
    description: create.description,
    is_active: create.isActive,
    scope: {
      level: create.scope.level,
      target_feeds: create.scope.targetFeeds,
    },
    conditions: conditions,
  };
}

function convertRuleUpdate(update: RuleUpdate): any {
  const result: any = {};
  if (update.ruleName !== undefined) result.rule_name = update.ruleName;
  if (update.description !== undefined)
    result.description = update.description;
  if (update.isActive !== undefined) result.is_active = update.isActive;
  if (update.scope !== undefined) {
    result.scope = {
      level: update.scope.level,
      target_feeds: update.scope.targetFeeds,
    };
  }
  if (update.conditions !== undefined) {
    const conditions: any = {
      evaluation_type: update.conditions.evaluationType,
    };
    if (update.conditions.evaluationType === 'KEYWORD_MATCH') {
      conditions.operator = update.conditions.operator;
      conditions.keywords = update.conditions.keywords;
      conditions.case_sensitive = update.conditions.caseSensitive;
    } else if (update.conditions.evaluationType === 'REGEX_MATCH') {
      conditions.expression = update.conditions.expression;
      conditions.flags = update.conditions.flags;
    } else if (update.conditions.evaluationType === 'RULE_GROUP') {
      conditions.operator = update.conditions.operator;
      conditions.child_rule_ids = update.conditions.childRuleIds;
    }
    result.conditions = conditions;
  }
  return result;
}

@Route('api/v1/rules')
@Tags('Rules')
@Response(401, 'Unauthorized')
export class RulesController extends Controller {
  private async getClient() {
    const auth = new GoogleAuth();
    return await auth.getIdTokenClient(RULES_API_URL!);
  }

  @Get('')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules_management_api')
  public async listRules(): Promise<Rule[]> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: RULES_API_URL!,
        method: 'GET',
      });
      const data = response.data as RuleResponse[];
      return data.map(convertRuleResponse);
    } catch (error: unknown) {
      console.error('Error fetching rules:', error);
      throw new Error('Error fetching rules', { cause: error });
    }
  }

  @Get('{ruleId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules_management_api')
  public async getRule(
    @Path() ruleId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<Rule> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: `${RULES_API_URL}/${ruleId}`,
        method: 'GET',
      });
      return convertRuleResponse(response.data as RuleResponse);
    } catch (error: unknown) {
      if (this.isAxiosError(error) && error.response?.status === 404) {
        return notFound(404, { message: `Rule ${ruleId} not found` });
      }
      console.error(`Error fetching rule ${ruleId}:`, error);
      throw new Error(`Error fetching rule ${ruleId}`, { cause: error });
    }
  }

  @Post('')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules_management_api')
  public async createRule(@Body() requestBody: RuleCreate): Promise<Rule> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: RULES_API_URL!,
        method: 'POST',
        data: convertRuleCreate(requestBody),
      });
      this.setStatus(201);
      return convertRuleResponse(response.data as RuleResponse);
    } catch (error: unknown) {
      console.error('Error creating rule:', error);
      throw new Error('Error creating rule', { cause: error });
    }
  }

  @Put('{ruleId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules_management_api')
  public async updateRule(
    @Path() ruleId: string,
    @Body() requestBody: RuleUpdate,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<Rule> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: `${RULES_API_URL}/${ruleId}`,
        method: 'PUT',
        data: convertRuleUpdate(requestBody),
      });
      return convertRuleResponse(response.data as RuleResponse);
    } catch (error: unknown) {
      if (this.isAxiosError(error) && error.response?.status === 404) {
        return notFound(404, { message: `Rule ${ruleId} not found` });
      }
      console.error(`Error updating rule ${ruleId}:`, error);
      throw new Error(`Error updating rule ${ruleId}`, { cause: error });
    }
  }

  @Delete('{ruleId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules_management_api')
  public async deleteRule(
    @Path() ruleId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<void> {
    const client = await this.getClient();
    try {
      await client.request({
        url: `${RULES_API_URL}/${ruleId}`,
        method: 'DELETE',
      });
      this.setStatus(204);
    } catch (error: unknown) {
      if (this.isAxiosError(error) && error.response?.status === 404) {
        return notFound(404, { message: `Rule ${ruleId} not found` });
      }
      console.error(`Error deleting rule ${ruleId}:`, error);
      throw new Error(`Error deleting rule ${ruleId}`, { cause: error });
    }
  }

  private isAxiosError(
    error: any
  ): error is { response?: { status: number } } {
    return error && typeof error === 'object' && 'response' in error;
  }
}
