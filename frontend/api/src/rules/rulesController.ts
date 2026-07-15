import type {
  LogicalOperator,
  Rule,
  RuleCreate,
  RuleUpdate,
  ScopeLevel,
  Tag,
} from '@transcription/common';
import {
  Body,
  Controller,
  Delete,
  Extension,
  Get,
  Path,
  Post,
  Put,
  Queries,
  Request,
  Response,
  Route,
  Security,
  SuccessResponse,
  Tags,
} from 'tsoa';

import { AuthenticatedRequest } from '../authentication.js';
import { RULES_API_URL } from '../config.js';
import {
  HttpError,
  getServiceClient,
  handleBackendError,
  toCamel,
} from '../utils.js';

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
  tags?: Tag[];
  metadata: RuleMetadataResponse;
}

interface RuleCreateBackend {
  rule_name: string;
  description?: string;
  is_active?: boolean;
  scope: ScopeResponse;
  conditions: RuleConditionsResponse;
  tags?: Tag[];
}

type RuleUpdateBackend = Partial<RuleCreateBackend>;

function convertRuleResponse(response: RuleResponse): Rule {
  return toCamel<Rule>(response);
}

function convertRuleCreate(create: RuleCreate): RuleCreateBackend {
  let conditions: RuleConditionsResponse;
  switch (create.conditions.evaluationType) {
    case 'KEYWORD_MATCH':
      conditions = {
        evaluation_type: 'KEYWORD_MATCH',
        operator: create.conditions.operator,
        keywords: create.conditions.keywords,
        case_sensitive: create.conditions.caseSensitive,
      };
      break;
    case 'REGEX_MATCH':
      conditions = {
        evaluation_type: 'REGEX_MATCH',
        expression: create.conditions.expression,
        flags: create.conditions.flags,
      };
      break;
    case 'RULE_GROUP':
      conditions = {
        evaluation_type: 'RULE_GROUP',
        operator: create.conditions.operator,
        child_rule_ids: create.conditions.childRuleIds,
      };
      break;
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
    tags: create.tags,
  };
}

function convertRuleUpdate(update: RuleUpdate): RuleUpdateBackend {
  const result: RuleUpdateBackend = {};
  if (update.ruleName !== undefined) result.rule_name = update.ruleName;
  if (update.description !== undefined) result.description = update.description;
  if (update.isActive !== undefined) result.is_active = update.isActive;
  if (update.scope !== undefined) {
    result.scope = {
      level: update.scope.level,
      target_feeds: update.scope.targetFeeds,
    };
  }
  if (update.conditions !== undefined) {
    let conditions: RuleConditionsResponse;
    switch (update.conditions.evaluationType) {
      case 'KEYWORD_MATCH':
        conditions = {
          evaluation_type: 'KEYWORD_MATCH',
          operator: update.conditions.operator,
          keywords: update.conditions.keywords,
          case_sensitive: update.conditions.caseSensitive,
        };
        break;
      case 'REGEX_MATCH':
        conditions = {
          evaluation_type: 'REGEX_MATCH',
          expression: update.conditions.expression,
          flags: update.conditions.flags,
        };
        break;
      case 'RULE_GROUP':
        conditions = {
          evaluation_type: 'RULE_GROUP',
          operator: update.conditions.operator,
          child_rule_ids: update.conditions.childRuleIds,
        };
        break;
    }
    result.conditions = conditions;
  }
  if (update.tags !== undefined) result.tags = update.tags;
  return result;
}

export class ListRulesQueryParams {
  /**
   * Optional list of rule IDs to filter by.
   */
  ruleIds?: string[];
}

@Route('api/v1/rules')
@Tags('Rules')
@Response(401, 'Unauthorized')
export class RulesController extends Controller {
  private async getClient() {
    return await getServiceClient(RULES_API_URL);
  }

  @Get('')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listRules(
    @Queries() query: ListRulesQueryParams
  ): Promise<Rule[]> {
    try {
      const client = await this.getClient();
      const params = new URLSearchParams();
      if (query.ruleIds) {
        query.ruleIds.forEach((id) => params.append('rule_ids', id));
      }

      const response = await client.request({
        url: RULES_API_URL,
        method: 'GET',
        params: params,
      });

      const data = response.data as RuleResponse[];
      return data.map(convertRuleResponse);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(error, 'fetching rules');
      throw new HttpError(status, message);
    }
  }

  @Get('{ruleId}')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getRule(@Path() ruleId: string): Promise<Rule> {
    try {
      const client = await this.getClient();
      const response = await client.request({
        url: `${RULES_API_URL}/${ruleId}`,
        method: 'GET',
      });
      return convertRuleResponse(response.data as RuleResponse);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `fetching rule ${ruleId}`
      );
      throw new HttpError(status, message);
    }
  }

  @Post('')
  @Security('google_id_token')
  @SuccessResponse('201', 'Created')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async createRule(
    @Request() request: AuthenticatedRequest,
    @Body() requestBody: RuleCreate
  ): Promise<Rule> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }
    try {
      const client = await this.getClient();
      const response = await client.request({
        url: RULES_API_URL,
        method: 'POST',
        data: convertRuleCreate(requestBody),
      });
      return convertRuleResponse(response.data as RuleResponse);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(error, 'creating rule');
      throw new HttpError(status, message);
    }
  }

  @Put('{ruleId}')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async updateRule(
    @Request() request: AuthenticatedRequest,
    @Path() ruleId: string,
    @Body() requestBody: RuleUpdate
  ): Promise<Rule> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }
    try {
      const client = await this.getClient();
      const response = await client.request({
        url: `${RULES_API_URL}/${ruleId}`,
        method: 'PUT',
        data: convertRuleUpdate(requestBody),
      });
      return convertRuleResponse(response.data as RuleResponse);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `updating rule ${ruleId}`
      );
      throw new HttpError(status, message);
    }
  }

  @Delete('{ruleId}')
  @Security('google_id_token')
  @SuccessResponse('204', 'No Content')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async deleteRule(
    @Request() request: AuthenticatedRequest,
    @Path() ruleId: string
  ): Promise<void> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }
    try {
      const client = await this.getClient();
      await client.request({
        url: `${RULES_API_URL}/${ruleId}`,
        method: 'DELETE',
      });
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `deleting rule ${ruleId}`
      );
      throw new HttpError(status, message);
    }
  }
}
