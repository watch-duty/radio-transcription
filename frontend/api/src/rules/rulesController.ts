import type {
  DryRunRequest,
  DryRunResponse,
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
  toSnake,
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

interface TextMatchSpanResponse {
  start: number;
  end: number;
  matched_text: string;
}

interface DryRunMatchExampleResponse {
  audio_segment_id: string;
  feed_id: string;
  text: string;
  matched_spans: TextMatchSpanResponse[];
}

interface DryRunResponseBackend {
  hit_count: number;
  total_evaluated: number;
  examples: DryRunMatchExampleResponse[];
}

function convertRuleResponse(response: RuleResponse): Rule {
  return toCamel<Rule>(response);
}

function convertRuleCreate(create: RuleCreate): RuleCreateBackend {
  return toSnake<RuleCreateBackend>(create);
}

function convertRuleUpdate(update: RuleUpdate): RuleUpdateBackend {
  return toSnake<RuleUpdateBackend>(update);
}

function convertDryRunResponse(
  response: DryRunResponseBackend
): DryRunResponse {
  return {
    hitCount: response.hit_count,
    totalEvaluated: response.total_evaluated,
    examples: response.examples.map((ex) => ({
      audioSegmentId: ex.audio_segment_id,
      feedId: ex.feed_id,
      text: ex.text,
      matchedSpans: ex.matched_spans.map((span) => ({
        startIndex: span.start,
        endIndex: span.end,
        matchedText: span.matched_text,
      })),
    })),
  };
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

  @Post('dry-run')
  @Security('google_id_token')
  @SuccessResponse('200', 'OK')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async dryRunRule(
    @Request() request: AuthenticatedRequest,
    @Body() requestBody: DryRunRequest
  ): Promise<DryRunResponse> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }
    try {
      const client = await this.getClient();
      const response = await client.request({
        url: `${RULES_API_URL}/dry-run`,
        method: 'POST',
        data: toSnake(requestBody),
      });
      return convertDryRunResponse(response.data as DryRunResponseBackend);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(error, 'dry running rule');
      throw new HttpError(status, message);
    }
  }
}
