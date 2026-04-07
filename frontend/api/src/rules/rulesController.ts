import axios from 'axios';
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

import { RULES_MANAGEMENT_API_URL } from '../config.js';

export enum ScopeLevel {
  FEED_SPECIFIC = 'FEED_SPECIFIC',
  GLOBAL = 'GLOBAL',
}

export enum EvaluationType {
  KEYWORD_MATCH = 'KEYWORD_MATCH',
  REGEX_MATCH = 'REGEX_MATCH',
  RULE_GROUP = 'RULE_GROUP',
}

export enum LogicalOperator {
  ANY = 'ANY',
  ALL = 'ALL',
}

export interface Scope {
  level: ScopeLevel;
  targetFeeds?: string[];
}

export interface KeywordConditions {
  evaluationType: EvaluationType.KEYWORD_MATCH;
  operator?: LogicalOperator;
  keywords: string[];
  caseSensitive?: boolean;
}

export interface RegexConditions {
  evaluationType: EvaluationType.REGEX_MATCH;
  expression: string;
  flags?: string;
}

export interface GroupConditions {
  evaluationType: EvaluationType.RULE_GROUP;
  operator?: LogicalOperator;
  childRuleIds: string[];
}

export type RuleConditions = KeywordConditions | RegexConditions | GroupConditions;

export interface RuleMetadata {
  createdBy?: string;
  createdAt?: string;
  updatedAt?: string;
}

export interface Rule {
  ruleId: string;
  ruleName: string;
  description?: string;
  isActive?: boolean;
  scope: Scope;
  conditions: RuleConditions;
  metadata?: RuleMetadata;
}

export interface RuleCreate extends Omit<Rule, 'ruleId'> {}

export interface RuleUpdate {
  ruleName?: string;
  description?: string;
  isActive?: boolean;
  scope?: Scope;
  conditions?: RuleConditions;
  metadata?: RuleMetadata;
}

@Route('api/v1/rules')
@Tags('Rules')
@Response(401, 'Unauthorized')
export class RulesController extends Controller {
  private async getAuthToken(): Promise<string> {
    const auth = new GoogleAuth();
    const client = await auth.getIdTokenClient(RULES_MANAGEMENT_API_URL!);
    const tokenResponse = await client.getRequestHeaders();
    const token = tokenResponse.get('Authorization');
    if (!token) {
      throw new Error('Failed to get authorization token');
    }
    return token;
  }
  @Get()
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules-management-api')
  public async listRules(): Promise<Rule[]> {
    const token = await this.getAuthToken();

    try {
      const response = await axios.get(RULES_MANAGEMENT_API_URL!, {
        headers: { Authorization: token },
      });
      return response.data;
    } catch (error: unknown) {
      console.error('Error fetching rules:', error);
      throw error;
    }
  }

  @Post()
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules-management-api')
  public async createRule(
    @Body() rule: RuleCreate,
  ): Promise<Rule> {
    const token = await this.getAuthToken();

    try {
      const response = await axios.post(RULES_MANAGEMENT_API_URL!, rule, {
        headers: { Authorization: token },
      });
      return response.data;
    } catch (error: unknown) {
      console.error('Error creating rule:', error);
      throw error;
    }
  }

  @Get('{ruleId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules-management-api')
  public async getRule(
    @Path() ruleId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<Rule> {
    const token = await this.getAuthToken();

    try {
      const response = await axios.get(`${RULES_MANAGEMENT_API_URL!}/${ruleId}`, {
        headers: { Authorization: token },
      });
      return response.data;
    } catch (error: unknown) {
      console.error(`Error fetching rule ${ruleId}:`, error);
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return notFound(404, { message: `Rule ${ruleId} not found` });
      }
      throw error;
    }
  }

  @Put('{ruleId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules-management-api')
  public async updateRule(
    @Path() ruleId: string,
    @Body() rule: RuleUpdate,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<Rule> {
    const token = await this.getAuthToken();

    try {
      const response = await axios.put(`${RULES_MANAGEMENT_API_URL!}/${ruleId}`, rule, {
        headers: { Authorization: token },
      });
      return response.data;
    } catch (error: unknown) {
      console.error(`Error updating rule ${ruleId}:`, error);
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return notFound(404, { message: `Rule ${ruleId} not found` });
      }
      throw error;
    }
  }

  @Delete('{ruleId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'rules-management-api')
  public async deleteRule(
    @Path() ruleId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<void> {
    const token = await this.getAuthToken();

    try {
      await axios.delete(`${RULES_MANAGEMENT_API_URL!}/${ruleId}`, {
        headers: { Authorization: token },
      });
    } catch (error: unknown) {
      console.error(`Error deleting rule ${ruleId}:`, error);
      if (axios.isAxiosError(error) && error.response?.status === 404) {
        return notFound(404, { message: `Rule ${ruleId} not found` });
      }
      throw error;
    }
  }
}
