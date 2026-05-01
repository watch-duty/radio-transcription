export class ApiError extends Error {
  public readonly status: number;
  public readonly statusText: string;
  public readonly responsePayload: unknown;
  constructor(status: number, statusText: string, payload?: unknown, message?: string) {
    super(message || `API Request failed [Status: ${status}] - ${statusText}`);
    this.status = status;
    this.statusText = statusText;
    this.responsePayload = payload;
    this.name = 'ApiError';
    // Restores prototype chains in environments compiling down to ES5
    Object.setPrototypeOf(this, ApiError.prototype);
  }
}
