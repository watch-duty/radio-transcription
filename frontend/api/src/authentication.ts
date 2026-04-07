import * as express from "express";
import * as jwt from "jsonwebtoken";

export function expressAuthentication(
  request: express.Request,
  securityName: string
): Promise<any> {
  // Match the name defined in tsoa.json and @Security decorators
  if (securityName === "google_id_token") {
    const authHeader = request.headers.authorization;
    
    if (!authHeader) {
      return Promise.reject(new Error("No authorization header provided"));
    }
    const token = authHeader.split(" ")[1];
    
    if (!token) {
      return Promise.reject(new Error("No token provided in Authorization header"));
    }
    // API Gateway already verified the signature at this point
    const decoded = jwt.decode(token);
    
    if (!decoded) {
      return Promise.reject(new Error("Invalid JWT token"));
    }

    // The resolved value is what gets injected into your controllers
    return Promise.resolve(decoded);
  }
  
  return Promise.reject(new Error(`Unknown security name: ${securityName}`));
}