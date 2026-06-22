export interface GoogleUser {
  email: string;
  email_verified: boolean;
  sub: string;
  aud: string;
  iss: string;
  isAdmin?: boolean;
}

export interface UserResponse {
  email: string;
  isAdmin: boolean;
}
