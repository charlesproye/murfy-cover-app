export type Fleet = {
  id: string;
  name: string;
};

export type Role = {
  id: string;
  name: string;
};

export type User = {
  id: string;
  created_at: string;
  updated_at: string;
  email: string;
  first_name: string;
  last_name: string;
  last_connection: string | null;
  is_active: boolean;
  phone: string;
  role: Role;
  fleets: Fleet[];
};

export type Company = {
  id: number;
  name: string;
  description: string;
};

export type AuthResponse = {
  company: Company;
  user: User;
};
