// Typed client for the Tenantboard API. Every error comes back as RFC 9457
// problem details; ApiError keeps them, including any Snowflake error code.

export type TenantRole = "viewer" | "admin";

export interface Merchant {
  tenant_id: string;
  slug: string;
  name: string;
  plan: string;
  region: string;
  currency: string;
}

export interface Summary {
  tenant_id: string;
  role: TenantRole;
  snowflake_role: string;
  orders: number;
  completed_orders: number;
  customers: number;
  revenue: string;
  average_order_value: string;
  currency: string;
  first_order_at: string | null;
  last_order_at: string | null;
}

export interface DailyRevenue {
  day: string;
  orders: number;
  revenue: number;
  revenue_7d_avg: number;
}

export interface RevenueSeries {
  tenant_id: string;
  currency: string;
  days: DailyRevenue[];
}

export interface TopCustomer {
  customer_id: number;
  name: string;
  email: string;
  orders: number;
  revenue: string;
}

export interface Order {
  order_id: string;
  tenant_id: string;
  ordered_at: string;
  status: string;
  channel: string;
  currency: string;
  total_amount: string;
  customer_id: number;
  customer_name: string | null;
  customer_email: string | null;
}

export interface OrderPage {
  items: Order[];
  page: number;
  page_size: number;
  total: number;
}

export interface UploadResult {
  tenant_id: string;
  rows: number;
  inserted: number;
  updated: number;
}

export interface Problem {
  type: string;
  title: string;
  status: number;
  detail: string;
  snowflake?: { error_code: string; sql_state: string | null; query_id: string | null };
  errors?: { line?: number; column?: string; loc?: string[]; msg: string }[];
}

export class ApiError extends Error {
  constructor(readonly problem: Problem) {
    super(problem.detail || problem.title);
  }
}

async function request<T>(path: string, role: TenantRole | null, init: RequestInit = {}, signal?: AbortSignal): Promise<T> {
  const headers = new Headers(init.headers);
  if (role) headers.set("X-Tenant-Role", role);
  const response = await fetch(path, { ...init, headers, signal });
  if (!response.ok) {
    let problem: Problem;
    try {
      problem = (await response.json()) as Problem;
    } catch {
      problem = { type: "about:blank", title: response.statusText, status: response.status, detail: `HTTP ${response.status}` };
    }
    throw new ApiError(problem);
  }
  return (await response.json()) as T;
}

const tenant = (slug: string) => `/api/tenants/${encodeURIComponent(slug)}`;

export const api = {
  tenants: (signal?: AbortSignal) => request<Merchant[]>("/api/tenants", null, {}, signal),
  summary: (slug: string, role: TenantRole, signal?: AbortSignal) => request<Summary>(`${tenant(slug)}/summary`, role, {}, signal),
  revenue: (slug: string, role: TenantRole, signal?: AbortSignal) => request<RevenueSeries>(`${tenant(slug)}/revenue`, role, {}, signal),
  topCustomers: (slug: string, role: TenantRole, signal?: AbortSignal) =>
    request<TopCustomer[]>(`${tenant(slug)}/top-customers?limit=5`, role, {}, signal),
  orders: (slug: string, role: TenantRole, page: number, pageSize: number, signal?: AbortSignal) =>
    request<OrderPage>(`${tenant(slug)}/orders?page=${page}&page_size=${pageSize}`, role, {}, signal),
  upload: (slug: string, file: File) => {
    const body = new FormData();
    body.append("file", file);
    return request<UploadResult>(`${tenant(slug)}/orders/upload`, "admin", { method: "POST", body });
  },
};
