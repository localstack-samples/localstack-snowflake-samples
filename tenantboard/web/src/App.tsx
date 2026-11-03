import { useCallback, useEffect, useState, type FormEvent, type ReactNode } from "react";
import {
  api,
  ApiError,
  type Merchant,
  type OrderPage,
  type Problem,
  type RevenueSeries,
  type Summary,
  type TenantRole,
  type TopCustomer,
  type UploadResult,
} from "./api";
import { formatMoney, formatTimestamp } from "./format";
import { RevenueChart } from "./RevenueChart";

const PAGE_SIZE = 10;

type Load<T> = { state: "loading" } | { state: "ready"; data: T } | { state: "error"; problem: Problem };

function toProblem(error: unknown): Problem {
  if (error instanceof ApiError) return error.problem;
  return { type: "about:blank", title: "Network error", status: 0, detail: String(error) };
}

/** Fetch whenever `deps` change; stale responses are aborted, never rendered. */
function useLoad<T>(fetcher: (signal: AbortSignal) => Promise<T>, deps: unknown[]): [Load<T>, () => void] {
  const [load, setLoad] = useState<Load<T>>({ state: "loading" });
  const [nonce, setNonce] = useState(0);
  useEffect(() => {
    const controller = new AbortController();
    setLoad({ state: "loading" });
    fetcher(controller.signal)
      .then((data) => setLoad({ state: "ready", data }))
      .catch((error: unknown) => {
        if (!controller.signal.aborted) setLoad({ state: "error", problem: toProblem(error) });
      });
    return () => controller.abort();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);
  return [load, useCallback(() => setNonce((n) => n + 1), [])];
}

function ProblemNotice({ problem }: { problem: Problem }) {
  return (
    <div className="problem" role="alert">
      <strong>{problem.title}</strong>
      <span>{problem.detail}</span>
      {problem.snowflake && (
        <code>Snowflake {problem.snowflake.error_code}{problem.snowflake.query_id ? ` · query ${problem.snowflake.query_id}` : ""}</code>
      )}
      {problem.errors?.slice(0, 5).map((e, i) => (
        <span key={i} className="muted">{e.line ? `line ${e.line}, ` : ""}{e.column ?? e.loc?.join(".")}: {e.msg}</span>
      ))}
    </div>
  );
}

function Card({ title, subtitle, children, actions }: { title: string; subtitle?: string; children: ReactNode; actions?: ReactNode }) {
  return (
    <section className="card">
      <header className="card-head">
        <div>
          <h2>{title}</h2>
          {subtitle && <p className="muted">{subtitle}</p>}
        </div>
        {actions}
      </header>
      {children}
    </section>
  );
}

function Loading<T>({ load, children }: { load: Load<T>; children: (data: T) => ReactNode }) {
  if (load.state === "loading") return <p className="muted" aria-busy="true">Loading...</p>;
  if (load.state === "error") return <ProblemNotice problem={load.problem} />;
  return <>{children(load.data)}</>;
}

function Kpis({ summary }: { summary: Summary }) {
  return (
    <div className="kpis">
      <div className="kpi kpi-hero">
        <span className="kpi-label">Revenue (completed orders)</span>
        <span className="kpi-value">{formatMoney(summary.revenue, summary.currency)}</span>
      </div>
      <div className="kpi"><span className="kpi-label">Orders</span><span className="kpi-value">{summary.orders}</span></div>
      <div className="kpi"><span className="kpi-label">Customers</span><span className="kpi-value">{summary.customers}</span></div>
      <div className="kpi">
        <span className="kpi-label">Average order value</span>
        <span className="kpi-value">{formatMoney(summary.average_order_value, summary.currency)}</span>
      </div>
    </div>
  );
}

function TopCustomers({ customers, currency }: { customers: TopCustomer[]; currency: string }) {
  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr><th scope="col">Customer</th><th scope="col">Email</th><th scope="col" className="num">Orders</th><th scope="col" className="num">Revenue</th></tr>
        </thead>
        <tbody>
          {customers.map((c) => (
            <tr key={c.customer_id}>
              <td>{c.name}</td>
              <td><Email value={c.email} /></td>
              <td className="num">{c.orders}</td>
              <td className="num">{formatMoney(c.revenue, currency)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function Email({ value }: { value: string | null }) {
  if (value === "***MASKED***") return <span className="masked" title="Masked by Snowflake policy EMAIL_MASK">***MASKED***</span>;
  return <span>{value ?? "-"}</span>;
}

function Orders({ page, onPage }: { page: OrderPage; onPage: (page: number) => void }) {
  const first = page.total ? (page.page - 1) * page.page_size + 1 : 0;
  const last = Math.min(page.page * page.page_size, page.total);
  return (
    <>
      <div className="table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Order</th><th scope="col">Tenant</th><th scope="col">Ordered (UTC)</th><th scope="col">Status</th>
              <th scope="col">Channel</th><th scope="col">Customer</th><th scope="col">customer_email</th><th scope="col" className="num">Total</th>
            </tr>
          </thead>
          <tbody>
            {page.items.map((o) => (
              <tr key={o.order_id}>
                <td><code>{o.order_id}</code></td>
                <td><span className="tenant-tag">{o.tenant_id.toLowerCase().replace(/_/g, "-")}</span></td>
                <td>{formatTimestamp(o.ordered_at)}</td>
                <td><span className={`status status-${o.status}`}>{o.status}</span></td>
                <td>{o.channel}</td>
                <td>{o.customer_name ?? "-"}</td>
                <td><Email value={o.customer_email} /></td>
                <td className="num">{formatMoney(o.total_amount, o.currency)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <nav className="pager" aria-label="Orders pages">
        <span className="muted">{first}-{last} of {page.total}</span>
        <button type="button" onClick={() => onPage(page.page - 1)} disabled={page.page <= 1}>Previous</button>
        <button type="button" onClick={() => onPage(page.page + 1)} disabled={last >= page.total}>Next</button>
      </nav>
    </>
  );
}

function Upload({ slug, onUploaded }: { slug: string; onUploaded: () => void }) {
  const [file, setFile] = useState<File | null>(null);
  const [result, setResult] = useState<{ ok: UploadResult } | { problem: Problem } | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async (event: FormEvent) => {
    event.preventDefault();
    if (!file) return;
    setBusy(true);
    try {
      setResult({ ok: await api.upload(slug, file) });
      onUploaded();
    } catch (error) {
      setResult({ problem: toProblem(error) });
    } finally {
      setBusy(false);
    }
  };

  return (
    <form className="upload" onSubmit={submit}>
      <label>
        <span className="muted">CSV columns: order_id, customer_id, ordered_at, status, total_amount[, currency]</span>
        <input type="file" accept=".csv,text/csv" onChange={(e) => { setFile(e.target.files?.[0] ?? null); setResult(null); }} />
      </label>
      <button type="submit" disabled={!file || busy}>{busy ? "Uploading..." : "Upload"}</button>
      {result && "ok" in result && (
        <p className="ok" role="status">{result.ok.rows} rows: {result.ok.inserted} inserted, {result.ok.updated} updated</p>
      )}
      {result && "problem" in result && <ProblemNotice problem={result.problem} />}
    </form>
  );
}

export function App() {
  const [tenants] = useLoad((signal) => api.tenants(signal), []);
  const [slug, setSlug] = useState<string>(() => new URLSearchParams(location.search).get("tenant") ?? "acme");
  const [role, setRole] = useState<TenantRole>("viewer");
  const [page, setPage] = useState(1);

  useEffect(() => {
    const url = new URL(location.href);
    url.searchParams.set("tenant", slug);
    history.replaceState(null, "", url);
  }, [slug]);

  const [summary, reloadSummary] = useLoad<Summary>((s) => api.summary(slug, role, s), [slug, role]);
  const [revenue, reloadRevenue] = useLoad<RevenueSeries>((s) => api.revenue(slug, role, s), [slug, role]);
  const [top] = useLoad<TopCustomer[]>((s) => api.topCustomers(slug, role, s), [slug, role]);
  const [orders, reloadOrders] = useLoad<OrderPage>((s) => api.orders(slug, role, page, PAGE_SIZE, s), [slug, role, page]);

  const merchant: Merchant | undefined = tenants.state === "ready" ? tenants.data.find((t) => t.slug === slug) : undefined;
  const currency = merchant?.currency ?? "USD";

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo" aria-hidden="true">tb</span>
          <div>
            <h1>Tenantboard</h1>
            <p className="muted">Merchant analytics, isolated by Snowflake row access policies</p>
          </div>
        </div>
        <div className="controls">
          <label className="field">
            <span>Merchant</span>
            <select value={slug} onChange={(e) => { setSlug(e.target.value); setPage(1); }} disabled={tenants.state !== "ready"} aria-label="Merchant">
              {tenants.state === "ready" ? (
                tenants.data.map((t) => <option key={t.slug} value={t.slug}>{t.name} ({t.slug})</option>)
              ) : (
                <option value={slug}>{slug}</option>
              )}
            </select>
          </label>
          <div className="field">
            <span id="role-label">Role</span>
            <div className="segmented" role="radiogroup" aria-labelledby="role-label">
              {(["viewer", "admin"] as const).map((r) => (
                <button key={r} type="button" role="radio" aria-checked={role === r} className={role === r ? "on" : ""} onClick={() => setRole(r)}>
                  {r === "viewer" ? "Viewer" : "Admin"}
                </button>
              ))}
            </div>
          </div>
        </div>
      </header>

      {tenants.state === "error" && <ProblemNotice problem={tenants.problem} />}

      <main>
        <div className="session" aria-live="polite">
          {summary.state === "ready" ? (
            <>Snowflake session role <code>{summary.data.snowflake_role}</code> · tenant <code>{summary.data.tenant_id}</code>
              {merchant && <> · {merchant.plan} plan · {merchant.region}</>}</>
          ) : (
            <>Opening a session for <code>{slug}</code>...</>
          )}
        </div>

        <Loading load={summary}>{(data) => <Kpis summary={data} />}</Loading>

        <Card title="Daily revenue" subtitle={`Completed orders, ${currency}`}>
          <Loading load={revenue}>{(data) => <RevenueChart days={data.days} currency={data.currency} />}</Loading>
        </Card>

        <div className="grid-2">
          <Card title="Top customers" subtitle="By completed revenue">
            <Loading load={top}>{(data) => <TopCustomers customers={data} currency={currency} />}</Loading>
          </Card>
          <Card title="Upload offline orders" subtitle={role === "admin" ? "Inserts new orders, updates existing ones (MERGE)" : "Switch to the Admin role to upload"}>
            {role === "admin" ? (
              <Upload slug={slug} onUploaded={() => { reloadSummary(); reloadRevenue(); reloadOrders(); }} />
            ) : (
              <p className="muted">Viewers read masked data and cannot write. The API also refuses uploads from the viewer role (403).</p>
            )}
          </Card>
        </div>

        <Card title="Orders" subtitle="Newest first">
          <Loading load={orders}>{(data) => <Orders page={data} onPage={setPage} />}</Loading>
        </Card>
      </main>
    </div>
  );
}
