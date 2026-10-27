const moneyFormats = new Map<string, Intl.NumberFormat>();
const compactFormats = new Map<string, Intl.NumberFormat>();

function cached(map: Map<string, Intl.NumberFormat>, currency: string, options: Intl.NumberFormatOptions) {
  let format = map.get(currency);
  if (!format) {
    format = new Intl.NumberFormat("en-US", { style: "currency", currency, ...options });
    map.set(currency, format);
  }
  return format;
}

export function formatMoney(value: number | string, currency: string): string {
  return cached(moneyFormats, currency, { minimumFractionDigits: 2 }).format(Number(value));
}

export function formatMoneyCompact(value: number | string, currency: string): string {
  return cached(compactFormats, currency, { notation: "compact", maximumFractionDigits: 1 }).format(Number(value));
}

export function formatDay(day: string): string {
  return new Date(`${day}T00:00:00Z`).toLocaleDateString("en-US", { month: "short", day: "numeric", timeZone: "UTC" });
}

export function formatTimestamp(timestamp: string): string {
  return new Date(`${timestamp}Z`).toLocaleString("en-US", {
    month: "short", day: "numeric", hour: "2-digit", minute: "2-digit", timeZone: "UTC",
  });
}
