import { useEffect, useMemo, useRef, useState } from "react";
import type { DailyRevenue } from "./api";
import { formatDay, formatMoney, formatMoneyCompact } from "./format";

// Daily completed revenue (columns, series slot 1) with its trailing 7-day mean
// (line, slot 2). One y-axis: both are the same currency.

const HEIGHT = 240;
const MARGIN = { top: 16, right: 64, bottom: 28, left: 56 };

function niceMax(value: number): number {
  if (value <= 0) return 1;
  const magnitude = 10 ** Math.floor(Math.log10(value));
  const step = [1, 2, 2.5, 5, 10].find((s) => s * magnitude >= value / 4) ?? 10;
  return Math.ceil(value / (step * magnitude)) * step * magnitude;
}

/** Column path with a 4px rounded data-end and a square baseline. */
function columnPath(x: number, y: number, width: number, baseline: number): string {
  const r = Math.min(4, width / 2, baseline - y);
  if (baseline - y <= 0) return "";
  return `M${x},${baseline} V${y + r} Q${x},${y} ${x + r},${y} H${x + width - r} Q${x + width},${y} ${x + width},${y + r} V${baseline} Z`;
}

export function RevenueChart({ days, currency }: { days: DailyRevenue[]; currency: string }) {
  const frame = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(720);
  const [hover, setHover] = useState<number | null>(null);
  const [showTable, setShowTable] = useState(false);

  useEffect(() => {
    const element = frame.current;
    if (!element) return;
    const observer = new ResizeObserver(([entry]) => setWidth(Math.max(320, Math.floor(entry.contentRect.width))));
    observer.observe(element);
    return () => observer.disconnect();
  }, [showTable]);

  const geometry = useMemo(() => {
    const plotWidth = width - MARGIN.left - MARGIN.right;
    const plotHeight = HEIGHT - MARGIN.top - MARGIN.bottom;
    const max = niceMax(Math.max(0, ...days.map((d) => Math.max(d.revenue, d.revenue_7d_avg))));
    const band = days.length ? plotWidth / days.length : plotWidth;
    const barWidth = Math.max(2, Math.min(24, band - 2)); // 2px surface gap between neighbours
    const x = (i: number) => MARGIN.left + i * band + band / 2;
    const y = (v: number) => MARGIN.top + plotHeight - (v / max) * plotHeight;
    const ticks = [0, 0.25, 0.5, 0.75, 1].map((t) => t * max);
    const line = days.map((d, i) => `${i ? "L" : "M"}${x(i).toFixed(1)},${y(d.revenue_7d_avg).toFixed(1)}`).join(" ");
    return { plotWidth, plotHeight, band, barWidth, x, y, ticks, line, baseline: y(0) };
  }, [days, width]);

  if (!days.length) return <p className="muted">No completed orders yet.</p>;

  const { band, barWidth, x, y, ticks, line, baseline } = geometry;
  const last = days.length - 1;
  const active = hover === null ? null : days[hover];
  const labelEvery = Math.ceil(days.length / Math.max(2, Math.floor(geometry.plotWidth / 90)));

  return (
    <div className="chart">
      <div className="chart-head">
        <ul className="legend" aria-label="Legend">
          <li><span className="key key-bar" aria-hidden="true" />Daily revenue</li>
          <li><span className="key key-line" aria-hidden="true" />7-day average</li>
        </ul>
        <button type="button" className="link" onClick={() => setShowTable((v) => !v)} aria-expanded={showTable}>
          {showTable ? "Show chart" : "Show as table"}
        </button>
      </div>

      {showTable ? (
        <div className="table-wrap">
          <table>
            <caption className="sr-only">Daily revenue in {currency}</caption>
            <thead>
              <tr><th scope="col">Day</th><th scope="col" className="num">Orders</th><th scope="col" className="num">Revenue</th><th scope="col" className="num">7-day average</th></tr>
            </thead>
            <tbody>
              {days.map((d) => (
                <tr key={d.day}>
                  <td>{formatDay(d.day)}</td>
                  <td className="num">{d.orders}</td>
                  <td className="num">{formatMoney(d.revenue, currency)}</td>
                  <td className="num">{formatMoney(d.revenue_7d_avg, currency)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <div className="plot" ref={frame}>
          <svg width={width} height={HEIGHT} role="img" aria-label={`Daily revenue and 7-day average in ${currency}, ${days.length} days`}
               onMouseLeave={() => setHover(null)}>
            {ticks.map((t) => (
              <g key={t}>
                <line className={t === 0 ? "axis" : "grid"} x1={MARGIN.left} x2={width - MARGIN.right} y1={y(t)} y2={y(t)} />
                <text className="tick" x={MARGIN.left - 8} y={y(t)} dy="0.32em" textAnchor="end">{formatMoneyCompact(t, currency)}</text>
              </g>
            ))}
            {days.map((d, i) => (i % labelEvery === 0 ? (
              <text key={d.day} className="tick" x={x(i)} y={HEIGHT - 8} textAnchor="middle">{formatDay(d.day)}</text>
            ) : null))}
            {hover !== null && <rect className="hover-band" x={x(hover) - band / 2} y={MARGIN.top} width={band} height={baseline - MARGIN.top} />}
            {days.map((d, i) => (
              <path key={d.day} className="bar" d={columnPath(x(i) - barWidth / 2, y(d.revenue), barWidth, baseline)} />
            ))}
            <path className="avg-line" d={line} />
            <circle className="avg-dot" cx={x(last)} cy={y(days[last].revenue_7d_avg)} r={4} />
            <text className="end-label" x={x(last) + 10} y={y(days[last].revenue_7d_avg)} dy="0.32em">
              {formatMoneyCompact(days[last].revenue_7d_avg, currency)}
            </text>
            {days.map((d, i) => (
              <rect key={d.day} className="hit" x={x(i) - band / 2} y={MARGIN.top} width={band} height={baseline - MARGIN.top}
                    onMouseEnter={() => setHover(i)} onFocus={() => setHover(i)} tabIndex={-1} />
            ))}
          </svg>
          {active && hover !== null && (
            <div className="tooltip" role="status"
                 style={{ left: Math.min(Math.max(x(hover), 90), width - 90), top: Math.max(y(Math.max(active.revenue, active.revenue_7d_avg)) - 12, 8) }}>
              <strong>{formatDay(active.day)}</strong>
              <span><span className="key key-bar" aria-hidden="true" />Revenue {formatMoney(active.revenue, currency)} ({active.orders} orders)</span>
              <span><span className="key key-line" aria-hidden="true" />7-day avg {formatMoney(active.revenue_7d_avg, currency)}</span>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
