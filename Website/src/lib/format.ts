export function fmtMoney(v: number): string {
  return "₹" + v.toLocaleString("en-IN", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

export function fmtPct(v: number): string {
  return (v >= 0 ? "+" : "") + v.toFixed(2) + "%";
}

export function fmtVol(v: number): string {
  if (v >= 1e7) return (v / 1e7).toFixed(2) + "Cr";
  if (v >= 1e5) return (v / 1e5).toFixed(2) + "L";
  return v.toLocaleString("en-IN");
}

export function fmtCompact(v: number): string {
  if (v >= 1e12) return "₹" + (v / 1e12).toFixed(2) + "T";
  if (v >= 1e9) return "₹" + (v / 1e9).toFixed(2) + "B";
  if (v >= 1e7) return "₹" + (v / 1e7).toFixed(2) + "Cr";
  return "₹" + v.toLocaleString("en-IN");
}

export function dayLabel(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleDateString("en-IN", { month: "short", day: "numeric" });
}

export function sparklinePath(values: number[], w: number, h: number): string {
  if (values.length === 0) return "";
  const min = Math.min(...values);
  const max = Math.max(...values);
  const range = max - min || 1;
  return values
    .map((v, i) => {
      const x = (i / (values.length - 1 || 1)) * w;
      const y = h - ((v - min) / range) * h;
      return (i === 0 ? "M" : "L") + x.toFixed(1) + "," + y.toFixed(1);
    })
    .join(" ");
}
