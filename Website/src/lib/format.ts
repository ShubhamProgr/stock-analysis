export function formatINR(value: number | null | undefined, opts: { compact?: boolean } = {}): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
  if (opts.compact) {
    return new Intl.NumberFormat("en-IN", {
      notation: "compact",
      maximumFractionDigits: 2
    }).format(value);
  }
  return new Intl.NumberFormat("en-IN", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2
  }).format(value);
}

export function formatPercent(value: number | null | undefined): string {
  if (value === null || value === undefined || Number.isNaN(value)) return "N/A";
  return `${(value * 100).toFixed(2)}%`;
}

export function formatDate(value: string | Date | null | undefined): string {
  if (!value) return "N/A";
  const date = typeof value === "string" ? new Date(value) : value;
  return new Intl.DateTimeFormat("en-IN", { day: "2-digit", month: "short", year: "numeric" }).format(date);
}
