export function StatCard({
  label,
  value,
  sub,
  tone
}: {
  label: string;
  value: string;
  sub?: string;
  tone?: "up" | "down" | "neutral";
}) {
  const toneClass = tone === "up" ? "stat-up" : tone === "down" ? "stat-down" : "text-text-primary";
  return (
    <article className="panel p-4">
      <span className="text-xs uppercase tracking-wide text-text-faint">{label}</span>
      <strong className={`mt-1 block font-mono text-xl ${toneClass}`}>{value}</strong>
      {sub && <small className="mt-1 block text-xs text-text-muted">{sub}</small>}
    </article>
  );
}
