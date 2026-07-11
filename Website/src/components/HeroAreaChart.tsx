"use client";

export function HeroAreaChart({ values }: { values: number[] }) {
  const data = values.length ? values : [5120, 5160, 5140, 5205, 5240, 5222, 5280, 5248];
  const min = Math.min(...data);
  const max = Math.max(...data);
  const spread = max - min || 1;
  const width = 640;
  const height = 220;
  const points = data.map((value, i) => {
    const x = (i / Math.max(data.length - 1, 1)) * (width - 20) + 10;
    const y = height - ((value - min) / spread) * (height - 30) - 10;
    return { x, y };
  });
  const linePath = points.map((p, i) => `${i === 0 ? "M" : "L"}${p.x},${p.y}`).join(" ");
  const areaPath = `${linePath} L ${width - 10},${height} L 10,${height} Z`;

  return (
    <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" className="h-56 w-full" aria-hidden="true">
      <defs>
        <linearGradient id="heroFill" x1="0" x2="0" y1="0" y2="1">
          <stop offset="0%" stopColor="rgba(242,177,52,0.35)" />
          <stop offset="100%" stopColor="rgba(242,177,52,0.02)" />
        </linearGradient>
      </defs>
      {[0.2, 0.4, 0.6, 0.8, 1].map((f) => (
        <line
          key={f}
          x1={10}
          x2={width - 10}
          y1={height * f}
          y2={height * f}
          stroke="#262e3d"
          strokeWidth={1}
        />
      ))}
      <path d={areaPath} fill="url(#heroFill)" />
      <path d={linePath} fill="none" stroke="#f2b134" strokeWidth={2} />
      {points.map((p, i) => (
        <circle key={i} cx={p.x} cy={p.y} r={3} fill="#f2b134" />
      ))}
    </svg>
  );
}
