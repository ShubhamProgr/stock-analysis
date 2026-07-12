"use client";

import { useEffect, useRef } from "react";
import { createChart, ColorType} from "lightweight-charts";

export type Candle = { time: string; open: number; high: number; low: number; close: number };

export function CandlestickChart({ data }: { data: Candle[] }) {
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!containerRef.current || data.length === 0) return;

    const chart = createChart(containerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: "transparent" },
        textColor: "#8b96ad",
        fontFamily: "var(--font-mono)"
      },
      grid: {
        vertLines: { color: "#1f2634" },
        horzLines: { color: "#1f2634" }
      },
      width: containerRef.current.clientWidth,
      height: 360,
      timeScale: { borderColor: "#262e3d" },
      rightPriceScale: { borderColor: "#262e3d" }
    });

    const series = chart.addCandlestickSeries({
      upColor: "#3ddc97",
      downColor: "#ff6b6b",
      borderVisible: false,
      wickUpColor: "#3ddc97",
      wickDownColor: "#ff6b6b"
    });

    series.setData(data);
    chart.timeScale().fitContent();

    const handleResize = () => {
      if (containerRef.current) chart.applyOptions({ width: containerRef.current.clientWidth });
    };
    window.addEventListener("resize", handleResize);

    return () => {
      window.removeEventListener("resize", handleResize);
      chart.remove();
    };
  }, [data]);

  if (data.length === 0) {
    return <div className="grid h-80 place-items-center text-sm text-text-muted">No price history available.</div>;
  }

  return <div ref={containerRef} className="w-full" />;
}
