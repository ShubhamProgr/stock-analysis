import type { Metadata } from "next";
import { Space_Grotesk, Inter, JetBrains_Mono } from "next/font/google";
import Link from "next/link";
import "./globals.css";

const display = Space_Grotesk({ subsets: ["latin"], variable: "--font-display", weight: ["500", "700"] });
const body = Inter({ subsets: ["latin"], variable: "--font-body", weight: ["400", "500", "600"] });
const mono = JetBrains_Mono({ subsets: ["latin"], variable: "--font-mono", weight: ["400", "500", "700"] });

export const metadata: Metadata = {
  title: "StockIQ | NSE Market Analysis",
  description: "Price history, fundamentals, sentiment and model forecasts for tracked NSE tickers."
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en" className={`${display.variable} ${body.variable} ${mono.variable}`}>
      <body className="bg-ink-900 text-text-primary font-body antialiased">
        <div className="flex min-h-screen flex-col">
          <header className="border-b border-ink-600/60 bg-ink-950/70 backdrop-blur">
            <div className="mx-auto flex max-w-7xl items-center justify-between px-6 py-4">
              <Link href="/" className="flex items-center gap-3">
                <span className="grid h-8 w-8 place-items-center rounded-md bg-signal-amber/15 font-mono text-sm font-bold text-signal-amber">
                  Q
                </span>
                <span className="font-display text-lg font-bold tracking-tight">
                  StockIQ
                </span>
              </Link>
              <nav className="flex items-center gap-6 text-sm text-text-muted">
                <Link href="/" className="hover:text-text-primary">Dashboard</Link>
                <Link href="/admin" className="hover:text-text-primary">Admin</Link>
              </nav>
            </div>
          </header>
          <main className="flex-1">{children}</main>
          <footer className="border-t border-ink-600/60 py-6 text-center text-xs text-text-faint">
            StockIQ &mdash; automated NSE research workspace. Data via yfinance, scored with FinBERT, forecast with a RandomForest close-price model.
          </footer>
        </div>
      </body>
    </html>
  );
}
