import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "Stock Analytics",
  description: "Price, sentiment, and model-derived trade signals for NSE-listed equities.",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
