import Link from "next/link";

export default function NotFound() {
  return (
    <div className="mx-auto max-w-xl px-6 py-24 text-center">
      <span className="text-xs uppercase tracking-wide text-signal-amber">404</span>
      <h1 className="mt-3 font-display text-2xl font-bold">Page not found</h1>
      <p className="mt-3 text-text-muted">The page you&apos;re looking for doesn&apos;t exist.</p>
      <Link href="/" className="mt-6 inline-block rounded-lg bg-signal-amber px-4 py-2 font-semibold text-ink-950">
        Back to dashboard
      </Link>
    </div>
  );
}
