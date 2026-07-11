"use client";

import { useState } from "react";
import { useRouter, useSearchParams } from "next/navigation";

export default function AdminLoginPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [key, setKey] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError(null);
    try {
      const res = await fetch("/api/admin/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ key })
      });
      if (!res.ok) {
        const body = await res.json().catch(() => ({}));
        throw new Error(body.error ?? "Invalid access key.");
      }
      router.push(searchParams.get("next") ?? "/admin");
      router.refresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Something went wrong.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="mx-auto flex min-h-[70vh] max-w-md flex-col justify-center px-6 py-16">
      <div className="panel p-8">
        <span className="text-xs uppercase tracking-wide text-signal-amber">Secure access</span>
        <h1 className="mt-2 font-display text-2xl font-bold">Enter the console key</h1>
        <p className="mt-2 text-sm text-text-muted">
          Authentication is required to reach the pipeline controls and SQL console.
        </p>

        {error && <div className="mt-4 rounded-lg bg-signal-down/10 px-3 py-2 text-sm text-signal-down">{error}</div>}

        <form onSubmit={handleSubmit} className="mt-6 space-y-4">
          <div>
            <label htmlFor="access_key" className="mb-1 block text-sm text-text-muted">
              Access key
            </label>
            <input
              id="access_key"
              type="password"
              required
              value={key}
              onChange={(e) => setKey(e.target.value)}
              className="w-full rounded-lg border border-ink-600 bg-ink-900 px-3 py-2 font-mono text-sm outline-none focus:border-signal-amber"
            />
          </div>
          <button
            type="submit"
            disabled={loading}
            className="w-full rounded-lg bg-signal-amber px-4 py-2 font-semibold text-ink-950 hover:brightness-110 disabled:opacity-50"
          >
            {loading ? "Checking..." : "Authorize"}
          </button>
        </form>
      </div>
    </div>
  );
}
