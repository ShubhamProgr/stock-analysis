"use client";

import { useRouter } from "next/navigation";
import { useState } from "react";

export function CompanySearch({ companies }: { companies: string[] }) {
  const router = useRouter();
  const [value, setValue] = useState("");

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    const trimmed = value.trim().toLowerCase();
    if (!trimmed) return;
    router.push(`/company/${encodeURIComponent(trimmed)}`);
  }

  return (
    <form onSubmit={handleSubmit} className="flex gap-2">
      <input
        list="company-options"
        value={value}
        onChange={(e) => setValue(e.target.value)}
        placeholder="Search a company (e.g. reliance)"
        className="w-full rounded-lg border border-ink-600 bg-ink-900 px-3 py-2 text-sm outline-none placeholder:text-text-faint focus:border-signal-amber"
      />
      <datalist id="company-options">
        {companies.map((c) => (
          <option key={c} value={c} />
        ))}
      </datalist>
      <button
        type="submit"
        className="shrink-0 rounded-lg bg-signal-amber px-4 py-2 text-sm font-semibold text-ink-950 hover:brightness-110"
      >
        Open
      </button>
    </form>
  );
}
