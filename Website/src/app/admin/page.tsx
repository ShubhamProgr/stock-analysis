import Link from "next/link";
import { query } from "@/lib/db";
import { PipelineRunner } from "@/components/PipelineRunner";
import { formatDate } from "@/lib/format";

export const dynamic = "force-dynamic";

type RunRow = { id: number; job: string; status: string; started_at: string; message: string | null };

async function getRecentRuns() {
  return query<RunRow>(
    `select id, job, status, started_at, message from pipeline_runs order by started_at desc limit 15`
  ).catch(() => [] as RunRow[]);
}

export default async function AdminPage() {
  const runs = await getRecentRuns();

  return (
    <div className="mx-auto max-w-7xl px-6 py-8">
      <div className="flex items-center justify-between">
        <div>
          <span className="text-xs uppercase tracking-wide text-signal-amber">Admin console</span>
          <h1 className="mt-2 font-display text-2xl font-bold">Pipeline controls</h1>
        </div>
        <div className="flex gap-3">
          <Link href="/admin/query" className="rounded-lg border border-ink-600 px-4 py-2 text-sm hover:border-signal-amber">
            SQL console
          </Link>
          <form action="/api/admin/logout" method="post">
            <button
              formAction="/api/admin/logout"
              className="rounded-lg border border-ink-600 px-4 py-2 text-sm hover:border-signal-down"
            >
              Sign out
            </button>
          </form>
        </div>
      </div>

      <section className="mt-6">
        <PipelineRunner />
      </section>

      <section className="mt-8 panel p-6">
        <h2 className="font-display text-lg font-semibold">Recent runs</h2>
        <div className="mt-4 overflow-x-auto">
          <table className="w-full text-left text-sm">
            <thead>
              <tr className="text-text-faint">
                <th className="pb-2 pr-4">Job</th>
                <th className="pb-2 pr-4">Status</th>
                <th className="pb-2 pr-4">Started</th>
                <th className="pb-2">Message</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-ink-600/50">
              {runs.map((r) => (
                <tr key={r.id}>
                  <td className="py-2 pr-4 font-mono">{r.job}</td>
                  <td className={`py-2 pr-4 ${r.status === "failed" ? "stat-down" : "stat-up"}`}>{r.status}</td>
                  <td className="py-2 pr-4 text-text-muted">{formatDate(r.started_at)}</td>
                  <td className="py-2 text-text-muted">{r.message ?? "-"}</td>
                </tr>
              ))}
              {runs.length === 0 && (
                <tr>
                  <td colSpan={4} className="py-4 text-text-muted">
                    No pipeline runs logged yet.
                  </td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}
