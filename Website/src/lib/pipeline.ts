import { spawn } from "child_process";
import path from "path";
import { query } from "@/lib/db";
import { PIPELINE_JOBS, type PipelineJob } from "@/lib/pipeline-jobs";

export { PIPELINE_JOBS, type PipelineJob };

const JOB_SCRIPTS: Record<PipelineJob, string> = {
  stock_data_daily: "Stock_Data_Daily.py",
  stock_data_5y: "Stock_Data_5Y.py",
  company_info: "Company_Data.py",
  news_extractor: "News_Extractor.py",
  sentiment_analyzer: "Sentiment_Analyzer.py",
  final_analysis: "Final_Analysis.py",
  prediction_vs_actual: "Actual_vs_Prediction.py"
};

// "pipeline" sits at the repo root, as a sibling of src/. In the Docker
// image both Next.js and this folder are copied to /app, so this resolves
// to /app/pipeline at runtime; PIPELINE_DIR can override it if you move it.
function pipelineDir(): string {
  return process.env.PIPELINE_DIR ?? path.join(process.cwd(), "..", "stock analysis");
}

function pythonBin(): string {
  return process.env.PYTHON_BIN ?? "python3";
}

// Fires the script in the background and returns immediately with a
// pipeline_runs id. The row gets updated to success/failed once the
// subprocess exits -- poll GET /api/admin/runs (or just refresh /admin) to
// see the result.
export async function triggerPipelineJob(
  job: PipelineJob,
  triggeredBy = "admin-ui"
): Promise<{ ok: boolean; runId: number | null; message: string }> {
  const script = JOB_SCRIPTS[job];
  const scriptPath = path.join(pipelineDir(), script);

  const [{ id: runId } = { id: null }] = await query<{ id: number }>(
    `insert into pipeline_runs (job, status, triggered_by) values ($1, 'started', $2) returning id`,
    [job, triggeredBy]
  ).catch(() => [{ id: null }]);

  const child = spawn(pythonBin(), [scriptPath], {
    cwd: pipelineDir(),
    env: process.env,
    detached: true,
    stdio: ["ignore", "pipe", "pipe"]
  });

  let stdout = "";
  let stderr = "";
  child.stdout?.on("data", (chunk) => (stdout += chunk.toString()));
  child.stderr?.on("data", (chunk) => (stderr += chunk.toString()));

  child.on("close", async (code) => {
    const ok = code === 0;
    const tail = (ok ? stdout : stderr || stdout).slice(-2000);
    if (runId !== null) {
      await query(
        `update pipeline_runs set status = $1, message = $2, finished_at = now() where id = $3`,
        [ok ? "success" : "failed", tail || "(no output)", runId]
      ).catch((e) => console.error("Failed to update pipeline_runs", e));
    }
  });

  child.unref();

  return { ok: true, runId, message: `${script} started.` };
}

export async function fetchLivePrice(ticker: string): Promise<{
  ticker: string;
  price: number | null;
  changePct: number | null;
} | null> {
  return new Promise((resolve) => {
    const child = spawn(pythonBin(), [path.join(pipelineDir(), "live_price.py"), ticker], {
      cwd: pipelineDir(),
      env: process.env
    });

    let stdout = "";
    child.stdout.on("data", (chunk) => (stdout += chunk.toString()));
    child.on("close", () => {
      try {
        resolve(JSON.parse(stdout.trim().split("\n").pop() ?? "null"));
      } catch {
        resolve(null);
      }
    });
    child.on("error", () => resolve(null));

    // Live quotes should be fast; don't let a hung process pile up.
    setTimeout(() => {
      child.kill();
      resolve(null);
    }, 15_000);
  });
}
