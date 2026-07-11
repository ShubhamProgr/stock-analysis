import { NextResponse } from "next/server";
import { triggerPipelineJob, PIPELINE_JOBS, type PipelineJob } from "@/lib/pipeline";

export async function POST(req: Request) {
  const { job } = await req.json().catch(() => ({ job: "" }));

  if (!PIPELINE_JOBS.includes(job as PipelineJob)) {
    return NextResponse.json({ error: "Unknown job." }, { status: 400 });
  }

  try {
    const result = await triggerPipelineJob(job as PipelineJob);
    return NextResponse.json({ message: result.message, runId: result.runId });
  } catch (error) {
    const message = error instanceof Error ? error.message : "Failed to start the job.";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}
