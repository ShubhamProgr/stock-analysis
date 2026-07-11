import { NextResponse } from "next/server";
import { createAdminSession, adminCookieOptions } from "@/lib/auth";
import { timingSafeEqual } from "crypto";

function safeEqual(a: string, b: string): boolean {
  const bufA = Buffer.from(a);
  const bufB = Buffer.from(b);
  if (bufA.length !== bufB.length) return false;
  return timingSafeEqual(bufA, bufB);
}

export async function POST(req: Request) {
  const adminKey = process.env.ADMIN_KEY;
  if (!adminKey) {
    return NextResponse.json({ error: "ADMIN_KEY is not configured on the server." }, { status: 500 });
  }

  const { key } = await req.json().catch(() => ({ key: "" }));
  if (typeof key !== "string" || !safeEqual(key, adminKey)) {
    return NextResponse.json({ error: "Invalid access key." }, { status: 401 });
  }

  const token = await createAdminSession();
  const response = NextResponse.json({ ok: true });
  const cookie = adminCookieOptions();
  response.cookies.set(cookie.name, token, cookie);
  return response;
}
