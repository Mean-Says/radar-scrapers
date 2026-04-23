/**
 * Cloudflare Worker — Remote APIs scraper
 * Fontes: Remotive, RemoteOK, Himalayas, Jobicy
 * Todas são JSON APIs públicas (sem auth, sem browser)
 * Roda via Cron Trigger a cada 15 minutos
 */

export interface Env {
  INGEST_URL: string;
  INGEST_API_KEY: string;
}

interface IngestJob {
  title: string;
  url: string;
  source: string;
  company?: string;
  location?: string;
  description?: string;
  job_type?: string;
  salary?: string;
  remote?: boolean;
  seniority?: string;
  skills?: string[];
  posted_at?: string;
}

// ---------- Remotive ----------

const REMOTIVE_CATEGORIES = ["software-dev", "data", "devops-sysadmin", "design", "product"];

async function fetchRemotive(): Promise<IngestJob[]> {
  const jobs: IngestJob[] = [];
  for (const cat of REMOTIVE_CATEGORIES) {
    try {
      const resp = await fetch(
        `https://remotive.com/api/remote-jobs?category=${cat}&limit=30`,
        { cf: { cacheTtl: 0 } }
      );
      if (!resp.ok) continue;
      const data: any = await resp.json();
      for (const item of data.jobs ?? []) {
        if (!item.url || !item.title) continue;
        jobs.push({
          title: item.title.trim().slice(0, 500),
          url: item.url.trim(),
          source: "remotive",
          company: item.company_name || undefined,
          location: item.candidate_required_location || "Remote",
          description: (item.description ?? "").slice(0, 3000) || undefined,
          job_type: item.job_type || undefined,
          salary: item.salary || undefined,
          remote: true,
          skills: (item.tags ?? []).slice(0, 20),
          posted_at: item.publication_date || undefined,
        });
      }
    } catch (_) {}
  }
  return jobs;
}

// ---------- RemoteOK ----------

async function fetchRemoteOK(): Promise<IngestJob[]> {
  const jobs: IngestJob[] = [];
  try {
    const resp = await fetch("https://remoteok.com/api", {
      headers: { "User-Agent": "Mozilla/5.0" },
      cf: { cacheTtl: 0 },
    });
    if (!resp.ok) return jobs;
    const data: any[] = await resp.json();
    for (const item of data.slice(1)) {
      const url = item.url?.startsWith("http") ? item.url : `https://remoteok.com${item.url}`;
      if (!url || !item.position) continue;
      jobs.push({
        title: item.position.trim().slice(0, 500),
        url: url.trim(),
        source: "remoteok",
        company: item.company || undefined,
        location: item.location || "Remote",
        description: (item.description ?? "").slice(0, 3000) || undefined,
        salary: item.salary || undefined,
        remote: true,
        skills: (item.tags ?? []).slice(0, 20),
        posted_at: item.epoch ? new Date(item.epoch * 1000).toISOString() : undefined,
      });
    }
  } catch (_) {}
  return jobs;
}

// ---------- Himalayas ----------

async function fetchHimalayas(): Promise<IngestJob[]> {
  const jobs: IngestJob[] = [];
  try {
    const resp = await fetch("https://himalayas.app/jobs/api?limit=50", {
      cf: { cacheTtl: 0 },
    });
    if (!resp.ok) return jobs;
    const data: any = await resp.json();
    for (const item of data.jobs ?? []) {
      const url = item.applicationLink || item.url;
      if (!url?.startsWith("http") || !item.title) continue;
      const skills = Array.isArray(item.skills)
        ? item.skills.map((s: any) => String(s).trim()).slice(0, 20)
        : [];
      jobs.push({
        title: item.title.trim().slice(0, 500),
        url: url.trim(),
        source: "himalayas",
        company: item.companyName || undefined,
        location: (item.locations ?? [])[0] || "Remote",
        description: (item.description ?? "").slice(0, 3000) || undefined,
        salary: item.salaryRange || undefined,
        remote: true,
        seniority: item.seniority || undefined,
        skills: skills.length ? skills : undefined,
        posted_at: item.postedAt || item.createdAt || undefined,
      });
    }
  } catch (_) {}
  return jobs;
}

// ---------- Jobicy ----------

async function fetchJobicy(): Promise<IngestJob[]> {
  const jobs: IngestJob[] = [];
  try {
    const resp = await fetch("https://jobicy.com/api/v2/remote-jobs?count=50", {
      cf: { cacheTtl: 0 },
    });
    if (!resp.ok) return jobs;
    const data: any = await resp.json();
    for (const item of data.jobs ?? []) {
      if (!item.url || !item.jobTitle) continue;
      const skills = Array.isArray(item.jobIndustry)
        ? item.jobIndustry.map((s: any) => String(s).trim()).slice(0, 10)
        : [];
      let salary: string | undefined;
      if (item.annualSalaryMin) {
        salary = `${item.annualSalaryMin}-${item.annualSalaryMax ?? ""} ${item.salaryCurrency ?? ""}`.trim();
      }
      jobs.push({
        title: item.jobTitle.trim().slice(0, 500),
        url: item.url.trim(),
        source: "jobicy",
        company: item.companyName || undefined,
        location: item.jobGeo || "Remote",
        description: (item.jobDescription ?? "").slice(0, 3000) || undefined,
        salary: salary || undefined,
        remote: true,
        seniority: item.jobLevel || undefined,
        skills: skills.length ? skills : undefined,
        posted_at: item.pubDate || undefined,
      });
    }
  } catch (_) {}
  return jobs;
}

// ---------- Ingest ----------

async function postToIngest(jobs: IngestJob[], env: Env): Promise<void> {
  if (!jobs.length) return;

  const BATCH = 100;
  for (let i = 0; i < jobs.length; i += BATCH) {
    const batch = jobs.slice(i, i + BATCH);
    try {
      const resp = await fetch(env.INGEST_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "x-api-key": env.INGEST_API_KEY,
        },
        body: JSON.stringify({ jobs: batch }),
      });
      const data: any = await resp.json();
      console.log(`Ingest batch ${i / BATCH + 1}: new=${data.new} dup=${data.duplicates}`);
    } catch (err) {
      console.error("Ingest error:", err);
    }
  }
}

// ---------- Handler ----------

export default {
  async scheduled(_event: ScheduledEvent, env: Env, _ctx: ExecutionContext): Promise<void> {
    const [remotive, remoteok, himalayas, jobicy] = await Promise.allSettled([
      fetchRemotive(),
      fetchRemoteOK(),
      fetchHimalayas(),
      fetchJobicy(),
    ]);

    const all: IngestJob[] = [];
    const seen = new Set<string>();

    for (const r of [remotive, remoteok, himalayas, jobicy]) {
      if (r.status === "fulfilled") {
        for (const job of r.value) {
          if (!seen.has(job.url)) {
            seen.add(job.url);
            all.push(job);
          }
        }
      }
    }

    console.log(`Total vagas coletadas: ${all.length}`);
    await postToIngest(all, env);
  },

  // Permite testar via GET /
  async fetch(_req: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    ctx.waitUntil(this.scheduled({} as any, env, ctx));
    return new Response("Scraper iniciado", { status: 202 });
  },
};
