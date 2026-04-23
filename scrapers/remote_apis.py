"""
Scrapers de fontes remotas internacionais via JSON API / RSS.
Fontes: Remotive, RemoteOK, Himalayas, Jobicy.
Todas são APIs públicas sem autenticação.
"""
import asyncio
import logging
from typing import List
from datetime import datetime, timezone

import httpx
from schema import IngestJob

logger = logging.getLogger(__name__)

# Categorias relevantes para o público BR do Radar de Empregos
REMOTIVE_CATEGORIES = ["software-dev", "data", "devops-sysadmin", "design", "product"]


async def _fetch_remotive(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    for category in REMOTIVE_CATEGORIES:
        try:
            resp = await client.get(
                "https://remotive.com/api/remote-jobs",
                params={"category": category, "limit": 30},
                timeout=15,
            )
            resp.raise_for_status()
            for item in resp.json().get("jobs", []):
                url = item.get("url", "").strip()
                title = item.get("title", "").strip()
                if not url or not title:
                    continue

                posted_at = None
                pub = item.get("publication_date")
                if pub:
                    try:
                        posted_at = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                    except Exception:
                        pass

                jobs.append(IngestJob(
                    title=title,
                    url=url,
                    source="remotive",
                    company=item.get("company_name") or None,
                    location=item.get("candidate_required_location") or "Remote",
                    description=(item.get("description") or "")[:3000] or None,
                    job_type=item.get("job_type") or None,
                    salary=item.get("salary") or None,
                    remote=True,
                    skills=[t.strip() for t in item.get("tags", []) if t.strip()][:20] or None,
                    posted_at=posted_at,
                ))
        except Exception as e:
            logger.warning(f"remotive/{category}: {e}")
    return jobs


async def _fetch_remoteok(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    try:
        resp = await client.get(
            "https://remoteok.com/api",
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=20,
        )
        resp.raise_for_status()
        data = resp.json()
        # Primeiro elemento é metadata, pular
        for item in data[1:]:
            url = item.get("url", "").strip()
            title = item.get("position", "").strip()
            if not url or not title:
                continue
            if not url.startswith("http"):
                url = f"https://remoteok.com{url}"

            posted_at = None
            epoch = item.get("epoch")
            if epoch:
                try:
                    posted_at = datetime.fromtimestamp(int(epoch), tz=timezone.utc)
                except Exception:
                    pass

            jobs.append(IngestJob(
                title=title,
                url=url,
                source="remoteok",
                company=item.get("company") or None,
                location=item.get("location") or "Remote",
                description=(item.get("description") or "")[:3000] or None,
                salary=item.get("salary") or None,
                remote=True,
                skills=[t.strip() for t in item.get("tags", []) if t.strip()][:20] or None,
                posted_at=posted_at,
            ))
    except Exception as e:
        logger.warning(f"remoteok: {e}")
    return jobs


async def _fetch_himalayas(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    try:
        resp = await client.get(
            "https://himalayas.app/jobs/api",
            params={"limit": 50},
            timeout=20,
        )
        resp.raise_for_status()
        for item in resp.json().get("jobs", []):
            url = item.get("applicationLink") or item.get("url") or ""
            title = item.get("title", "").strip()
            if not url or not title:
                continue
            if not url.startswith("http"):
                continue

            posted_at = None
            pub = item.get("postedAt") or item.get("createdAt")
            if pub:
                try:
                    posted_at = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                except Exception:
                    pass

            skills_raw = item.get("skills") or item.get("tags") or []
            if isinstance(skills_raw, list):
                skills = [str(s).strip() for s in skills_raw if s][:20]
            else:
                skills = []

            jobs.append(IngestJob(
                title=title,
                url=url,
                source="himalayas",
                company=item.get("companyName") or None,
                location=item.get("locations", [None])[0] if item.get("locations") else "Remote",
                description=(item.get("description") or "")[:3000] or None,
                salary=item.get("salaryRange") or None,
                remote=True,
                seniority=item.get("seniority") or None,
                skills=skills or None,
                posted_at=posted_at,
            ))
    except Exception as e:
        logger.warning(f"himalayas: {e}")
    return jobs


async def _fetch_jobicy(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    try:
        resp = await client.get(
            "https://jobicy.com/api/v2/remote-jobs",
            params={"count": 50},
            timeout=20,
        )
        resp.raise_for_status()
        for item in resp.json().get("jobs", []):
            url = item.get("url", "").strip()
            title = item.get("jobTitle", "").strip()
            if not url or not title:
                continue

            posted_at = None
            pub = item.get("pubDate")
            if pub:
                try:
                    posted_at = datetime.fromisoformat(pub.replace("Z", "+00:00"))
                except Exception:
                    pass

            skills_raw = item.get("jobIndustry") or []
            skills = [str(s).strip() for s in skills_raw if s][:10] if isinstance(skills_raw, list) else []

            jobs.append(IngestJob(
                title=title,
                url=url,
                source="jobicy",
                company=item.get("companyName") or None,
                location=item.get("jobGeo") or "Remote",
                description=(item.get("jobDescription") or "")[:3000] or None,
                salary=item.get("annualSalaryMin") and f"{item['annualSalaryMin']}-{item.get('annualSalaryMax', '')} {item.get('salaryCurrency', '')}".strip() or None,
                remote=True,
                seniority=item.get("jobLevel") or None,
                skills=skills or None,
                posted_at=posted_at,
            ))
    except Exception as e:
        logger.warning(f"jobicy: {e}")
    return jobs


async def scrape_async() -> List[IngestJob]:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            _fetch_remotive(client),
            _fetch_remoteok(client),
            _fetch_himalayas(client),
            _fetch_jobicy(client),
            return_exceptions=True,
        )

    jobs = []
    seen = set()
    for r in results:
        if isinstance(r, Exception):
            logger.warning(f"scraper falhou: {r}")
            continue
        for job in r:
            if job.url not in seen:
                seen.add(job.url)
                jobs.append(job)

    logger.info(f"remote_apis: {len(jobs)} vagas coletadas")
    return jobs


def scrape() -> List[IngestJob]:
    return asyncio.run(scrape_async())
