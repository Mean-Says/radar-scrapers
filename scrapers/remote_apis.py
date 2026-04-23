"""
Scrapers de fontes remotas internacionais via JSON API / RSS.
Fontes: Remotive, RemoteOK, Himalayas, Jobicy, WeWorkRemotely, Empllo, WorkingNomads.
Todas são APIs públicas sem autenticação.
"""
import asyncio
import logging
import re
import xml.etree.ElementTree as ET
from typing import List
from datetime import datetime, timezone

import httpx
from schema import IngestJob

logger = logging.getLogger(__name__)

# Categorias relevantes para o público BR do Radar de Empregos
REMOTIVE_CATEGORIES = ["software-dev", "data", "devops-sysadmin", "design", "product"]

WWR_FEEDS = [
    "https://weworkremotely.com/categories/remote-programming-jobs.rss",
    "https://weworkremotely.com/categories/remote-devops-sysadmin-jobs.rss",
    "https://weworkremotely.com/categories/remote-design-jobs.rss",
    "https://weworkremotely.com/categories/remote-product-jobs.rss",
]

WORKINGNOMADS_CATEGORIES = ["development", "design", "data", "devops-sysadmin"]


def _clean_html(html: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", html or "")).strip()


def _parse_rss_date(date_str: str) -> datetime | None:
    for fmt in ("%a, %d %b %Y %H:%M:%S %z", "%a, %d %b %Y %H:%M:%S +0000"):
        try:
            return datetime.strptime(date_str.strip(), fmt).replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return None


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
                seniority=", ".join(item["seniority"]) if isinstance(item.get("seniority"), list) else (item.get("seniority") or None),
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


async def _fetch_weworkremotely(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    for feed_url in WWR_FEEDS:
        try:
            resp = await client.get(feed_url, timeout=15)
            resp.raise_for_status()
            root = ET.fromstring(resp.text)
            for item in root.findall(".//item"):
                raw_title = (item.findtext("title") or "").strip()
                url = (item.findtext("link") or item.findtext("guid") or "").strip()
                if not url or not raw_title:
                    continue
                # Title format: "Company: Job Title"
                if ": " in raw_title:
                    company, title = raw_title.split(": ", 1)
                else:
                    company, title = "WeWorkRemotely", raw_title
                desc_html = item.findtext("description") or ""
                posted_at = _parse_rss_date(item.findtext("pubDate") or "")
                jobs.append(IngestJob(
                    title=title.strip(),
                    url=url,
                    source="weworkremotely",
                    company=company.strip() or None,
                    location=item.findtext("region") or "Remote",
                    description=_clean_html(desc_html)[:3000] or None,
                    remote=True,
                    posted_at=posted_at,
                ))
        except Exception as e:
            logger.warning(f"weworkremotely/{feed_url.split('/')[-1]}: {e}")
    return jobs


async def _fetch_empllo(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    try:
        resp = await client.get("https://empllo.com/feeds/remote-jobs.rss", timeout=15)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        for item in root.findall(".//item"):
            url = (item.findtext("link") or "").strip()
            title = (item.findtext("title") or "").strip()
            if not url or not title:
                continue
            min_sal = item.findtext("min_salary")
            max_sal = item.findtext("max_salary")
            salary = f"{min_sal}-{max_sal} USD" if min_sal and max_sal else None
            posted_at = _parse_rss_date(item.findtext("pubDate") or "")
            jobs.append(IngestJob(
                title=title,
                url=url,
                source="empllo",
                company=(item.findtext("company") or "").strip() or None,
                location=item.findtext("location") or "Remote",
                description=_clean_html(item.findtext("description") or "")[:3000] or None,
                salary=salary,
                remote=True,
                posted_at=posted_at,
            ))
    except Exception as e:
        logger.warning(f"empllo: {e}")
    return jobs


async def _fetch_workingnomads(client: httpx.AsyncClient) -> List[IngestJob]:
    jobs = []
    seen: set = set()
    for cat in WORKINGNOMADS_CATEGORIES:
        try:
            resp = await client.get(
                "https://www.workingnomads.com/api/exposed_jobs/",
                params={"category": cat},
                timeout=15,
            )
            resp.raise_for_status()
            for item in resp.json():
                url = (item.get("url") or "").strip()
                title = (item.get("title") or "").strip()
                if not url or not title or url in seen:
                    continue
                seen.add(url)
                posted_at = None
                pub = item.get("pub_date")
                if pub:
                    try:
                        posted_at = datetime.fromisoformat(pub).replace(tzinfo=timezone.utc)
                    except Exception:
                        pass
                jobs.append(IngestJob(
                    title=title,
                    url=url,
                    source="workingnomads",
                    company=(item.get("company_name") or "").strip() or None,
                    location=item.get("location") or "Remote",
                    description=_clean_html(item.get("description") or "")[:3000] or None,
                    remote=True,
                    posted_at=posted_at,
                ))
        except Exception as e:
            logger.warning(f"workingnomads/{cat}: {e}")
    return jobs


async def scrape_async() -> List[IngestJob]:
    async with httpx.AsyncClient() as client:
        results = await asyncio.gather(
            _fetch_remotive(client),
            _fetch_remoteok(client),
            _fetch_himalayas(client),
            _fetch_jobicy(client),
            _fetch_weworkremotely(client),
            _fetch_empllo(client),
            _fetch_workingnomads(client),
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

    logger.info(f"remote_apis: {len(jobs)} vagas coletadas (7 fontes)")
    return jobs


def scrape() -> List[IngestJob]:
    return asyncio.run(scrape_async())
