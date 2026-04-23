"""
LinkedIn scraper via python-jobspy.
Roda a cada 30min via GitHub Actions com hours_old=1 para garantir vagas frescas.
"""
import asyncio
import logging
from typing import List
from datetime import datetime, timezone

from schema import IngestJob

logger = logging.getLogger(__name__)

LOCATION = "Brazil"
RESULTS_PER_TERM = 20


def _row_to_job(row) -> IngestJob | None:
    try:
        url = str(row.get("job_url", "") or "").strip()
        title = str(row.get("title", "") or "").strip()
        if not url or not url.startswith("http") or not title:
            return None

        date_posted = row.get("date_posted")
        posted_at = None
        if date_posted:
            try:
                if hasattr(date_posted, "to_pydatetime"):
                    posted_at = date_posted.to_pydatetime().replace(tzinfo=timezone.utc)
                elif isinstance(date_posted, datetime):
                    posted_at = date_posted.replace(tzinfo=timezone.utc)
            except Exception:
                pass

        description = str(row.get("description", "") or "")[:3000]
        salary = str(row.get("min_amount", "") or "")
        if row.get("max_amount"):
            salary = f"{salary} - {row.get('max_amount')}" if salary else str(row["max_amount"])
        if row.get("currency"):
            salary = f"{salary} {row['currency']}".strip() if salary else str(row["currency"])

        return IngestJob(
            title=title,
            url=url,
            source="linkedin",
            company=str(row.get("company", "") or "").strip() or None,
            location=str(row.get("location", "") or "").strip() or None,
            description=description or None,
            job_type=_map_job_type(str(row.get("job_type", "") or "")),
            salary=salary or None,
            remote=_is_remote(str(row.get("location", "") or ""), str(row.get("job_type", "") or "")),
            posted_at=posted_at,
        )
    except Exception as e:
        logger.debug(f"linkedin: parse error: {e}")
        return None


def _map_job_type(raw: str) -> str | None:
    mapping = {"fulltime": "CLT", "parttime": "PJ", "internship": "Estágio", "contract": "PJ", "temporary": "PJ"}
    return mapping.get(raw.lower().replace("-", ""), None)


def _is_remote(location: str, job_type: str) -> bool | None:
    text = (location + " " + job_type).lower()
    if any(w in text for w in ["remote", "remoto", "anywhere"]):
        return True
    return None


def scrape() -> List[IngestJob]:
    try:
        from jobspy import scrape_jobs
    except ImportError:
        logger.error("python-jobspy não instalado: pip install python-jobspy")
        return []

    jobs: List[IngestJob] = []
    seen_urls: set = set()

    try:
        # search_term="" retorna todas as vagas do Brasil da última hora,
        # sem filtro por cargo — o matching acontece no backend ao despachar para usuários
        df = scrape_jobs(
            site_name=["linkedin"],
            search_term="",
            location=LOCATION,
            hours_old=1,
            results_wanted=300,
            linkedin_fetch_description=False,
            verbose=0,
        )
        if df is not None and not df.empty:
            for _, row in df.iterrows():
                job = _row_to_job(row)
                if job and job.url not in seen_urls:
                    seen_urls.add(job.url)
                    jobs.append(job)
    except Exception as e:
        logger.warning(f"linkedin: erro no scrape: {e}")

    logger.info(f"linkedin: {len(jobs)} vagas coletadas")
    return jobs
