"""
Indeed scraper via python-jobspy.
Itera uma lista de cargos de alta demanda no Brasil (25 vagas/termo, últimas 72h).
Roda a cada 30min via GitHub Actions.
"""
import logging
import time
from datetime import datetime, timezone
from typing import List

from schema import IngestJob

logger = logging.getLogger(__name__)

# Cargos de alta demanda no Brasil — baseado em dados reais do produto
HIGH_DEMAND_TERMS = [
    "atendente",
    "assistente administrativo",
    "vendedor",
    "auxiliar administrativo",
    "recepcionista",
    "analista",
    "desenvolvedor",
    "enfermeiro",
]

RESULTS_PER_TERM = 25
HOURS_OLD = 72
DELAY_BETWEEN_TERMS = 3  # segundos — respeita rate limit do Indeed


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
            salary = f"{salary} - {row['max_amount']}" if salary else str(row["max_amount"])
        if row.get("currency"):
            salary = f"{salary} {row['currency']}".strip() if salary else str(row["currency"])

        return IngestJob(
            title=title,
            url=url,
            source="indeed",
            company=str(row.get("company", "") or "").strip() or None,
            location=str(row.get("location", "") or "").strip() or None,
            description=description or None,
            job_type=_map_job_type(str(row.get("job_type", "") or "")),
            salary=salary or None,
            remote=_is_remote(
                str(row.get("location", "") or ""),
                str(row.get("job_type", "") or ""),
            ),
            posted_at=posted_at,
        )
    except Exception as e:
        logger.debug(f"indeed: parse error: {e}")
        return None


def _map_job_type(raw: str) -> str | None:
    mapping = {
        "fulltime": "CLT",
        "parttime": "PJ",
        "internship": "Estágio",
        "contract": "PJ",
        "temporary": "PJ",
    }
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

    for i, term in enumerate(HIGH_DEMAND_TERMS):
        if i > 0:
            time.sleep(DELAY_BETWEEN_TERMS)
        try:
            df = scrape_jobs(
                site_name=["indeed"],
                search_term=term,
                location="Brazil",
                country_indeed="Brazil",
                hours_old=HOURS_OLD,
                results_wanted=RESULTS_PER_TERM,
                verbose=0,
            )
            if df is None or df.empty:
                logger.debug(f"indeed: sem resultados para '{term}'")
                continue

            term_count = 0
            for _, row in df.iterrows():
                job = _row_to_job(row)
                if job and job.url not in seen_urls:
                    seen_urls.add(job.url)
                    jobs.append(job)
                    term_count += 1

            logger.info(f"indeed: '{term}' → {term_count} vagas")

        except Exception as e:
            logger.warning(f"indeed: erro ao buscar '{term}': {e}")
            continue

    logger.info(f"indeed: total {len(jobs)} vagas coletadas")
    return jobs
