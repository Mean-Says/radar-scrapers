"""
Sólides scraper — portal ATS brasileiro (vagas.solides.com.br).

Fonte: REST API interna do portal (sem auth, CORS aberto).
Endpoint: GET https://apigw.solides.com.br/jobs/v3/portal-vacancies-new

A API retorna ~73k vagas ordenadas por data (mais recentes primeiro),
10 vagas por página. Varremos as primeiras PAGE_LIMIT páginas em cada
execução para capturar somente vagas novas sem exaurir cota de requests.

Volume esperado por run: PAGE_LIMIT × 10 (após dedup = 80–100 vagas úteis).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

import httpx

from schema import IngestJob

logger = logging.getLogger(__name__)

# ─── Configuração ──────────────────────────────────────────────────────────────

_API_URL = "https://apigw.solides.com.br/jobs/v3/portal-vacancies-new"
_PORTAL_BASE = "https://vagas.solides.com.br"

# Número de páginas a varrer por run (10 vagas/página → 80 vagas brutas)
# Aumentar se quiser mais volume; a API é rápida e não tem rate-limit observado.
PAGE_LIMIT = 8

REQUEST_TIMEOUT = 20   # segundos por request
DELAY_BETWEEN_PAGES = 1.0  # segundos entre páginas

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Accept-Language": "pt-BR,pt;q=0.9",
    "Origin": _PORTAL_BASE,
    "Referer": f"{_PORTAL_BASE}/vagas",
}

# Mapeamento de jobType da API → campo job_type do IngestJob
_JOB_TYPE_MAP = {
    "presencial": None,        # não é um tipo de contrato, é modalidade
    "home_office": None,
    "hibrido": None,
    "hibrido ": None,
}

_CONTRACT_MAP = {
    "CLT": "CLT",
    "PJ": "PJ",
    "Estagio": "Estágio",
    "Estágio": "Estágio",
    "Temporario": "PJ",
    "Freelancer": "Freelance",
    "Aprendiz": "Estágio",
    "Jovem Aprendiz": "Estágio",
}

_SENIORITY_MAP = {
    "Estagiário": "Junior",
    "Aprendiz": "Junior",
    "Júnior": "Junior",
    "Junior": "Junior",
    "Pleno": "Pleno",
    "Sênior": "Senior",
    "Senior": "Senior",
    "Especialista": "Senior",
    "Coordenador": "Senior",
    "Gerente": "Senior",
    "Diretor": "Senior",
}


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _parse_location(vaga: dict) -> Optional[str]:
    city = (vaga.get("city") or {}).get("name") or ""
    state = (vaga.get("state") or {}).get("code") or ""
    parts = [p.strip() for p in [city, state] if p.strip()]
    return ", ".join(parts) if parts else None


def _parse_salary(salary_data: dict) -> Optional[str]:
    if not salary_data:
        return None
    lo = salary_data.get("initialRange") or 0
    hi = salary_data.get("finalRange") or 0
    negotiable = salary_data.get("negotiable", False)
    if negotiable and not lo:
        return "A combinar"
    if lo and hi and hi > lo:
        return f"R$ {lo:,.0f} - R$ {hi:,.0f}".replace(",", ".")
    if lo:
        return f"R$ {lo:,.0f}".replace(",", ".")
    return None


def _parse_contract(vaga: dict) -> Optional[str]:
    """Extrai tipo de contrato da lista recruitmentContractType."""
    contracts = vaga.get("recruitmentContractType") or []
    for c in contracts:
        name = (c.get("name") or "").strip()
        mapped = _CONTRACT_MAP.get(name)
        if mapped:
            return mapped
    return None


def _parse_seniority(vaga: dict) -> Optional[str]:
    """Extrai senioridade da lista seniority."""
    seniorities = vaga.get("seniority") or []
    for s in seniorities:
        name = (s.get("name") or "").strip()
        for key, val in _SENIORITY_MAP.items():
            if key.lower() in name.lower():
                return val
    return None


def _parse_skills(vaga: dict) -> Optional[List[str]]:
    hard_skills = vaga.get("hardSkills") or []
    skills = [hs.get("name", "").strip() for hs in hard_skills if hs.get("name")]
    return skills[:20] if skills else None


def _parse_remote(vaga: dict) -> Optional[bool]:
    job_type = (vaga.get("jobType") or "").lower()
    home_office = vaga.get("homeOffice", False)
    if home_office or job_type in ("home_office", "remoto"):
        return True
    if job_type == "hibrido":
        return None  # híbrido = não é nem True nem False
    return False


def _parse_posted_at(vaga: dict) -> Optional[datetime]:
    raw = vaga.get("createdAt")
    if not raw:
        return None
    try:
        # A API retorna "YYYY-MM-DD"
        dt = datetime.strptime(raw[:10], "%Y-%m-%d")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _build_url(vaga: dict) -> Optional[str]:
    """
    Usa o redirectLink da API (URL canônica da empresa no Sólides ATS).
    Fallback para portal genérico usando id se necessário.
    """
    redirect = (vaga.get("redirectLink") or "").strip()
    if redirect and redirect.startswith("http"):
        return redirect
    vacancy_id = vaga.get("id") or vaga.get("referenceId")
    slug = vaga.get("slug")
    if vacancy_id and slug:
        return f"https://{slug}.solides.jobs/vacancies/{vacancy_id}?origem=portal"
    return None


def _row_to_job(vaga: dict) -> Optional[IngestJob]:
    try:
        url = _build_url(vaga)
        if not url:
            return None

        title = (vaga.get("title") or "").strip()
        if not title:
            return None

        company = (vaga.get("companyName") or "").strip() or None
        location = _parse_location(vaga)
        description = (vaga.get("description") or "").strip()[:3000] or None
        salary = _parse_salary(vaga.get("salary") or {})
        job_type = _parse_contract(vaga)
        seniority = _parse_seniority(vaga)
        skills = _parse_skills(vaga)
        remote = _parse_remote(vaga)
        posted_at = _parse_posted_at(vaga)

        return IngestJob(
            title=title,
            url=url,
            source="solides",
            company=company,
            location=location,
            description=description,
            job_type=job_type,
            salary=salary,
            remote=remote,
            seniority=seniority,
            skills=skills,
            posted_at=posted_at,
        )
    except Exception as e:
        logger.debug(f"solides: parse error: {e}")
        return None


# ─── Fetch ────────────────────────────────────────────────────────────────────

def _fetch_page(client: httpx.Client, page: int) -> list:
    """Faz GET na API e retorna lista de vagas brutas. Retorna [] em caso de erro."""
    try:
        resp = client.get(
            _API_URL,
            params={"page": page},
            timeout=REQUEST_TIMEOUT,
            follow_redirects=True,
        )
        if resp.status_code == 429:
            logger.warning(f"solides: rate-limit (429) na página {page}")
            return []
        if resp.status_code != 200:
            logger.warning(f"solides: HTTP {resp.status_code} na página {page}")
            return []

        payload = resp.json()
        if not payload.get("success"):
            logger.warning(f"solides: success=false na página {page}: {payload}")
            return []

        return payload.get("data", {}).get("data") or []

    except httpx.TimeoutException:
        logger.warning(f"solides: timeout na página {page}")
        return []
    except Exception as e:
        logger.warning(f"solides: erro na página {page}: {e}")
        return []


# ─── Ponto de entrada ─────────────────────────────────────────────────────────

def scrape() -> List[IngestJob]:
    """
    Varre as primeiras PAGE_LIMIT páginas da API Sólides e retorna
    List[IngestJob] com vagas deduplicadas por URL.
    """
    jobs: List[IngestJob] = []
    seen_urls: set = set()

    with httpx.Client(headers=_HEADERS, verify=True) as client:
        for page in range(1, PAGE_LIMIT + 1):
            if page > 1:
                time.sleep(DELAY_BETWEEN_PAGES)

            raw_vagas = _fetch_page(client, page)
            if not raw_vagas:
                logger.debug(f"solides: página {page} vazia, encerrando")
                break

            page_count = 0
            for vaga in raw_vagas:
                job = _row_to_job(vaga)
                if not job:
                    continue
                if job.url in seen_urls:
                    continue
                seen_urls.add(job.url)
                jobs.append(job)
                page_count += 1

            logger.info(f"solides: página {page} → {page_count} vagas")

    logger.info(f"solides: total {len(jobs)} vagas coletadas")
    return jobs
