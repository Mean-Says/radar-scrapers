"""
Scrapers de boards brasileiros: TrabalhaBrasil + Vagas.com.
Usa httpx (sync) portando a lógica de parsing dos scrapers aiohttp do Radar_de_emprego.

Expõe um único scrape() que varre matriz cargos×cidades e retorna List[IngestJob].
"""
from __future__ import annotations

import html as html_module
import logging
import re
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx

from schema import IngestJob

logger = logging.getLogger(__name__)

# ─── Configuração ──────────────────────────────────────────────────────────────

# Mesmos cargos do indeed.py — alta demanda no BR
SEARCH_TERMS = [
    "atendente",
    "assistente administrativo",
    "vendedor",
    "auxiliar administrativo",
    "recepcionista",
    "analista",
    "desenvolvedor",
    "enfermeiro",
]

# Cidades principais + sweep nacional (None = sem filtro de cidade)
CITIES: List[tuple[Optional[str], Optional[str]]] = [
    ("rio-de-janeiro", "rj"),
    ("sao-paulo", "sp"),
    ("belo-horizonte", "mg"),
    (None, None),  # sweep nacional
]

PAGES_PER_COMBO = 1      # 1 página por combinação (15 vagas/TB, 40 vagas/Vagas.com)
REQUEST_TIMEOUT = 20     # segundos
DELAY_BETWEEN_REQUESTS = 1.5  # segundos entre requests

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
}


# ─── Helpers compartilhados ────────────────────────────────────────────────────

def _unescape(text: str) -> str:
    return html_module.unescape(text).strip() if text else ""


def _strip_tags(s: str) -> str:
    return re.sub(r"<[^>]+>", "", s)


def _clean(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _slugify_tb(text: str) -> str:
    """Slug para TrabalhaBrasil (hifens, sem acentos)."""
    text = text.lower().strip()
    replacements = {
        "á": "a", "ã": "a", "â": "a", "à": "a",
        "é": "e", "ê": "e", "è": "e",
        "í": "i", "î": "i",
        "ó": "o", "õ": "o", "ô": "o",
        "ú": "u", "û": "u",
        "ç": "c", "ñ": "n",
    }
    for acc, plain in replacements.items():
        text = text.replace(acc, plain)
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


def _slugify_vagas(text: str) -> str:
    """Slug para Vagas.com (NFKD unicode normalisation)."""
    nfkd = unicodedata.normalize("NFKD", text)
    normalized = "".join(c for c in nfkd if not unicodedata.combining(c))
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9\s-]", "", normalized)
    normalized = re.sub(r"[\s-]+", "-", normalized)
    return normalized.strip("-")


def _fetch(client: httpx.Client, url: str) -> Optional[str]:
    """GET com tratamento de erros. Retorna HTML ou None."""
    try:
        resp = client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        if resp.status_code == 404:
            return None
        if resp.status_code == 429:
            logger.warning(f"brazil_boards: rate-limit (429) em {url}")
            return None
        if resp.status_code != 200:
            logger.debug(f"brazil_boards: HTTP {resp.status_code} para {url}")
            return None
        return resp.text
    except httpx.TimeoutException:
        logger.warning(f"brazil_boards: timeout em {url}")
        return None
    except Exception as e:
        logger.warning(f"brazil_boards: erro em {url}: {e}")
        return None


# ─── TrabalhaBrasil ────────────────────────────────────────────────────────────

_TB_BASE = "https://www.trabalhabrasil.com.br"


def _tb_build_url(job_slug: str, city_slug: Optional[str], uf: Optional[str], page: int) -> str:
    if city_slug and uf:
        base = f"{_TB_BASE}/vagas-de-emprego-em-{city_slug}-{uf}/{job_slug}"
    else:
        base = f"{_TB_BASE}/vagas-de-emprego/{job_slug}"
    return base if page <= 1 else f"{base}?pagina={page}"


def _tb_parse(html: str) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    articles = re.findall(
        r'<article[^>]+class="job-card"[^>]*>.*?</article>',
        html,
        re.S,
    )
    for article in articles:
        try:
            href_m = re.search(r'href="(/vagas-de-emprego[^"]+)"', article)
            if not href_m:
                continue
            link = _TB_BASE + href_m.group(1)

            title_m = re.search(r'class="job-title"[^>]*>\s*(.*?)\s*</h2>', article, re.S)
            if not title_m:
                continue
            title = _unescape(_strip_tags(title_m.group(1)).strip())
            title = re.sub(r"^\d*\s*Vagas?\s+de\s+", "", title, flags=re.I).strip()
            if not title or len(title) < 3:
                continue

            company_m = re.search(r'class="job-company"[^>]*>.*?<span>(.*?)</span>', article, re.S)
            company = _unescape(company_m.group(1)) if company_m else None

            location_m = re.search(r'class="job-location"[^>]*>.*?<span>(.*?)</span>', article, re.S)
            location = _unescape(location_m.group(1)) if location_m else "Brasil"

            salary_m = re.search(r'class="salary"[^>]*>(.*?)</span>', article, re.S)
            salary = _unescape(salary_m.group(1)) if salary_m else None

            workplace_m = re.search(r'class="workplace">(.*?)</span>', article)
            employment_m = re.search(r'class="employment-type">(.*?)</span>', article)
            desc_parts = [
                p for p in [
                    _unescape(salary_m.group(1)) if salary_m else "",
                    _unescape(workplace_m.group(1)) if workplace_m else "",
                    _unescape(employment_m.group(1)) if employment_m else "",
                ]
                if p
            ]

            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "link": link,
                "description": " | ".join(desc_parts) or None,
                "salary": salary,
                "source": "trabalhabrasil",
            })
        except Exception as e:
            logger.debug(f"trabalhabrasil: parse error: {e}")
    return jobs


def scrape_trabalhabrasil(client: httpx.Client) -> List[IngestJob]:
    jobs: List[IngestJob] = []
    seen_urls: set = set()

    for term in SEARCH_TERMS:
        slug = _slugify_tb(term)
        for city_slug, uf in CITIES:
            for page in range(1, PAGES_PER_COMBO + 1):
                url = _tb_build_url(slug, city_slug, uf, page)
                time.sleep(DELAY_BETWEEN_REQUESTS)
                html = _fetch(client, url)
                if not html:
                    continue
                for raw in _tb_parse(html):
                    link = raw["link"]
                    if link in seen_urls:
                        continue
                    seen_urls.add(link)
                    try:
                        jobs.append(IngestJob(
                            title=raw["title"],
                            url=link,
                            source="trabalhabrasil",
                            company=raw.get("company"),
                            location=raw.get("location"),
                            description=raw.get("description"),
                            salary=raw.get("salary"),
                        ))
                    except Exception as e:
                        logger.debug(f"trabalhabrasil: IngestJob error: {e}")

    logger.info(f"trabalhabrasil: {len(jobs)} vagas coletadas")
    return jobs


# ─── Vagas.com ─────────────────────────────────────────────────────────────────

_VAGAS_BASE = "https://www.vagas.com.br"
_CARD_RE = re.compile(r'<li class="vaga [^"]*">(.*?)</li>', re.DOTALL)
_DATE_RE = re.compile(r'(\d{2})/(\d{2})/(\d{4})')


def _vagas_parse_date(raw: str) -> Optional[datetime]:
    m = _DATE_RE.search(raw)
    if m:
        d, mo, y = m.groups()
        try:
            return datetime(int(y), int(mo), int(d))
        except ValueError:
            return None
    return None


def _vagas_parse(html: str) -> List[Dict[str, Any]]:
    jobs: List[Dict[str, Any]] = []
    for m in _CARD_RE.finditer(html):
        card = m.group(1)
        try:
            id_title = re.search(r'data-id-vaga="\d+"[^>]*title="([^"]+)"', card)
            if not id_title:
                continue
            title = id_title.group(1).strip()

            href_m = re.search(r'href="(/vagas/v\d+/[^"]+)"', card)
            if not href_m:
                continue
            link = _VAGAS_BASE + href_m.group(1)

            company_m = re.search(r'<span class="emprVaga">\s*(.*?)\s*</span>', card, re.DOTALL)
            company = _clean(_strip_tags(company_m.group(1))) if company_m else None

            loc_m = re.search(r'<div class="vaga-local">\s*<i[^>]+></i>\s*([^\n<]+)', card)
            location = loc_m.group(1).strip() if loc_m else None

            desc_m = re.search(r'<div class="detalhes">\s*<p>(.*?)</p>', card, re.DOTALL)
            description = _clean(_strip_tags(desc_m.group(1)))[:800] if desc_m else None

            date_m = re.search(r'<span class="data-publicacao">.*?(\d{2}/\d{2}/\d{4})', card, re.DOTALL)
            posted_at = _vagas_parse_date(date_m.group(1)) if date_m else None

            nivel_m = re.search(r'<span class="nivelVaga">\s*(.*?)\s*</span>', card, re.DOTALL)
            seniority = _clean(nivel_m.group(1)) if nivel_m else None

            jobs.append({
                "title": title,
                "company": company,
                "location": location,
                "link": link,
                "description": description,
                "seniority": seniority,
                "posted_at": posted_at,
                "source": "vagas.com",
            })
        except Exception as e:
            logger.debug(f"vagas.com: parse error: {e}")
    return jobs


def scrape_vagas_com(client: httpx.Client) -> List[IngestJob]:
    jobs: List[IngestJob] = []
    seen_urls: set = set()

    for term in SEARCH_TERMS:
        slug = _slugify_vagas(term)
        for page in range(1, PAGES_PER_COMBO + 1):
            params = f"?pagina={page}" if page > 1 else ""
            url = f"{_VAGAS_BASE}/vagas-de-{slug}{params}"
            time.sleep(DELAY_BETWEEN_REQUESTS)
            html = _fetch(client, url)
            if not html:
                continue
            for raw in _vagas_parse(html):
                link = raw["link"]
                if link in seen_urls:
                    continue
                seen_urls.add(link)
                try:
                    jobs.append(IngestJob(
                        title=raw["title"],
                        url=link,
                        source="vagas.com",
                        company=raw.get("company"),
                        location=raw.get("location"),
                        description=raw.get("description"),
                        seniority=raw.get("seniority"),
                        posted_at=raw.get("posted_at"),
                    ))
                except Exception as e:
                    logger.debug(f"vagas.com: IngestJob error: {e}")

    logger.info(f"vagas.com: {len(jobs)} vagas coletadas")
    return jobs


# ─── Ponto de entrada unificado ────────────────────────────────────────────────

def scrape() -> List[IngestJob]:
    """
    Roda TrabalhaBrasil + Vagas.com e retorna lista combinada de IngestJob.
    Compartilha um único httpx.Client entre os dois scrapers.
    """
    jobs: List[IngestJob] = []

    with httpx.Client(headers=_HEADERS, verify=False) as client:
        try:
            tb_jobs = scrape_trabalhabrasil(client)
            jobs.extend(tb_jobs)
        except Exception as e:
            logger.error(f"brazil_boards: trabalhabrasil falhou: {e}")

        try:
            vc_jobs = scrape_vagas_com(client)
            jobs.extend(vc_jobs)
        except Exception as e:
            logger.error(f"brazil_boards: vagas.com falhou: {e}")

    logger.info(f"brazil_boards: total {len(jobs)} vagas ({len([j for j in jobs if j.source == 'trabalhabrasil'])} TB + {len([j for j in jobs if j.source == 'vagas.com'])} Vagas.com)")
    return jobs
