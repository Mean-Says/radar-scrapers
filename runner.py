"""
Ponto de entrada do radar-scrapers.
Uso: python runner.py <grupo>
Grupos: linkedin | remote | all
"""
import asyncio
import logging
import sys
from typing import List

from schema import IngestJob
from client import post_jobs

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

GROUPS = {
    "linkedin": "scrapers.linkedin",
    "remote": "scrapers.remote_apis",
}


async def run(group: str) -> None:
    jobs: List[IngestJob] = []

    targets = list(GROUPS.items()) if group == "all" else [(group, GROUPS[group])]

    for name, module_path in targets:
        try:
            mod = __import__(module_path, fromlist=["scrape"])
            scrape_fn = getattr(mod, "scrape")
            result = scrape_fn()
            logger.info(f"{name}: {len(result)} vagas")
            jobs.extend(result)
        except Exception as e:
            logger.error(f"{name}: falhou — {e}")

    await post_jobs(jobs)


if __name__ == "__main__":
    group = sys.argv[1] if len(sys.argv) > 1 else "all"
    if group not in list(GROUPS.keys()) + ["all"]:
        print(f"Grupo inválido: {group}. Use: {' | '.join(list(GROUPS.keys()) + ['all'])}")
        sys.exit(1)
    asyncio.run(run(group))
