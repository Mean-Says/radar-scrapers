"""Envia vagas normalizadas para o endpoint de ingestão do Radar de Empregos."""
import httpx
import logging
from typing import List
from schema import IngestJob
from config import INGEST_URL, INGEST_API_KEY

logger = logging.getLogger(__name__)

BATCH_SIZE = 100  # máximo de vagas por request


async def post_jobs(jobs: List[IngestJob]) -> None:
    if not jobs:
        print("Nenhuma vaga para enviar.")
        return

    valid = []
    for job in jobs:
        try:
            valid.append(IngestJob.model_validate(job.model_dump()))
        except Exception as e:
            logger.warning(f"Vaga inválida ignorada: {e}")

    total_new = 0
    total_dup = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for i in range(0, len(valid), BATCH_SIZE):
            batch = valid[i : i + BATCH_SIZE]
            payload = {"jobs": [j.model_dump(mode="json") for j in batch]}
            try:
                resp = await client.post(
                    INGEST_URL,
                    headers={"x-api-key": INGEST_API_KEY},
                    json=payload,
                )
                resp.raise_for_status()
                data = resp.json()
                total_new += data.get("new", 0)
                total_dup += data.get("duplicates", 0)
            except httpx.HTTPStatusError as e:
                logger.error(f"Ingest HTTP {e.response.status_code}: {e.response.text[:200]}")
            except Exception as e:
                logger.error(f"Ingest error: {e}")

    print(f"Enviadas: {len(valid)} | Novas: {total_new} | Duplicadas: {total_dup}")
