"""
Schema padronizado de vaga para ingestão no Radar de Empregos.
Todos os scrapers devem produzir instâncias de IngestJob antes de enviar.
"""
from pydantic import BaseModel, field_validator
from typing import Optional, List
from datetime import datetime


class IngestJob(BaseModel):
    # --- Obrigatórios ---
    title: str
    url: str        # chave de dedup — URL canônica da vaga
    source: str     # ex: "linkedin", "remotive", "remoteok"

    # --- Altamente recomendados ---
    company: Optional[str] = None
    location: Optional[str] = None
    description: Optional[str] = None

    # --- Contrato / remuneração ---
    job_type: Optional[str] = None   # CLT | PJ | Estágio | Freelance
    salary: Optional[str] = None
    remote: Optional[bool] = None

    # --- Senioridade e skills ---
    seniority: Optional[str] = None  # Junior | Pleno | Senior
    skills: Optional[List[str]] = None

    # --- Metadados ---
    posted_at: Optional[datetime] = None

    @field_validator("title")
    @classmethod
    def title_not_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("title cannot be empty")
        return v[:500]

    @field_validator("url")
    @classmethod
    def url_valid(cls, v: str) -> str:
        v = v.strip()
        if not v.startswith("http"):
            raise ValueError(f"invalid URL: {v!r}")
        return v

    @field_validator("source")
    @classmethod
    def source_lowercase(cls, v: str) -> str:
        return v.lower().strip()

    @field_validator("skills")
    @classmethod
    def clean_skills(cls, v: Optional[List[str]]) -> Optional[List[str]]:
        if not v:
            return None
        return [s.strip() for s in v if s and s.strip()][:30]

    @field_validator("description")
    @classmethod
    def truncate_description(cls, v: Optional[str]) -> Optional[str]:
        return v.strip()[:3000] if v else None

    def to_job_data(self) -> dict:
        """Converte para o formato esperado por salvar_vagas_analisadas."""
        return {
            "link": self.url,
            "title": self.title,
            "company": self.company,
            "location": self.location,
            "description": self.description,
            "description_snippet": (self.description or "")[:500],
            "job_type": self.job_type,
            "salary_info": self.salary,
            "salary": self.salary,
            "source": self.source,
            "date": self.posted_at.isoformat() if self.posted_at else None,
            "detected_level": self.seniority,
            "detected_skills": self.skills or [],
            "skills": self.skills or [],
            "remote_friendly": self.remote,
        }
