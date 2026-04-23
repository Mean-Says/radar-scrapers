import os

INGEST_URL = os.environ["INGEST_URL"]          # https://radardeempregos.com/api/jobs/ingest
INGEST_API_KEY = os.environ["INGEST_API_KEY"]  # chave secreta compartilhada

# Termos de busca usados pelos scrapers que aceitam query livre (LinkedIn, etc.)
# Cobrem as principais buscas dos usuários do Radar de Empregos
SEARCH_TERMS = [
    "desenvolvedor python",
    "desenvolvedor javascript",
    "desenvolvedor frontend",
    "desenvolvedor backend",
    "desenvolvedor fullstack",
    "engenheiro de software",
    "analista de dados",
    "cientista de dados",
    "machine learning",
    "devops",
    "desenvolvedor mobile",
    "desenvolvedor react",
    "ux designer",
    "product manager",
    "desenvolvedor",
]

LINKEDIN_TERMS = [
    "desenvolvedor",
    "analista de dados",
    "engenheiro de software",
    "devops",
    "ux designer",
]
