"""Parâmetros configuráveis (podem ser sobrescritos por variáveis de ambiente)."""
import os

MAX_ROWS = int(os.environ.get("MAX_ROWS", 5_000_000))
BLOCK_SIZE_MB = float(os.environ.get("BLOCK_SIZE_MB", 64))
REPEATS = int(os.environ.get("REPEATS", 3))
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", 30))
MAX_ATTEMPTS = int(os.environ.get("MAX_ATTEMPTS", 3))
BACKOFF_BASE = float(os.environ.get("BACKOFF_BASE", 0.5))
DATA_PATH = os.environ.get("DATA_PATH", "data/sample.csv")
RESULTS_DIR = os.environ.get("RESULTS_DIR", "resultados")
UNKNOWN = "desconhecido"
