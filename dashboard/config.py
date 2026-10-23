"""Dashboard configuration - paths, DB settings, cache TTLs.

All values come from environment variables loaded via dotenv (.env) with
safe defaults. No secrets are printed or hardcoded.
"""
from pathlib import Path

from dotenv import load_dotenv
import os

REPO = Path(__file__).resolve().parent.parent
load_dotenv(REPO / ".env")

# Postgres (analytics serving layer)
ANALYTICS_HOST = os.getenv("ANALYTICS_POSTGRES_HOST", "localhost")
ANALYTICS_PORT = int(os.getenv("ANALYTICS_POSTGRES_PORT", "15433"))
ANALYTICS_DB = os.getenv("ANALYTICS_POSTGRES_DB", "analytics")
ANALYTICS_USER = os.getenv("ANALYTICS_POSTGRES_USER", "postgres")
ANALYTICS_PASSWORD = os.getenv("ANALYTICS_POSTGRES_PASSWORD", "")  # from .env only

# File-based data
EXPORTS = REPO / "exports"
BATCH_METRICS = REPO / "streaming_lake" / "_metrics" / "batch_metrics.jsonl"

# Service endpoints
CONNECT_URL = "http://localhost:8083"
KSQL_URL = "http://localhost:8088"
CONSOLE_URL = "http://localhost:8084"
SPARK_UI_URL = "http://localhost:4040"

PRIORITY_ORDER = ["HIGH_PRIORITY", "MEDIUM_PRIORITY", "LOW_PRIORITY"]

MIGRATION_NOTE = "Dashboard reads only: analytics Postgres + Delta-table exports + local run files."
