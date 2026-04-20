"""datarift — Riot Games data pipeline with async ingestion and Delta Lake storage."""

__version__ = "0.1.0"

from datarift.riot_client import RiotRateLimiter

__all__ = ["RiotRateLimiter"]
