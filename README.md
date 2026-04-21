# DataRift

Riot Games data pipeline — async ingestion, adaptive rate limiting, and Delta Lake storage using a Bronze/Silver/Gold Medallion architecture with Postgres export.

DataRift pulls ranked League of Legends data from the Riot API (league entries, summoner profiles, accounts, match details, and timelines), writes raw responses into Bronze Delta tables, transforms them into 11 normalized Silver tables, then aggregates champion-vs-champion matchup statistics in 3 Gold tables. The pipeline is orchestrated by Dagster (20 assets) and stores everything locally as Delta Lake tables via Polars + delta-rs. Gold tables can be exported to Postgres for serving.

## Prerequisites

- **Python 3.12+**
- **git**
- **Docker** (optional — for containerized runs and Postgres)

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/gustavofs-git/riftdata.git
cd riftdata
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

### 2. Explore the data (no API key needed)

Bronze and Silver Delta tables are included in the `data/` directory. You can query them directly with Polars or DuckDB without running any extraction:

```python
import polars as pl

# Read Silver match participants
df = pl.read_delta("data/silver/match_participants")
print(df.shape)  # (34260, 63)

# Champion pick rates
df.group_by("champion_name").len().sort("len", descending=True).head(10)
```

### 3. Smoke test

The smoke test seeds Bronze tables from committed fixtures and runs the full Silver + Gold transformation pipeline — no Riot API key required.

```bash
make smoke
```

### 4. Real API setup

Get a Riot API key from [developer.riotgames.com](https://developer.riotgames.com) and configure it:

```bash
cp .env.example .env
# Edit .env and set your key:
#   RIOT_API_KEY=RGAPI-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

### 5. Run with Dagster (local)

Start the Dagster development server:

```bash
make dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser. The asset graph shows each extraction entity as an independent asset you can materialize, monitor, and retry individually.

**Bronze assets** (6 — one per API entity, materialize in dependency order):

1. **bronze_league_entries** — league entries from League-Exp-V4 (produces puuids)
2. **bronze_accounts** — account data from Account-V1 (depends on league entries)
3. **bronze_summoners** — summoner profiles from Summoner-V4 (depends on league entries)
4. **bronze_match_ids** — match ID lists from Match-V5 (depends on league entries)
5. **bronze_match_details** — match detail data from Match-V5 (depends on match IDs)
6. **bronze_match_timelines** — match timeline data from Match-V5 (depends on match IDs)

**Silver assets** (11 — transform Bronze into normalized tables):

7. **silver_matches** — match-level metadata and game info
8. **silver_match_participants** — one row per match x participant
9. **silver_match_teams** — one row per match x team
10. **silver_match_teams_bans** — one row per match x team x ban
11. **silver_match_teams_objectives** — one row per match x team x objective
12. **silver_match_timeline_frames** — one row per match x frame
13. **silver_match_timeline_participant_frames** — one row per match x frame x participant
14. **silver_match_timeline_events** — one row per match x frame x event
15. **silver_league_entries** — tier, rank, LP per puuid
16. **silver_summoners** — summoner profiles
17. **silver_accounts** — game name and tag

**Gold assets** (3 — champion-vs-champion matchup analytics):

18. **gold_matchup_detail** — one row per match x lane with stats for both champions
19. **gold_matchup_intervals** — per-interval (5/10/15/20 min) stat snapshots per matchup (gold, XP, CS, level)
20. **gold_matchup_aggregates** — aggregated win rates and averages per champion pair, lane, patch, and tier

Each asset logs human-readable progress to the Dagster UI.

### 6. Postgres export (optional)

Start Postgres and export Gold tables:

```bash
docker compose up -d postgres
python scripts/export_to_postgres.py
```

This applies the schema from `sql/gold_schema.sql` and bulk-loads all 3 Gold tables via `COPY FROM STDIN`. Default connection: `postgresql://datarift:datarift@localhost:5432/datarift`.

### 7. Configure extraction

Edit `config/sample.yaml` to control which region, tiers, and batch size to pull:

```yaml
region: br
tiers:
  - CHALLENGER
  - GRANDMASTER
  - MASTER
batch_size: 200
```

Supported regions: `br`, `eune`, `euw`, `jp`, `kr`, `la1`, `la2`, `na`, `oce`, `ph`, `ru`, `sg`, `th`, `tr`, `tw`, `vn`

### 8. Run with Docker

```bash
docker compose up --build
```

This starts the Dagster UI at [http://localhost:3000](http://localhost:3000) with the `data/` directory mounted as a volume and a Postgres 16 instance for Gold export. Make sure your `.env` file exists with a valid `RIOT_API_KEY` before starting.

## Project Structure

```
datarift/
├── config/
│   └── sample.yaml                # Extraction config (region, tiers, batch_size)
├── sql/
│   └── gold_schema.sql            # Postgres DDL for Gold tables (3 tables + indexes)
├── scripts/
│   ├── smoke.py                   # Smoke test — Bronze fixtures -> Silver + Gold transforms
│   ├── export_to_postgres.py      # Delta Gold -> Postgres COPY FROM STDIN bulk export
│   └── validate_gold.py           # DuckDB cross-validation: Gold aggregates vs Silver
├── src/datarift/
│   ├── definitions.py             # Dagster asset definitions (20 assets, entrypoint)
│   ├── config.py                  # ExtractionConfig model + 16 region mappings
│   ├── runner.py                  # Standalone async extraction orchestrator
│   ├── extractors.py              # Riot API data extractors (one per entity)
│   ├── riot_client.py             # httpx-based Riot API client with rate limiting
│   ├── riot_client_models.py      # Pydantic models for API responses
│   ├── bronze_writer.py           # Delta Lake writer for Bronze tables
│   ├── silver_match.py            # Bronze -> Silver match transforms
│   ├── silver_timeline.py         # Bronze -> Silver timeline transforms
│   ├── silver_league.py           # Bronze -> Silver league/summoner/account transforms
│   ├── gold_matchup.py            # Silver -> Gold matchup transforms (detail, intervals, aggregates)
│   └── logging.py                 # structlog configuration
├── tests/
│   ├── fixtures/                  # JSON fixtures for smoke + unit tests
│   ├── test_gold_matchup.py       # Gold transform unit tests
│   ├── test_postgres_export.py    # Postgres integration tests
│   ├── test_smoke.py              # End-to-end smoke + DuckDB Gold-Silver validation
│   └── ...                        # Bronze, Silver, config, extractor tests
├── data/
│   ├── bronze/                    # Raw API responses (6 Delta tables)
│   └── silver/                    # Normalized typed tables (11 Delta tables)
├── Dockerfile
├── docker-compose.yaml            # Dagster + Postgres 16
├── Makefile
└── pyproject.toml
```

## Pipeline Architecture

**Bronze layer** — Raw API responses stored as Delta tables with metadata (endpoint, status code, region, timestamp). Six tables: `league_entries_raw`, `accounts_raw`, `summoners_raw`, `match_ids_raw`, `match_details_raw`, `match_timelines_raw`.

**Silver layer** — Normalized, typed tables derived from Bronze via JSON parsing, flattening, and array explosion. Eleven tables covering matches, participants, teams, bans, objectives, timeline frames, timeline events, league entries, summoners, and accounts. Uses Delta MERGE for idempotent writes.

**Gold layer** — Pre-aggregated champion-vs-champion matchup statistics derived from Silver. Three tables:

- **matchup_detail** — per-match, per-lane stats for both champions (kills, deaths, assists, gold, damage, CS, vision)
- **matchup_intervals** — per-interval snapshots (5/10/15/20 min) with gold, XP, level, and CS using nearest-frame lookup from timeline data
- **matchup_aggregates** — averaged stats and win rates grouped by champion pair, lane, patch, and tier

Gold tables are full-recompute (overwrite mode) and can be exported to Postgres for serving via `scripts/export_to_postgres.py`.

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make smoke` | Run smoke test (no API key needed) |
| `make dev` | Start Dagster dev server on localhost:3000 |
| `make test` | Run pytest suite |
| `make docker-up` | Build and start Docker containers (Dagster + Postgres) |
| `make docker-down` | Stop Docker containers |

## License

See [pyproject.toml](pyproject.toml) for project metadata.
