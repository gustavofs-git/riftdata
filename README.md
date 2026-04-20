# DataRift

Riot Games data pipeline — async ingestion, adaptive rate limiting, and Delta Lake storage using a Bronze/Silver Medallion architecture.

DataRift pulls ranked League of Legends data from the Riot API (league entries, summoner profiles, accounts, match details, and timelines), writes raw responses into Bronze Delta tables, then transforms them into 11 normalized Silver tables ready for analysis. The pipeline is orchestrated by Dagster and stores everything locally as Delta Lake tables via Polars + delta-rs.

## Prerequisites

- **Python 3.12+**
- **git**
- **Docker** (optional — for containerized runs)

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/your-org/datarift.git
cd datarift
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
```

### 2. Smoke test (no API key needed)

The smoke test seeds Bronze tables from committed fixtures and runs the full Silver transformation — no Riot API key required.

```bash
make smoke
```

You should see all 11 Silver tables verified in under a second:

```
Smoke test: seeding Bronze from fixtures and running Silver transforms...

All 11 Silver tables verified (0.1s):
  accounts: 3 rows
  league_entries: 3 rows
  ...

Smoke test PASSED.
```

### 3. Real API setup

Get a Riot API key from [developer.riotgames.com](https://developer.riotgames.com) and configure it:

```bash
cp .env.example .env
# Edit .env and set your key:
#   RIOT_API_KEY=RGAPI-xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

### 4. Run with Dagster (local)

Start the Dagster development server:

```bash
make dev
```

Open [http://localhost:3000](http://localhost:3000) in your browser. From the Dagster UI, materialize assets in order:

1. **bronze_extraction** — pulls data from the Riot API into Bronze Delta tables
2. **silver_matches**, **silver_timelines**, **silver_league** — transform Bronze data into 11 Silver tables

### 5. Configure extraction

Edit `config/sample.yaml` to control which region and tiers to pull:

```yaml
region: kr
tiers:
  - CHALLENGER
batch_size: 50
```

Supported regions: `br`, `eune`, `euw`, `jp`, `kr`, `la1`, `la2`, `na`, `oce`, `ph`, `ru`, `sg`, `th`, `tr`, `tw`, `vn`

### 6. Run with Docker

```bash
docker compose up --build
```

This starts the Dagster UI at [http://localhost:3000](http://localhost:3000) with the `data/` directory mounted as a volume. Make sure your `.env` file exists with a valid `RIOT_API_KEY` before starting.

To stop:

```bash
docker compose down
```

## Project Structure

```
datarift/
├── config/
│   └── sample.yaml              # Extraction config (region, tiers, batch_size)
├── scripts/
│   └── smoke.py                 # Smoke test — Bronze fixtures → Silver transforms
├── src/datarift/
│   ├── definitions.py           # Dagster asset definitions (entrypoint)
│   ├── config.py                # ExtractionConfig model + 16 region mappings
│   ├── runner.py                # Async extraction orchestrator
│   ├── extractors.py            # Riot API data extractors
│   ├── riot_client.py           # httpx-based Riot API client with rate limiting
│   ├── riot_client_models.py    # Pydantic models for API responses
│   ├── bronze_writer.py         # Delta Lake writer for Bronze tables
│   ├── silver_match.py          # Bronze → Silver match transforms
│   ├── silver_timeline.py       # Bronze → Silver timeline transforms
│   ├── silver_league.py         # Bronze → Silver league/summoner/account transforms
│   └── logging.py               # structlog configuration
├── tests/
│   ├── fixtures/                # JSON fixtures for smoke + unit tests
│   ├── test_bronze_writer.py
│   ├── test_config.py
│   ├── test_definitions.py
│   ├── test_extractors.py
│   ├── test_logging.py
│   ├── test_rate_limiter.py
│   ├── test_runner.py
│   ├── test_silver_league.py
│   ├── test_silver_match.py
│   ├── test_silver_timeline.py
│   └── test_smoke.py
├── data/                        # Auto-created — Bronze + Silver Delta tables
├── Dockerfile                   # python:3.12-slim container
├── docker-compose.yaml          # Dagster UI on port 3000, data/ volume mount
├── Makefile                     # Task runner
└── pyproject.toml               # Project metadata and dependencies
```

## Makefile Targets

| Target | Description |
|--------|-------------|
| `make smoke` | Run smoke test (no API key needed) |
| `make dev` | Start Dagster dev server on localhost:3000 |
| `make test` | Run pytest suite |
| `make docker-up` | Build and start Docker container |
| `make docker-down` | Stop Docker container |

## Pipeline Architecture

**Bronze layer** — Raw API responses stored as Delta tables with metadata (endpoint, status code, region, timestamp). Six tables: `league_entries_raw`, `accounts_raw`, `summoners_raw`, `match_ids_raw`, `match_details_raw`, `match_timelines_raw`.

**Silver layer** — Normalized, typed tables derived from Bronze. Eleven tables: `matches`, `match_participants`, `match_teams`, `match_teams_bans`, `match_teams_objectives`, `match_timeline_frames`, `match_timeline_participant_frames`, `match_timeline_events`, `league_entries`, `summoners`, `accounts`.

## License

See [pyproject.toml](pyproject.toml) for project metadata.
