# DataRift Architecture

## Overview

DataRift is a **League of Legends ranked data pipeline** that extracts player and match data from the Riot Games API, stores it as raw JSON in a Bronze layer, then transforms it into typed relational tables in a Silver layer. All storage uses **Delta Lake** (Parquet + transaction log) for ACID writes, schema enforcement, and idempotent MERGE operations.

Orchestration is handled by **Dagster**, which manages dependencies between assets, provides a web UI for materialization, and supports individual asset retries.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         Dagster Orchestrator                         │
│                    (definitions.py — 17 assets)                      │
└────────────────────────────┬────────────────────────────────────────┘
                             │
          ┌──────────────────┼──────────────────┐
          ▼                  ▼                  ▼
   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
   │   Bronze    │   │   Bronze    │   │   Bronze    │
   │  Extraction │   │  Extraction │   │  Extraction │
   │ (6 assets)  │   │  (async)    │   │ (rate-ltd)  │
   └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
          │                  │                  │
          ▼                  ▼                  ▼
   ┌────────────────────────────────────────────────┐
   │            data/bronze/ (Delta Lake)            │
   │  6 tables: league_entries_raw, accounts_raw,   │
   │  summoners_raw, match_ids_raw,                 │
   │  match_details_raw, match_timelines_raw        │
   └────────────────────────┬───────────────────────┘
                            │
                            ▼
   ┌────────────────────────────────────────────────┐
   │         Silver Transforms (11 assets)          │
   │  Polars DataFrames — JSON parsing, flattening, │
   │  array explosion, type casting                 │
   └────────────────────────┬───────────────────────┘
                            │
                            ▼
   ┌────────────────────────────────────────────────┐
   │            data/silver/ (Delta Lake)            │
   │  11 tables: matches, match_participants,       │
   │  match_teams, match_teams_bans,                │
   │  match_teams_objectives, match_timeline_frames,│
   │  match_timeline_participant_frames,            │
   │  match_timeline_events, league_entries,        │
   │  summoners, accounts                           │
   └────────────────────────────────────────────────┘
```

---

## Layers

### Bronze (Raw Extraction)

**Purpose:** Capture raw API responses exactly as received. No transformation, no data loss.

| Table | Source Endpoint | Key | Rows |
|-------|----------------|-----|------|
| `league_entries_raw` | `/lol/league-exp/v4/entries` | puuid | 300 |
| `accounts_raw` | `/riot/account/v1/accounts/by-puuid` | puuid | 300 |
| `summoners_raw` | `/lol/summoner/v4/summoners/by-puuid` | puuid | 300 |
| `match_ids_raw` | `/lol/match/v5/matches/by-puuid/.../ids` | puuid | 300 |
| `match_details_raw` | `/lol/match/v5/matches/{matchId}` | match_id | 3,389 |
| `match_timelines_raw` | `/lol/match/v5/matches/{matchId}/timeline` | match_id | 3,389 |

**Schema (all tables):**
```
primary_key (Utf8) | raw_json (Utf8) | ingested_at (Timestamp) | endpoint (Utf8) | status_code (Int32) | region (Utf8)
```

**Key design choices:**
- **Anti-join resume:** Before extraction, `BronzeWriter.existing_keys()` reads all primary keys from the Delta table. Only missing keys are fetched. This makes the pipeline SIGINT-safe and restart-free.
- **Append-only writes:** New rows are appended (no MERGE needed at Bronze level).
- **Rate limiting:** `RiotRateLimiter` tracks both app-level and per-method sliding windows, auto-adjusting from response headers. Concurrency capped at 10 parallel requests.

### Silver (Typed Relational Tables)

**Purpose:** Parse raw JSON into typed, query-ready relational tables. One table per analytical entity.

| Table | Source Bronze | Rows | Grain |
|-------|-------------|------|-------|
| `matches` | match_details_raw | 3,389 | 1 per match |
| `match_participants` | match_details_raw | 34,260 | 1 per match × participant (10/match) |
| `match_teams` | match_details_raw | 6,774 | 1 per match × team (2/match) |
| `match_teams_bans` | match_details_raw | 34,050 | 1 per match × team × ban |
| `match_teams_objectives` | match_details_raw | 54,192 | 1 per match × team × objective type |
| `match_timeline_frames` | match_timelines_raw | 90,086 | 1 per match × frame (~26/match) |
| `match_timeline_participant_frames` | match_timelines_raw | 911,232 | 1 per match × frame × participant |
| `match_timeline_events` | match_timelines_raw | 3,829,352 | 1 per match × frame × event |
| `league_entries` | league_entries_raw | 300 | 1 per puuid |
| `summoners` | summoners_raw | 300 | 1 per puuid |
| `accounts` | accounts_raw | 300 | 1 per puuid |

**Key design choices:**
- **Delta MERGE (upsert):** `write_silver()` uses `MERGE INTO ... WHEN NOT MATCHED THEN INSERT` for idempotent writes. Re-running a Silver asset on the same data produces no duplicates.
- **Chunked processing:** High-cardinality assets (`match_participants`, all 3 timeline tables) process `silver_batch_size` Bronze rows per batch with explicit `gc.collect()` between batches. This caps peak memory at ~400MB regardless of dataset size.
- **Transform patterns:**
  - Simple scalars: `json_path_match("$.path") + .cast()` (native Polars, zero Python overhead)
  - Arrays/nested: `json.loads()` + Python dict traversal + DataFrame construction (when JSONPath can't explode arrays)
  - Booleans: String equality comparison (`== "true"`) since Polars 1.39 can't cast Utf8→Boolean directly

---

## Dependency Graph

```
bronze_league_entries (root)
├── bronze_accounts ──────────────── → silver_accounts
├── bronze_summoners ─────────────── → silver_summoners
├── bronze_match_ids
│   ├── bronze_match_details ─────── → silver_matches
│   │                                  silver_match_participants (chunked)
│   │                                  silver_match_teams
│   │                                  silver_match_teams_bans
│   │                                  silver_match_teams_objectives
│   └── bronze_match_timelines ───── → silver_match_timeline_frames (chunked)
│                                      silver_match_timeline_participant_frames (chunked)
│                                      silver_match_timeline_events (chunked)
└── silver_league_entries
```

---

## Key Components

| File | Responsibility |
|------|---------------|
| `config.py` | `ExtractionConfig` Pydantic model — region routing, batch sizes, paths |
| `riot_client.py` | `RiotRateLimiter` — async HTTP + dual-layer sliding-window rate limiter |
| `bronze_writer.py` | `BronzeWriter` — Delta append + anti-join resume via `existing_keys()` |
| `extractors.py` | 6 async extraction functions (one per Bronze entity) |
| `silver_match.py` | Match-detail transforms + `write_silver()` Delta MERGE helper |
| `silver_timeline.py` | Timeline transforms (frames, participant_frames, events) |
| `silver_league.py` | League/summoner/account transforms |
| `logging.py` | structlog configuration per asset invocation |
| `definitions.py` | Dagster asset definitions, `_materialize_silver()` helper, job config |

---

## Configuration

**`config/sample.yaml`:**
```yaml
region: kr          # Riot API region (routes to correct platform/regional hosts)
tiers:
  - CHALLENGER      # Which ranked tiers to extract
batch_size: 50      # Bronze: rows per API write batch
silver_batch_size: 100  # Silver: Bronze rows per chunked batch (memory cap)
```

---

## Memory Management

The pipeline is designed to run on constrained hardware (8GB RAM VPS target):

| Mechanism | Where | Effect |
|-----------|-------|--------|
| `silver_batch_size: 100` | config | Processes 100 Bronze rows at a time (~200-400MB peak) |
| `gc.collect()` per batch | `_materialize_silver()` | Forces immediate memory reclaim |
| `in_process_executor` | Dagster job | Prevents parallel Silver materializations from stacking memory |
| Anti-join resume | Bronze extraction | Avoids re-loading already-fetched data |
| `chunked=True` flag | Heavy Silver assets only | Only applied where expansion ratio is high (timelines, participants) |

---

## Data Flow (end-to-end for a single match)

1. **League entries** → get puuids of CHALLENGER players in KR
2. **Match IDs** → for each puuid, fetch recent ranked match IDs
3. **Match details** → for each unique match_id, fetch full match JSON (~50KB)
4. **Match timelines** → for each match_id, fetch frame-by-frame timeline (~130KB)
5. **Silver transforms** → parse each raw JSON into 11 typed tables

---

## Running

```bash
# Start Dagster web UI
dagster dev -m datarift.definitions

# Materialize everything
# In the Dagster UI: select all assets → Materialize

# Or via CLI
dagster asset materialize --select '*' -m datarift.definitions
```

**Environment variables:**
- `RIOT_API_KEY` — Required. Riot Games API key.
- `DAGSTER_HOME` — Optional. Defaults to temp directory.

---

## Production Deployment (2 vCPU / 8GB RAM / 100GB NVMe)

The current dataset (Bronze + Silver) occupies ~550MB on disk. The chunked Silver processing peaks at ~2GB total memory (OS + Dagster + transform batch + Arrow overhead). The VPS has comfortable headroom.

**Bottleneck:** CPU-bound `json.loads()` loops on 2 vCPUs. Timeline Silver processing takes ~10-15 minutes for 3,389 matches. Not a problem — just slower than a workstation.

**Scaling concern:** If the dataset grows 10x (30k+ matches), consider:
- Reducing `silver_batch_size` to 50
- Partitioning timeline tables by match_id range
- Adding a Gold layer for pre-aggregated analytics
