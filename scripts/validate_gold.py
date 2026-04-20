"""DuckDB cross-validation: assert Gold matchup_aggregates is consistent with Silver source tables."""

from __future__ import annotations

import argparse
import sys

import duckdb


def validate_gold(base_path: str) -> None:
    conn = duckdb.connect()
    conn.install_extension("delta")
    conn.load_extension("delta")

    gold_path = f"{base_path}/gold/matchup_aggregates"
    silver_mp = f"{base_path}/silver/match_participants"
    silver_matches = f"{base_path}/silver/matches"
    silver_league = f"{base_path}/silver/league_entries"

    gold_row_count = conn.sql(
        f"SELECT COUNT(*) FROM delta_scan('{gold_path}')"
    ).fetchone()[0]
    assert gold_row_count > 0, "Gold matchup_aggregates has 0 rows"
    print(f"Gold matchup_aggregates: {gold_row_count} rows")

    silver_agg_sql = f"""
        WITH matchup_pairs AS (
            SELECT
                a.match_id,
                a.champion_id AS champion_a_id,
                a.champion_name AS champion_a_name,
                b.champion_id AS champion_b_id,
                b.champion_name AS champion_b_name,
                a.team_position AS lane
            FROM delta_scan('{silver_mp}') a
            JOIN delta_scan('{silver_mp}') b
                ON a.match_id = b.match_id
                AND a.team_position = b.team_position
                AND a.team_id = 100
                AND b.team_id = 200
            WHERE a.team_position IS NOT NULL
              AND a.team_position != ''
        ),
        match_patches AS (
            SELECT
                match_id,
                COALESCE(
                    split_part(game_version, '.', 1) || '.' || split_part(game_version, '.', 2),
                    'UNKNOWN'
                ) AS patch
            FROM delta_scan('{silver_matches}')
        ),
        participant_tiers AS (
            SELECT
                mp.match_id,
                COALESCE(le.tier, 'UNKNOWN') AS tier
            FROM delta_scan('{silver_mp}') mp
            LEFT JOIN delta_scan('{silver_league}') le
                ON mp.puuid = le.puuid
        ),
        modal_tier AS (
            SELECT match_id, tier
            FROM (
                SELECT
                    match_id,
                    tier,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (PARTITION BY match_id ORDER BY COUNT(*) DESC) AS rn
                FROM participant_tiers
                GROUP BY match_id, tier
            )
            WHERE rn = 1
        ),
        detail_with_meta AS (
            SELECT
                mp.match_id,
                mp.champion_a_id,
                mp.champion_a_name,
                mp.champion_b_id,
                mp.champion_b_name,
                mp.lane,
                COALESCE(p.patch, 'UNKNOWN') AS patch,
                COALESCE(mt.tier, 'UNKNOWN') AS tier
            FROM matchup_pairs mp
            LEFT JOIN match_patches p ON mp.match_id = p.match_id
            LEFT JOIN modal_tier mt ON mp.match_id = mt.match_id
        )
        SELECT
            champion_a_id,
            champion_a_name,
            champion_b_id,
            champion_b_name,
            lane,
            patch,
            tier,
            COUNT(*) AS sample_size
        FROM detail_with_meta
        GROUP BY champion_a_id, champion_a_name, champion_b_id, champion_b_name, lane, patch, tier
    """

    silver_rows = conn.sql(silver_agg_sql).fetchall()
    silver_row_count = len(silver_rows)
    print(f"Silver-derived aggregation: {silver_row_count} rows")

    gold_collapsed_rows = conn.sql(f"""
        SELECT
            champion_a_id, champion_a_name,
            champion_b_id, champion_b_name,
            lane, patch, tier,
            MAX(sample_size) AS sample_size
        FROM delta_scan('{gold_path}')
        GROUP BY champion_a_id, champion_a_name, champion_b_id, champion_b_name, lane, patch, tier
    """).fetchall()

    gold_collapsed_count = len(gold_collapsed_rows)
    print(f"Gold collapsed (ignoring interval_min): {gold_collapsed_count} rows")

    assert silver_row_count == gold_collapsed_count, (
        f"Row count mismatch: Silver-derived={silver_row_count}, "
        f"Gold (collapsed)={gold_collapsed_count}"
    )
    print("Row count consistency: PASSED")

    if silver_rows and gold_collapsed_rows:
        sr = silver_rows[0]
        champ_a, champ_a_name, champ_b, champ_b_name, lane, patch, tier, silver_size = sr

        gold_matches = [
            r for r in gold_collapsed_rows
            if r[0] == champ_a and r[2] == champ_b and r[4] == lane
            and r[5] == patch and r[6] == tier
        ]
        assert len(gold_matches) == 1, (
            f"Spot-check: expected 1 Gold row for "
            f"({champ_a}, {champ_b}, {lane}, {patch}, {tier}), got {len(gold_matches)}"
        )
        gold_size = gold_matches[0][7]
        assert silver_size == gold_size, (
            f"Spot-check sample_size mismatch: Silver={silver_size}, Gold={gold_size} "
            f"for ({champ_a} vs {champ_b}, {lane}, {patch}, {tier})"
        )
        print(
            f"Spot-check ({champ_a} vs {champ_b}, {lane}, {patch}, {tier}): "
            f"sample_size={silver_size} — PASSED"
        )

    print("\nAll Gold-Silver consistency checks PASSED.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate Gold-Silver consistency via DuckDB")
    parser.add_argument("--base-path", default="data", help="Base path for Delta tables")
    args = parser.parse_args()
    try:
        validate_gold(args.base_path)
    except Exception as e:
        print(f"VALIDATION FAILED: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
