CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.matchup_detail (
    match_id            TEXT        NOT NULL,
    champion_a_id       BIGINT      NOT NULL,
    champion_a_name     TEXT        NOT NULL,
    champion_b_id       BIGINT      NOT NULL,
    champion_b_name     TEXT        NOT NULL,
    lane                TEXT        NOT NULL,
    win_a               BOOLEAN,
    kills_a             BIGINT,
    kills_b             BIGINT,
    deaths_a            BIGINT,
    deaths_b            BIGINT,
    assists_a           BIGINT,
    assists_b           BIGINT,
    gold_earned_a       BIGINT,
    gold_earned_b       BIGINT,
    total_damage_dealt_to_champions_a BIGINT,
    total_damage_dealt_to_champions_b BIGINT,
    total_minions_killed_a BIGINT,
    total_minions_killed_b BIGINT,
    vision_score_a      BIGINT,
    vision_score_b      BIGINT,
    PRIMARY KEY (match_id, lane)
);

CREATE TABLE IF NOT EXISTS gold.matchup_intervals (
    match_id            TEXT        NOT NULL,
    champion_a_id       BIGINT      NOT NULL,
    champion_a_name     TEXT        NOT NULL,
    champion_b_id       BIGINT      NOT NULL,
    champion_b_name     TEXT        NOT NULL,
    lane                TEXT        NOT NULL,
    interval_min        BIGINT      NOT NULL,
    total_gold_a        BIGINT,
    total_gold_b        BIGINT,
    xp_a                BIGINT,
    xp_b                BIGINT,
    level_a             BIGINT,
    level_b             BIGINT,
    minions_killed_a    BIGINT,
    minions_killed_b    BIGINT,
    jungle_minions_killed_a BIGINT,
    jungle_minions_killed_b BIGINT,
    current_gold_a      BIGINT,
    current_gold_b      BIGINT,
    PRIMARY KEY (match_id, lane, interval_min)
);

CREATE TABLE IF NOT EXISTS gold.matchup_aggregates (
    champion_a_id       BIGINT          NOT NULL,
    champion_a_name     TEXT            NOT NULL,
    champion_b_id       BIGINT          NOT NULL,
    champion_b_name     TEXT            NOT NULL,
    lane                TEXT            NOT NULL,
    interval_min        BIGINT,
    patch               TEXT            NOT NULL,
    tier                TEXT            NOT NULL,
    avg_kills_a         DOUBLE PRECISION,
    avg_kills_b         DOUBLE PRECISION,
    avg_deaths_a        DOUBLE PRECISION,
    avg_deaths_b        DOUBLE PRECISION,
    avg_assists_a       DOUBLE PRECISION,
    avg_assists_b       DOUBLE PRECISION,
    avg_gold_earned_a   DOUBLE PRECISION,
    avg_gold_earned_b   DOUBLE PRECISION,
    avg_total_damage_dealt_to_champions_a DOUBLE PRECISION,
    avg_total_damage_dealt_to_champions_b DOUBLE PRECISION,
    avg_total_minions_killed_a DOUBLE PRECISION,
    avg_total_minions_killed_b DOUBLE PRECISION,
    avg_vision_score_a  DOUBLE PRECISION,
    avg_vision_score_b  DOUBLE PRECISION,
    avg_total_gold_a    DOUBLE PRECISION,
    avg_total_gold_b    DOUBLE PRECISION,
    avg_xp_a            DOUBLE PRECISION,
    avg_xp_b            DOUBLE PRECISION,
    avg_level_a         DOUBLE PRECISION,
    avg_level_b         DOUBLE PRECISION,
    avg_minions_killed_a DOUBLE PRECISION,
    avg_minions_killed_b DOUBLE PRECISION,
    avg_jungle_minions_killed_a DOUBLE PRECISION,
    avg_jungle_minions_killed_b DOUBLE PRECISION,
    avg_current_gold_a  DOUBLE PRECISION,
    avg_current_gold_b  DOUBLE PRECISION,
    win_rate_a          DOUBLE PRECISION,
    sample_size         BIGINT,
    PRIMARY KEY (champion_a_id, champion_b_id, lane, interval_min, patch, tier)
);

CREATE INDEX IF NOT EXISTS idx_matchup_agg_champ_lane
    ON gold.matchup_aggregates (champion_a_id, champion_b_id, lane);

CREATE INDEX IF NOT EXISTS idx_matchup_agg_patch_tier
    ON gold.matchup_aggregates (patch, tier);
