"""Tests for ExtractionConfig model and REGION_MAP."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from datarift.config import ExtractionConfig, REGION_MAP


class TestRegionMap:
    def test_all_regions_have_two_hosts(self):
        for code, hosts in REGION_MAP.items():
            assert len(hosts) == 2, f"Region {code} missing host tuple"
            assert hosts[0].startswith("https://"), f"{code} platform_host not https"
            assert hosts[1].startswith("https://"), f"{code} regional_host not https"

    def test_kr_mapping(self):
        assert REGION_MAP["kr"] == (
            "https://kr.api.riotgames.com",
            "https://asia.api.riotgames.com",
        )

    def test_br_mapping(self):
        assert REGION_MAP["br"] == (
            "https://br1.api.riotgames.com",
            "https://americas.api.riotgames.com",
        )

    def test_minimum_region_count(self):
        assert len(REGION_MAP) >= 15


class TestExtractionConfig:
    def test_valid_config(self):
        cfg = ExtractionConfig(region="kr", tiers=["CHALLENGER", "GRANDMASTER"])
        assert cfg.region == "kr"
        assert cfg.tiers == ["CHALLENGER", "GRANDMASTER"]

    def test_default_values(self):
        cfg = ExtractionConfig(region="na", tiers=["GOLD"])
        assert cfg.queue == "RANKED_SOLO_5x5"
        assert cfg.batch_size == 200
        assert cfg.bronze_path == "data/bronze"

    def test_computed_platform_host(self):
        cfg = ExtractionConfig(region="kr", tiers=["IRON"])
        assert cfg.platform_host == "https://kr.api.riotgames.com"

    def test_computed_regional_host(self):
        cfg = ExtractionConfig(region="br", tiers=["IRON"])
        assert cfg.regional_host == "https://americas.api.riotgames.com"

    def test_invalid_region_raises(self):
        with pytest.raises(ValidationError, match="Unknown region"):
            ExtractionConfig(region="invalid", tiers=["IRON"])

    def test_region_normalized_to_lowercase(self):
        cfg = ExtractionConfig(region="KR", tiers=["IRON"])
        assert cfg.region == "kr"

    def test_required_fields(self):
        with pytest.raises(ValidationError):
            ExtractionConfig()  # type: ignore[call-arg]

    def test_custom_overrides(self):
        cfg = ExtractionConfig(
            region="euw",
            tiers=["DIAMOND"],
            queue="RANKED_FLEX_SR",
            batch_size=50,
            bronze_path="/tmp/bronze",
        )
        assert cfg.queue == "RANKED_FLEX_SR"
        assert cfg.batch_size == 50
        assert cfg.bronze_path == "/tmp/bronze"

    def test_strict_mode_default_false(self):
        cfg = ExtractionConfig(region="na", tiers=["GOLD"])
        assert cfg.strict_mode is False

    def test_strict_mode_can_be_enabled(self):
        cfg = ExtractionConfig(region="na", tiers=["GOLD"], strict_mode=True)
        assert cfg.strict_mode is True

    def test_silver_path_default(self):
        cfg = ExtractionConfig(region="na", tiers=["GOLD"])
        assert cfg.silver_path == "data/silver"

    def test_silver_path_custom(self):
        cfg = ExtractionConfig(region="na", tiers=["GOLD"], silver_path="/tmp/silver")
        assert cfg.silver_path == "/tmp/silver"
