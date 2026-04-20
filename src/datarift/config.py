"""Extraction configuration model with Riot API region routing."""

from __future__ import annotations

from pydantic import BaseModel, field_validator, computed_field

# Maps region code → (platform_host, regional_host)
REGION_MAP: dict[str, tuple[str, str]] = {
    "br": ("https://br1.api.riotgames.com", "https://americas.api.riotgames.com"),
    "eune": ("https://eun1.api.riotgames.com", "https://europe.api.riotgames.com"),
    "euw": ("https://euw1.api.riotgames.com", "https://europe.api.riotgames.com"),
    "jp": ("https://jp1.api.riotgames.com", "https://asia.api.riotgames.com"),
    "kr": ("https://kr.api.riotgames.com", "https://asia.api.riotgames.com"),
    "la1": ("https://la1.api.riotgames.com", "https://americas.api.riotgames.com"),
    "la2": ("https://la2.api.riotgames.com", "https://americas.api.riotgames.com"),
    "na": ("https://na1.api.riotgames.com", "https://americas.api.riotgames.com"),
    "oce": ("https://oc1.api.riotgames.com", "https://sea.api.riotgames.com"),
    "ph": ("https://ph2.api.riotgames.com", "https://sea.api.riotgames.com"),
    "ru": ("https://ru.api.riotgames.com", "https://europe.api.riotgames.com"),
    "sg": ("https://sg2.api.riotgames.com", "https://sea.api.riotgames.com"),
    "th": ("https://th2.api.riotgames.com", "https://sea.api.riotgames.com"),
    "tr": ("https://tr1.api.riotgames.com", "https://europe.api.riotgames.com"),
    "tw": ("https://tw2.api.riotgames.com", "https://sea.api.riotgames.com"),
    "vn": ("https://vn2.api.riotgames.com", "https://sea.api.riotgames.com"),
}


class ExtractionConfig(BaseModel):
    """Configuration for a Bronze extraction run."""

    region: str
    tiers: list[str]
    queue: str = "RANKED_SOLO_5x5"
    batch_size: int = 200
    bronze_path: str = "data/bronze"
    silver_path: str = "data/silver"
    strict_mode: bool = False

    @field_validator("region")
    @classmethod
    def _validate_region(cls, v: str) -> str:
        v = v.lower()
        if v not in REGION_MAP:
            raise ValueError(
                f"Unknown region {v!r}. Valid regions: {sorted(REGION_MAP)}"
            )
        return v

    @computed_field  # type: ignore[prop-decorator]
    @property
    def platform_host(self) -> str:
        return REGION_MAP[self.region][0]

    @computed_field  # type: ignore[prop-decorator]
    @property
    def regional_host(self) -> str:
        return REGION_MAP[self.region][1]
