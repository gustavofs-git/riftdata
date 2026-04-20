"""Delta Lake writer for Bronze-layer raw JSON storage with anti-join resume."""

from __future__ import annotations

from datetime import datetime, timezone

import polars as pl
from deltalake import DeltaTable, write_deltalake


class BronzeWriter:
    """Writes raw API responses to Delta tables and supports anti-join resume via existing keys."""

    def __init__(self, table_name: str, primary_key_col: str, base_path: str) -> None:
        self._table_name = table_name
        self._primary_key_col = primary_key_col
        self._base_path = base_path

    @property
    def table_path(self) -> str:
        return f"{self._base_path}/{self._table_name}"

    def write_batch(
        self,
        records: list[dict],
        endpoint: str,
        status_code: int,
        region: str,
    ) -> None:
        """Write a batch of records to the Delta table (append mode).

        Each dict in *records* must contain the primary key value under
        ``self._primary_key_col`` and the raw JSON string under ``raw_json``.
        """
        now = datetime.now(timezone.utc)
        df = pl.DataFrame(
            {
                self._primary_key_col: [r[self._primary_key_col] for r in records],
                "raw_json": [r["raw_json"] for r in records],
                "ingested_at": [now] * len(records),
                "endpoint": [endpoint] * len(records),
                "status_code": [status_code] * len(records),
                "region": [region] * len(records),
            }
        )
        write_deltalake(self.table_path, df.to_arrow(), mode="append")

    def existing_keys(self) -> set[str]:
        """Return the set of primary-key values already stored in the Delta table.

        Returns an empty set when the table does not yet exist.
        """
        try:
            dt = DeltaTable(self.table_path)
            df = pl.DataFrame(dt.to_pyarrow_table(columns=[self._primary_key_col]))
            return set(df[self._primary_key_col].to_list())
        except Exception:
            # Table doesn't exist yet — deltalake raises TableNotFoundError
            return set()
