"""Export Gold Delta tables to Postgres via COPY FROM STDIN."""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import polars as pl
import psycopg

DEFAULT_PG_URL = "postgresql://datarift:datarift@localhost:5432/datarift"

GOLD_TABLES = [
    "matchup_detail",
    "matchup_intervals",
    "matchup_aggregates",
]


def _df_to_csv_buffer(df: pl.DataFrame) -> io.BytesIO:
    buf = io.BytesIO()
    for row in df.iter_rows():
        fields: list[str] = []
        for val in row:
            if val is None:
                fields.append("")
            elif isinstance(val, bool):
                fields.append("t" if val else "f")
            else:
                s = str(val)
                if "," in s or '"' in s or "\n" in s:
                    s = '"' + s.replace('"', '""') + '"'
                fields.append(s)
        buf.write((",".join(fields) + "\n").encode())
    buf.seek(0)
    return buf


def main() -> int:
    parser = argparse.ArgumentParser(description="Export Gold Delta tables to Postgres")
    parser.add_argument("--gold-path", required=True, help="Path to Gold Delta tables directory")
    parser.add_argument("--pg-url", default=DEFAULT_PG_URL, help="Postgres connection URL")
    args = parser.parse_args()

    gold_path = Path(args.gold_path)
    ddl_path = Path(__file__).resolve().parent.parent / "sql" / "gold_schema.sql"

    if not ddl_path.exists():
        print(f"ERROR: DDL not found at {ddl_path}", file=sys.stderr)
        return 1

    ddl_sql = ddl_path.read_text()

    try:
        conn = psycopg.connect(args.pg_url, autocommit=False)
    except psycopg.OperationalError as e:
        print(f"ERROR: Cannot connect to Postgres: {e}", file=sys.stderr)
        return 1

    try:
        with conn.cursor() as cur:
            cur.execute(ddl_sql)

            for table_name in GOLD_TABLES:
                table_path = gold_path / table_name
                if not table_path.exists():
                    print(f"ERROR: Delta table not found at {table_path}", file=sys.stderr)
                    conn.rollback()
                    return 1

                df = pl.read_delta(str(table_path))
                columns = df.columns

                cur.execute(f"TRUNCATE gold.{table_name}")

                csv_buf = _df_to_csv_buffer(df)
                col_list = ", ".join(columns)
                copy_sql = f"COPY gold.{table_name} ({col_list}) FROM STDIN WITH (FORMAT csv)"

                with cur.copy(copy_sql) as copy:
                    while chunk := csv_buf.read(65536):
                        copy.write(chunk)

                print(f"{table_name}: {len(df)} rows exported")

        conn.commit()
        print("Export complete.")
        return 0

    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        conn.rollback()
        return 1
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
