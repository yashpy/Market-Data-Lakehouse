"""The incremental upsert — the heart of the correctness guarantee.

For each business key ``(symbol, event_time)`` we keep exactly one row: the one with the
highest ``ingest_seq`` (the most recently *arrived* version). This makes the model:

  * idempotent  — re-running or re-delivering never creates duplicates, and
  * late-safe    — an out-of-order event cannot overwrite a newer one, because ordering is
                   decided by arrival sequence, not by arrival time.

The SQL below is the DuckDB form of the dbt incremental MERGE in ``dbt/models``.
"""

from __future__ import annotations


def build_curated(con, target: str = "fct_latest_price", source: str = "raw_ticks") -> None:
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {target} AS
        SELECT symbol, event_time, price, ingest_seq
        FROM (
            SELECT
                symbol, event_time, price, ingest_seq,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol, event_time
                    ORDER BY ingest_seq DESC
                ) AS rn
            FROM {source}
        )
        WHERE rn = 1
        """
    )


def duplicate_count(con, target: str = "fct_latest_price") -> int:
    return con.execute(
        f"""
        SELECT COALESCE(SUM(c - 1), 0) FROM (
            SELECT COUNT(*) AS c FROM {target} GROUP BY symbol, event_time HAVING COUNT(*) > 1
        )
        """
    ).fetchone()[0]


def stale_rows(con, target: str = "fct_latest_price", source: str = "raw_ticks") -> int:
    """Rows in curated whose ingest_seq is NOT the max for their key -> should be 0."""
    return con.execute(
        f"""
        SELECT COUNT(*)
        FROM {target} t
        JOIN (
            SELECT symbol, event_time, MAX(ingest_seq) AS max_seq
            FROM {source} GROUP BY symbol, event_time
        ) m ON t.symbol = m.symbol AND t.event_time = m.event_time
        WHERE t.ingest_seq <> m.max_seq
        """
    ).fetchone()[0]
