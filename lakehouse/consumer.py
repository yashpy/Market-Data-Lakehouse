"""Stream consumer: land validated ticks into the raw table.

In production this is a Kafka consumer writing to a Snowflake landing table. Locally it
writes to a DuckDB connection. Landing is append-only and keeps every arrival (including
duplicates/late) — correctness is resolved downstream in the incremental transform.
"""

from __future__ import annotations

from typing import Iterable

from .producer import Tick


def ensure_raw_table(con) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS raw_ticks (
            symbol      VARCHAR,
            event_time  TIMESTAMP,
            price       DOUBLE,
            ingest_seq  BIGINT,
            arrived_at  TIMESTAMP DEFAULT now()
        )
        """
    )


def land(con, ticks: Iterable[Tick]) -> int:
    rows = [(t.symbol, t.event_time, t.price, t.ingest_seq) for t in ticks]
    con.executemany(
        "INSERT INTO raw_ticks (symbol, event_time, price, ingest_seq) VALUES (?, ?, ?, ?)",
        rows,
    )
    return len(rows)
