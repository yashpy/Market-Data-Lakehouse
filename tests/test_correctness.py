"""Prove the dedup + late-event correctness guarantees over DuckDB."""

import duckdb
import pytest

from lakehouse import consumer, producer, transform
from lakehouse.producer import Tick


@pytest.fixture
def con():
    c = duckdb.connect(":memory:")
    consumer.ensure_raw_table(c)
    return c


def test_duplicate_events_collapse_to_one(con):
    # Same (symbol, event_time) delivered twice with different ingest_seq.
    consumer.land(con, [
        Tick("AAPL", "2026-05-01T13:00:00+00:00", 100.0, 1),
        Tick("AAPL", "2026-05-01T13:00:00+00:00", 100.0, 2),
    ])
    transform.build_curated(con)
    n = con.execute("SELECT COUNT(*) FROM fct_latest_price").fetchone()[0]
    assert n == 1
    assert transform.duplicate_count(con) == 0


def test_latest_arrival_wins_not_latest_event_time(con):
    # A correction for the SAME event_time arrives later (higher seq) with a new price.
    consumer.land(con, [
        Tick("MSFT", "2026-05-01T13:00:00+00:00", 300.0, 1),
        Tick("MSFT", "2026-05-01T13:00:00+00:00", 305.0, 5),  # correction, arrived later
    ])
    transform.build_curated(con)
    price = con.execute("SELECT price FROM fct_latest_price WHERE symbol='MSFT'").fetchone()[0]
    assert price == 305.0  # the later-arrived version wins


def test_late_out_of_order_event_does_not_regress(con):
    # An older event_time arrives late; it must not overwrite the newer event_time's row.
    consumer.land(con, [
        Tick("GOOG", "2026-05-01T13:00:10+00:00", 150.0, 1),  # newer event
        Tick("GOOG", "2026-05-01T13:00:05+00:00", 149.0, 2),  # older event, arrived late
    ])
    transform.build_curated(con)
    rows = con.execute(
        "SELECT event_time, price FROM fct_latest_price WHERE symbol='GOOG' ORDER BY event_time"
    ).fetchall()
    # Both event_times are distinct keys, both present, neither corrupted.
    assert len(rows) == 2
    assert transform.stale_rows(con) == 0


def test_no_duplicates_or_stale_on_messy_stream(con):
    ticks = producer.generate(20_000, dup_rate=0.08, late_rate=0.05)
    consumer.land(con, ticks)
    transform.build_curated(con)
    assert transform.duplicate_count(con) == 0
    assert transform.stale_rows(con) == 0
