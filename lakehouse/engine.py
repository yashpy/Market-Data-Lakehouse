"""Wire producer -> consumer -> incremental transform over DuckDB.

Local stand-in for Kafka + Snowflake so the entire pipeline — including the dedup and
late-event correctness logic — runs on a laptop.
"""

from __future__ import annotations

import argparse
import time

import duckdb

from . import consumer, producer, transform


def run(ticks: int, dup_rate: float, late_rate: float) -> int:
    con = duckdb.connect(":memory:")
    consumer.ensure_raw_table(con)

    produced = producer.generate(ticks, dup_rate=dup_rate, late_rate=late_rate)

    t0 = time.time()
    landed = consumer.land(con, producer.stream(produced))
    transform.build_curated(con)
    elapsed_ms = (time.time() - t0) * 1000

    curated = con.execute("SELECT COUNT(*) FROM fct_latest_price").fetchone()[0]
    dups = transform.duplicate_count(con)
    stale = transform.stale_rows(con)

    print(f"produced ticks           : {len(produced)}")
    print(f"landed in raw            : {landed}")
    print(f"curated rows             : {curated}    (one row per symbol/event_time, latest wins)")
    print(f"duplicate rows in curated: {dups}      {'OK idempotent' if dups == 0 else 'FAIL'}")
    print(f"stale rows (older kept)  : {stale}      {'OK late-safe' if stale == 0 else 'FAIL'}")
    print(f"end-to-end build time    : {elapsed_ms:.1f} ms for {landed} records")

    # Non-zero exit if the correctness guarantees are violated.
    return 0 if (dups == 0 and stale == 0) else 1


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description="Local market-data lakehouse engine (DuckDB).")
    p.add_argument("--ticks", type=int, default=100_000)
    p.add_argument("--dup-rate", type=float, default=0.05)
    p.add_argument("--late-rate", type=float, default=0.03)
    args = p.parse_args(argv)
    return run(args.ticks, args.dup_rate, args.late_rate)


if __name__ == "__main__":
    raise SystemExit(main())
