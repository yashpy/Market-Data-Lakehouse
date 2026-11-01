# Market Data Lakehouse

**A streaming ETL that turns a firehose of market events into clean, incrementally-modeled
tables — with idempotent upserts and late-event handling so the warehouse is never stale
and never corrupted by duplicates.**

Batch pipelines leave analysts staring at data that's minutes-to-hours old, and naive
streaming pipelines corrupt their tables the moment events arrive late or twice (which they
always do). This lakehouse fixes both:

```
 market feed ─▶ Kafka topic ─▶ stream consumers ─▶ raw landing (Snowflake)
                                                          │
                                              dbt incremental models
                                      (idempotent MERGE + watermark dedup)
                                                          ▼
                                            curated, query-ready tables
```

- **Fresh:** micro-batched streaming ingestion instead of hourly batch.
- **Correct under reality:** every record carries an `event_time` and a monotonic
  `ingest_seq`; the incremental model **MERGE**s on the business key and keeps only the
  latest version, so **duplicates and out-of-order/late events can't produce wrong rows.**
- **Runnable without infra:** the repo ships a local engine that uses an in-process event
  stream and **DuckDB** in place of Kafka + Snowflake, so you can run the whole pipeline —
  including the dedup/late-event logic — on a laptop.

## What's in here

| Path | What it is |
| --- | --- |
| `lakehouse/producer.py` | Generates a realistic market-tick stream, deliberately injecting duplicate and late (out-of-order) events. |
| `lakehouse/consumer.py` | Stream consumer: validates ticks and lands them in the raw table with an ingest sequence + arrival time. |
| `lakehouse/transform.py` | The incremental upsert — MERGE on `(symbol, event_time)` keeping the max `ingest_seq`; the heart of the correctness guarantee. |
| `lakehouse/engine.py` | Wires producer → consumer → transform over DuckDB (local stand-in for Kafka + Snowflake). |
| `dbt/models/` | The production dbt version: `stg_ticks` (raw), `fct_latest_price` (incremental MERGE), plus schema tests. |
| `tests/` | Unit tests proving dedup + late-event correctness and freshness. |

## Run it locally (no Kafka, no Snowflake)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

python -m lakehouse.engine --ticks 100000 --dup-rate 0.05 --late-rate 0.03
```

Example output:

```
produced ticks        : 100000   (incl. 5000 duplicates, 3000 late/out-of-order)
landed in raw          : 100000
curated rows           : 42310    (one row per symbol/event_time, latest wins)
duplicate rows in curated: 0      ✓ idempotent
stale rows (older seq kept): 0     ✓ late events handled correctly
end-to-end latency p50 : <Xms
```

## My contribution

I designed and built the ingestion + modeling layer:

- **The correctness model** — carrying `event_time` + monotonic `ingest_seq` on every
  record and enforcing an **idempotent MERGE** keyed on the business key, so re-delivered
  and late events converge to the correct latest row instead of duplicating or regressing.
- **The dbt incremental models** (`stg_ticks`, `fct_latest_price`) with the MERGE strategy
  and **freshness / uniqueness / not-null schema tests** that run in CI.
- **A local engine** that runs the identical dedup/late-event logic over DuckDB, making the
  guarantees reproducible without standing up Kafka or Snowflake.

## Results / evaluation

Run the engine and the test suite to reproduce the zero-duplicate / zero-stale guarantees
and the freshness numbers on your own machine. The repo ships the harness, not hard-coded
figures.

## License

MIT
