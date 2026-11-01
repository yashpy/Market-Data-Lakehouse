"""Generate a realistic market-tick stream.

To mirror production reality, the stream deliberately contains:
  * duplicates   (at-least-once delivery re-sends the same event), and
  * late events  (out-of-order arrival: an older event_time shows up after a newer one).

Each emitted record carries a monotonically increasing ``ingest_seq`` reflecting *arrival*
order, which is what lets the transform pick the correct latest version.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterator, List

SYMBOLS = ["AAPL", "MSFT", "GOOG", "AMZN", "NVDA", "TSLA", "META"]


@dataclass
class Tick:
    symbol: str
    event_time: str   # ISO8601 — when the trade happened
    price: float
    ingest_seq: int   # arrival order (assigned by the producer/broker)


def generate(n: int, dup_rate: float = 0.05, late_rate: float = 0.03, seed: int = 7) -> List[Tick]:
    rng = random.Random(seed)
    base = datetime.now(timezone.utc) - timedelta(minutes=30)
    prices = {s: rng.uniform(50, 500) for s in SYMBOLS}

    emitted: List[Tick] = []
    seq = 0
    for i in range(n):
        symbol = rng.choice(SYMBOLS)
        prices[symbol] *= 1 + rng.uniform(-0.002, 0.002)
        et = base + timedelta(milliseconds=i * 10)

        # Late event: emit with an older event_time than already seen.
        if emitted and rng.random() < late_rate:
            et = et - timedelta(seconds=rng.randint(5, 60))

        tick = Tick(symbol, et.isoformat(), round(prices[symbol], 4), seq)
        emitted.append(tick)
        seq += 1

        # Duplicate: re-deliver the same event with a NEW (higher) ingest_seq.
        if rng.random() < dup_rate:
            emitted.append(Tick(tick.symbol, tick.event_time, tick.price, seq))
            seq += 1

    return emitted


def stream(ticks: List[Tick]) -> Iterator[Tick]:
    yield from ticks
