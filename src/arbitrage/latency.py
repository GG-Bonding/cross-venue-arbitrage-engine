"""Read-only, offline nearest-rank execution latency statistics."""

import argparse
import json
import math
import sqlite3
from pathlib import Path

STAGES = ("entry_dispatch", "cancel_dispatch", "hedge_dispatch")


def summarize(path: Path) -> dict:
    samples = {stage: [] for stage in STAGES}
    with sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True) as db:
        for (payload,) in db.execute("SELECT payload FROM live_trades"):
            timings = json.loads(payload).get("timings_ns", {})
            for stage in STAGES:
                value = timings.get(stage)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    samples[stage].append(value / 1_000_000)
    report = {}
    for stage, values in samples.items():
        values.sort()
        report[stage] = {"count": len(values)}
        for percentile in (50, 95, 99):
            report[stage][f"p{percentile}_ms"] = (
                values[math.ceil(len(values) * percentile / 100) - 1] if values else None
            )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("database", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.database), indent=2))


if __name__ == "__main__":
    main()
