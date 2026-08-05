#!/usr/bin/env python3
from pathlib import Path

source = (Path(__file__).parents[1] / "src/pages/KeyPortal.tsx").read_text()
section = source[source.index("function IPActivitySection"):source.index("function ActivityStat")]

required = (
    "const PAGE_SIZE = 5;",
    "activity.ips.slice(start, start + PAGE_SIZE)",
    "Showing {start + 1}–{Math.min(start + PAGE_SIZE, activity.ips.length)} of {activity.ips.length}",
    "{safePage + 1} / {totalPages}",
    "aria-label=\"Previous IP page\"",
    "aria-label=\"Next IP page\"",
    "{start + index + 1}",
)
missing = [text for text in required if text not in section]
assert not missing, f"IP Activity pagination incomplete: {missing}"
print("IP Activity pagination check passed")
