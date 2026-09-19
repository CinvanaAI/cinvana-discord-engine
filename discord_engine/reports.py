from __future__ import annotations

import json
from pathlib import Path

from .repository import Repository
from .timeutil import utc_now


def write_dry_run_report(
    *,
    repository: Repository,
    reports_dir: Path,
    guild_key: str,
    limit: int,
) -> Path:
    reports_dir.mkdir(parents=True, exist_ok=True)
    report = {
        "generated_at": utc_now(),
        "guild_key": guild_key,
        "stats": repository.stats(guild_key),
        "controls": repository.controls(),
        "projection": repository.projection_summary(guild_key),
        "remaining_actions": repository.remaining_actions(guild_key),
        "ready_actions": repository.ready_actions(guild_key),
        "sample": repository.calendar_sample(guild_key, limit),
    }
    target = reports_dir / f"calendar_dry_run_{guild_key}.json"
    target.write_text(
        json.dumps(report, indent=2, ensure_ascii=False),
        encoding="utf-8",
        newline="\n",
    )
    return target
