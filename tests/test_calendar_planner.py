from __future__ import annotations

import json

from discord_engine.config import EngineConfig
from discord_engine.db import EngineDatabase
from discord_engine.planner import parse_date, plan_calendar
from discord_engine.repository import Repository
from discord_engine.reports import write_dry_run_report


def make_repo(tmp_path):
    config = EngineConfig.load(tmp_path / "Discord Bot")
    config.create_runtime()
    database = EngineDatabase(config.state_db)
    database.initialize()
    connection = database.connect()
    repo = Repository(connection)
    repo.configure_bot_head(
        bot_head_key="private_chronological_manager",
        display_name="Private Chronological Manager",
        token_env="DISCORD_PRIVATE_CHRONO_BOT_TOKEN",
    )
    repo.configure_guild(
        guild_key="private_chronological",
        name="CinvanaAI Private Chronological Archive",
        purpose="Private chronological archive",
        server_model="chronological_private",
        bot_head_key="private_chronological_manager",
        discord_guild_id=None,
    )
    connection.commit()
    return config, database, connection, repo


def test_calendar_plan_counts_current_range(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        plan = plan_calendar(
            repo,
            guild_key="private_chronological",
            start=parse_date("2025-05-01"),
            end=parse_date("2026-07-20"),
            example_count=3,
        )
        connection.commit()

        assert plan.categories == 2
        assert plan.channels == 15
        assert plan.threads == 446
        assert plan.messages == 1784
        assert repo.stats("private_chronological")["messages"]["planned"] == 1784
        assert repo.remaining_actions("private_chronological") == {
            "categories": 2,
            "channels": 15,
            "threads": 446,
            "messages": 1784,
        }
        assert repo.ready_actions("private_chronological") == {
            "categories": 2,
            "channels": 0,
            "threads": 0,
            "replies": 0,
        }
    finally:
        connection.close()


def test_calendar_plan_is_idempotent(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        for _ in range(2):
            plan_calendar(
                repo,
                guild_key="private_chronological",
                start=parse_date("2025-05-01"),
                end=parse_date("2025-05-02"),
                example_count=3,
            )
            connection.commit()

        stats = repo.stats("private_chronological")
        assert stats["categories"]["total"] == 1
        assert stats["channels"]["total"] == 1
        assert stats["threads"]["total"] == 2
        assert stats["messages"]["total"] == 8
    finally:
        connection.close()


def test_day_thread_has_starter_and_three_examples(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        plan_calendar(
            repo,
            guild_key="private_chronological",
            start=parse_date("2025-05-01"),
            end=parse_date("2025-05-01"),
            example_count=3,
        )
        connection.commit()

        rows = connection.execute(
            """
            SELECT ordinal, role, literal_text
            FROM messages
            ORDER BY ordinal
            """
        ).fetchall()
        assert [(row["ordinal"], row["role"], row["literal_text"]) for row in rows] == [
            (0, "thread_starter", "2025-05-01"),
            (1, "example", "example 1"),
            (2, "example", "example 2"),
            (3, "example", "example 3"),
        ]
    finally:
        connection.close()


def test_dry_run_report_uses_discord_topology(tmp_path):
    config, _, connection, repo = make_repo(tmp_path)
    try:
        plan_calendar(
            repo,
            guild_key="private_chronological",
            start=parse_date("2025-05-01"),
            end=parse_date("2025-05-03"),
            example_count=3,
        )
        connection.commit()

        target = write_dry_run_report(
            repository=repo,
            reports_dir=config.reports_dir,
            guild_key="private_chronological",
            limit=2,
        )

        value = json.loads(target.read_text(encoding="utf-8"))
        assert value["stats"]["threads"]["total"] == 3
        assert value["stats"]["messages"]["total"] == 12
        assert value["sample"][0]["category_name"] == "2025"
        assert value["sample"][0]["channel_name"] == "2025-05"
        assert value["sample"][0]["thread_name"] == "2025-05-01"
    finally:
        connection.close()
