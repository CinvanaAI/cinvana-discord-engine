from __future__ import annotations

import argparse
import asyncio
import json
import time
from datetime import date

from .config import EngineConfig
from .db import EngineDatabase
from .planner import parse_date, plan_calendar
from .projection import project_ce_private_calendar, project_ce_public_mapping
from .reconciler import plan_publication_queue
from .repository import Repository
from .reports import write_dry_run_report


DEFAULT_START = "2025-05-01"
DEFAULT_END = date.today().isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="cinvana-discord-engine")
    parser.add_argument("--root", help="Discord Engine root folder")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Create Runtime, env template, and database")

    configure = subparsers.add_parser("configure-guild", help="Configure one Discord guild")
    configure.add_argument("--guild-key", default="private_chronological")
    configure.add_argument("--name", required=True)
    configure.add_argument("--purpose", default="Private chronological archive")
    configure.add_argument("--server-model", choices=("chronological_private", "mapping"), default="chronological_private")
    configure.add_argument("--discord-guild-id")
    configure.add_argument("--bot-head", default="private_chronological_manager")
    configure.add_argument("--bot-name", default="Private Chronological Manager")
    configure.add_argument("--token-env", default="DISCORD_PRIVATE_CHRONO_BOT_TOKEN")

    subparsers.add_parser("configure-from-env", help="Configure Discord guilds from env file")

    plan = subparsers.add_parser("plan-calendar", help="Plan year/month/day topology")
    plan.add_argument("--guild-key", default="private_chronological")
    plan.add_argument("--start", default=DEFAULT_START)
    plan.add_argument("--end", default=DEFAULT_END)
    plan.add_argument("--examples", type=int, default=3)

    project = subparsers.add_parser(
        "project-ce-calendar",
        help="Project CE private Discord chunks into the chronological server DB",
    )
    project.add_argument("--guild-key", default="private_chronological")
    project.add_argument("--start", default=DEFAULT_START)
    project.add_argument("--end", default=DEFAULT_END)

    mapping = subparsers.add_parser(
        "project-ce-mapping",
        help="Project CE public Discord chunks through the Mapping Index",
    )
    mapping.add_argument("--guild-key", default="public_mapping")
    mapping.add_argument("--start")
    mapping.add_argument("--end")

    pause = subparsers.add_parser("pause-publication", help="Pause live Discord publication")
    pause.add_argument("--reason", default="Publication paused by operator.")

    unpause = subparsers.add_parser("unpause-publication", help="Unpause live Discord publication")
    unpause.add_argument("--reason", default="Publication unpaused by operator.")

    status = subparsers.add_parser("status", help="Show database and plan status")
    status.add_argument("--guild-key", default="private_chronological")

    dry = subparsers.add_parser("dry-run", help="Write and print a dry-run report")
    dry.add_argument("--guild-key", default="private_chronological")
    dry.add_argument("--limit", type=int, default=20)

    sync = subparsers.add_parser("sync-calendar", help="Create missing Discord calendar objects")
    sync.add_argument("--guild-key", default="private_chronological")
    sync.add_argument("--max-actions", type=int)
    sync.add_argument("--min-seconds-between-writes", type=float)

    plan_pub = subparsers.add_parser(
        "plan-publication",
        help="Compare desired Discord DB state to published slots and queue writes",
    )
    plan_pub.add_argument("--guild-key", default="private_chronological")

    reconcile = subparsers.add_parser(
        "reconcile-now",
        help="Project CE into Discord DB, plan queues, and leave publication paused if paused",
    )
    reconcile.add_argument("--start", default=DEFAULT_START)
    reconcile.add_argument("--end")

    daemon = subparsers.add_parser(
        "run-daemon",
        help="Run CE projection, publication planning, and queued publication forever",
    )
    daemon.add_argument("--start", default=DEFAULT_START)
    daemon.add_argument("--end")
    daemon.add_argument("--interval-seconds", type=int)
    daemon.add_argument("--max-actions-per-guild", type=int)
    daemon.add_argument("--min-seconds-between-writes", type=float)
    return parser


def print_json(value: object) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False))


def open_repo(config: EngineConfig) -> tuple[EngineDatabase, object, Repository]:
    database = EngineDatabase(config.state_db)
    database.initialize()
    connection = database.connect()
    return database, connection, Repository(connection)


def configure_from_env(repository: Repository, config: EngineConfig) -> dict[str, object]:
    bot_head = config.default_bot_head
    bot_name = config.value(
        "DISCORD_PRIVATE_CHRONO_BOT_NAME",
        "Private Chronological Manager",
    ) or "Private Chronological Manager"
    guild_key = config.default_guild_key
    guild_name = config.value(
        "DISCORD_PRIVATE_CHRONO_GUILD_NAME",
        "CinvanaAI Private Chronological Archive",
    ) or "CinvanaAI Private Chronological Archive"
    guild_id = config.value("DISCORD_PRIVATE_CHRONO_GUILD_ID")
    repository.configure_bot_head(
        bot_head_key=bot_head,
        display_name=bot_name,
        token_env="DISCORD_PRIVATE_CHRONO_BOT_TOKEN",
    )
    repository.configure_guild(
        guild_key=guild_key,
        name=guild_name,
        purpose="Private chronological archive",
        server_model="chronological_private",
        bot_head_key=bot_head,
        discord_guild_id=guild_id,
    )
    public_bot_head = config.public_mapping_bot_head
    public_bot_name = config.value(
        "DISCORD_PUBLIC_MAPPING_BOT_NAME",
        "Public Mapping Manager",
    ) or "Public Mapping Manager"
    public_guild_key = config.public_mapping_guild_key
    public_guild_name = config.value(
        "DISCORD_PUBLIC_MAPPING_GUILD_NAME",
        "CinvanaAI Public Mapping Archive",
    ) or "CinvanaAI Public Mapping Archive"
    public_guild_id = config.value("DISCORD_PUBLIC_MAPPING_GUILD_ID")
    repository.configure_bot_head(
        bot_head_key=public_bot_head,
        display_name=public_bot_name,
        token_env="DISCORD_PUBLIC_MAPPING_BOT_TOKEN",
    )
    repository.configure_guild(
        guild_key=public_guild_key,
        name=public_guild_name,
        purpose="Public Mapping Index archive",
        server_model="mapping",
        bot_head_key=public_bot_head,
        discord_guild_id=public_guild_id,
    )
    return {
        "guild_key": guild_key,
        "guild_name": guild_name,
        "discord_guild_id_set": bool(guild_id),
        "bot_head": bot_head,
        "public_mapping": {
            "guild_key": public_guild_key,
            "guild_name": public_guild_name,
            "discord_guild_id_set": bool(public_guild_id),
            "bot_head": public_bot_head,
        },
    }


def reconcile_now(
    *,
    config: EngineConfig,
    start_text: str,
    end_text: str | None,
) -> dict[str, object]:
    database, connection, repository = open_repo(config)
    try:
        configured = configure_from_env(repository, config)
        start = parse_date(start_text)
        end = parse_date(end_text) if end_text else date.today()
        private_guild_key = config.default_guild_key
        public_guild_key = config.public_mapping_guild_key
        private_projection = project_ce_private_calendar(
            repository,
            ce_root=config.ce_root,
            guild_key=private_guild_key,
            start=start,
            end=end,
        )
        public_projection = project_ce_public_mapping(
            repository,
            ce_root=config.ce_root,
            guild_key=public_guild_key,
            start=start,
            end=end,
        )
        private_plan = plan_publication_queue(
            repository,
            guild_key=private_guild_key,
        )
        public_plan = plan_publication_queue(
            repository,
            guild_key=public_guild_key,
        )
        connection.commit()
        return {
            "configured": configured,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "private": {
                "projection": private_projection.to_dict(),
                "plan": private_plan.to_dict(),
                "queue": repository.queue_summary(private_guild_key),
            },
            "public": {
                "projection": public_projection.to_dict(),
                "plan": public_plan.to_dict(),
                "queue": repository.queue_summary(public_guild_key),
            },
            "publication_paused": repository.is_paused("publication"),
            "database": str(config.state_db),
            "verify": database.verify(),
        }
    finally:
        connection.close()


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = EngineConfig.load(args.root)
    config.create_runtime()

    if args.command == "init":
        database = EngineDatabase(config.state_db)
        database.initialize()
        print_json(
            {
                "status": "initialized",
                "root": str(config.root),
                "database": str(config.state_db),
                "env_file": str(config.env_file),
                "verify": database.verify(),
            }
        )
        return 0

    database, connection, repository = open_repo(config)
    try:
        if args.command == "configure-guild":
            repository.configure_bot_head(
                bot_head_key=args.bot_head,
                display_name=args.bot_name,
                token_env=args.token_env,
            )
            repository.configure_guild(
                guild_key=args.guild_key,
                name=args.name,
                purpose=args.purpose,
                server_model=args.server_model,
                bot_head_key=args.bot_head,
                discord_guild_id=args.discord_guild_id,
            )
            connection.commit()
            print_json(
                {
                    "status": "configured",
                    "guild_key": args.guild_key,
                    "discord_guild_id_set": bool(args.discord_guild_id),
                }
            )
            return 0

        if args.command == "configure-from-env":
            configured = configure_from_env(repository, config)
            connection.commit()
            print_json({"status": "configured", **configured})
            return 0

        if args.command == "plan-calendar":
            repository.guild(args.guild_key)
            plan = plan_calendar(
                repository,
                guild_key=args.guild_key,
                start=parse_date(args.start),
                end=parse_date(args.end),
                example_count=args.examples,
            )
            connection.commit()
            print_json(
                {
                    "status": "planned",
                    "guild_key": plan.guild_key,
                    "start": plan.start.isoformat(),
                    "end": plan.end.isoformat(),
                    "categories": plan.categories,
                    "channels": plan.channels,
                    "threads": plan.threads,
                    "messages": plan.messages,
                    "stats": repository.stats(args.guild_key),
                }
            )
            return 0

        if args.command == "project-ce-calendar":
            repository.guild(args.guild_key)
            report = project_ce_private_calendar(
                repository,
                ce_root=config.ce_root,
                guild_key=args.guild_key,
                start=parse_date(args.start),
                end=parse_date(args.end),
            )
            connection.commit()
            print_json({"status": "projected", **report.to_dict()})
            return 0

        if args.command == "project-ce-mapping":
            repository.guild(args.guild_key)
            report = project_ce_public_mapping(
                repository,
                ce_root=config.ce_root,
                guild_key=args.guild_key,
                start=parse_date(args.start) if args.start else None,
                end=parse_date(args.end) if args.end else None,
            )
            connection.commit()
            print_json({"status": "projected", **report.to_dict()})
            return 0

        if args.command == "pause-publication":
            repository.set_control("publication", True, args.reason)
            connection.commit()
            print_json({"status": "paused", "control": repository.control("publication")})
            return 0

        if args.command == "unpause-publication":
            repository.set_control("publication", False, args.reason)
            connection.commit()
            print_json({"status": "unpaused", "control": repository.control("publication")})
            return 0

        if args.command == "status":
            guild_exists = connection.execute(
                "SELECT 1 FROM guilds WHERE guild_key = ?",
                (args.guild_key,),
            ).fetchone()
            print_json(
                {
                    "root": str(config.root),
                    "ce_root": str(config.ce_root),
                    "database": str(config.state_db),
                    "env_file": str(config.env_file),
                    "verify": database.verify(),
                    "controls": repository.controls(),
                    "stats": repository.stats(args.guild_key),
                    "remaining_actions": repository.remaining_actions(args.guild_key)
                    if guild_exists else None,
                    "ready_actions": repository.ready_actions(args.guild_key)
                    if guild_exists else None,
                    "queue": repository.queue_summary(args.guild_key)
                    if guild_exists else None,
                    "published_slots": repository.published_slot_count(args.guild_key)
                    if guild_exists else None,
                    "projection": repository.projection_summary(args.guild_key)
                    if guild_exists else None,
                }
            )
            return 0

        if args.command == "dry-run":
            repository.guild(args.guild_key)
            target = write_dry_run_report(
                repository=repository,
                reports_dir=config.reports_dir,
                guild_key=args.guild_key,
                limit=args.limit,
            )
            print_json(
                {
                    "status": "written",
                    "path": str(target),
                    "stats": repository.stats(args.guild_key),
                    "remaining_actions": repository.remaining_actions(args.guild_key),
                    "ready_actions": repository.ready_actions(args.guild_key),
                    "sample": repository.calendar_sample(args.guild_key, args.limit),
                }
            )
            return 0

        if args.command == "plan-publication":
            repository.guild(args.guild_key)
            report = plan_publication_queue(repository, guild_key=args.guild_key)
            connection.commit()
            print_json(
                {
                    "status": "planned",
                    "plan": report.to_dict(),
                    "queue": repository.queue_summary(args.guild_key),
                    "projection": repository.projection_summary(args.guild_key),
                }
            )
            return 0

        if args.command == "reconcile-now":
            connection.close()
            report = reconcile_now(
                config=config,
                start_text=args.start,
                end_text=args.end,
            )
            print_json({"status": "reconciled", **report})
            return 0

        if args.command == "run-daemon":
            connection.close()
            interval = args.interval_seconds or config.reconcile_interval_seconds
            min_seconds = (
                args.min_seconds_between_writes
                if args.min_seconds_between_writes is not None
                else config.min_seconds_between_writes
            )
            max_actions = args.max_actions_per_guild
            if max_actions is None:
                max_actions = max(1, int(interval / max(1.0, min_seconds)))
            while True:
                started = time.monotonic()
                cycle = reconcile_now(
                    config=config,
                    start_text=args.start,
                    end_text=args.end,
                )
                sync_reports = []
                if not cycle["publication_paused"]:
                    from .discord_runtime import sync_calendar

                    for guild_key in (
                        config.default_guild_key,
                        config.public_mapping_guild_key,
                    ):
                        sync_reports.append(
                            asyncio.run(
                                sync_calendar(
                                    config=config,
                                    guild_key=guild_key,
                                    max_actions=max_actions,
                                    min_seconds_between_writes=min_seconds,
                                )
                            )
                        )
                print_json(
                    {
                        "status": "daemon_cycle_complete",
                        "interval_seconds": interval,
                        "max_actions_per_guild": max_actions,
                        "min_seconds_between_writes": min_seconds,
                        "cycle": cycle,
                        "sync": sync_reports,
                    }
                )
                elapsed = time.monotonic() - started
                time.sleep(max(0.0, interval - elapsed))

        if args.command == "sync-calendar":
            connection.close()
            max_actions = args.max_actions or config.default_max_actions
            from .discord_runtime import sync_calendar

            report = asyncio.run(
                sync_calendar(
                    config=config,
                    guild_key=args.guild_key,
                    max_actions=max_actions,
                    min_seconds_between_writes=args.min_seconds_between_writes,
                )
            )
            print_json({"status": "sync_complete", **report})
            return 0
    finally:
        if args.command != "sync-calendar":
            connection.close()

    parser.error(f"unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
