from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any

from .ce_client import ConversationEngineClient
from .config import EngineConfig
from .db import EngineDatabase
from .reconciler import plan_publication_queue
from .repository import Repository


@dataclass
class SyncReport:
    guild_key: str
    max_actions: int
    min_seconds_between_writes: float
    actions: int = 0
    categories_created: int = 0
    channels_created: int = 0
    starter_messages_created: int = 0
    threads_created: int = 0
    replies_created: int = 0
    messages_edited: int = 0
    messages_deleted: int = 0
    names_updated: int = 0
    categories_deleted: int = 0
    channels_deleted: int = 0
    threads_deleted: int = 0
    plan: dict[str, Any] | None = None
    failed: bool = False
    error: str | None = None

    def to_dict(self, remaining_actions: dict[str, int]) -> dict[str, Any]:
        return {
            "guild_key": self.guild_key,
            "max_actions": self.max_actions,
            "min_seconds_between_writes": self.min_seconds_between_writes,
            "actions": self.actions,
            "categories_created": self.categories_created,
            "channels_created": self.channels_created,
            "starter_messages_created": self.starter_messages_created,
            "threads_created": self.threads_created,
            "replies_created": self.replies_created,
            "messages_edited": self.messages_edited,
            "messages_deleted": self.messages_deleted,
            "names_updated": self.names_updated,
            "categories_deleted": self.categories_deleted,
            "channels_deleted": self.channels_deleted,
            "threads_deleted": self.threads_deleted,
            "plan": self.plan,
            "failed": self.failed,
            "error": self.error,
            "remaining_actions": remaining_actions,
        }


def _ensure_discord():
    try:
        import discord
    except ImportError as exc:
        raise RuntimeError(
            "discord.py is not installed. Install with: python -m pip install -e .[discord]"
        ) from exc
    return discord


class WriteGovernor:
    def __init__(self, min_seconds_between_writes: float):
        self.min_seconds_between_writes = max(0.0, min_seconds_between_writes)
        self.last_write_at: float | None = None

    async def wait(self) -> None:
        if self.last_write_at is not None and self.min_seconds_between_writes > 0:
            elapsed = time.monotonic() - self.last_write_at
            remaining = self.min_seconds_between_writes - elapsed
            if remaining > 0:
                await asyncio.sleep(remaining)
        self.last_write_at = time.monotonic()


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _message_content(row: dict[str, Any], config: EngineConfig) -> str:
    if row["content_kind"] == "literal":
        value = row.get("literal_text")
        if value is None:
            raise RuntimeError(f"literal message has no text: {row['message_key']}")
        return value
    if row["content_kind"] == "ce_pointer":
        payload_json = row.get("source_payload_json")
        if not payload_json:
            raise RuntimeError(f"CE pointer has no binding: {row['message_key']}")
        payload = json.loads(payload_json)
        content = ConversationEngineClient(config.ce_root).resolve_payload(payload)
        expected_hash = row.get("desired_content_hash")
        actual_hash = _sha256_text(content)
        if expected_hash and expected_hash != actual_hash:
            raise RuntimeError(
                f"CE pointer hash mismatch for {row['message_key']}: {actual_hash}"
            )
        return content
    raise RuntimeError(f"unsupported content_kind: {row['content_kind']}")


async def _fetch_channel(client, guild, discord_id: str):
    channel = client.get_channel(int(discord_id))
    if channel is not None:
        return channel
    return await guild.fetch_channel(int(discord_id))


async def _create_thread(channel, starter_message, *, name: str, reason: str):
    created = await channel.create_thread(
        name=name,
        message=starter_message,
        auto_archive_duration=1440,
        reason=reason,
    )
    if isinstance(created, tuple):
        return created[0]
    return created


def _single_row(repository: Repository, query: str, args: tuple[Any, ...], label: str):
    row = repository.connection.execute(query, args).fetchone()
    if row is None:
        raise RuntimeError(f"missing {label}")
    return row


def _message_row(repository: Repository, message_key: str) -> dict[str, Any]:
    row = _single_row(
        repository,
        """
        SELECT messages.*, bindings.source_payload_json
        FROM messages
        LEFT JOIN content_bindings bindings
          ON bindings.message_key = messages.message_key
         AND bindings.source_system = 'conversation_engine'
        WHERE messages.message_key = ?
        """,
        (message_key,),
        f"message {message_key}",
    )
    return dict(row)


def _container_row(repository: Repository, container_key: str) -> dict[str, Any]:
    row = _single_row(
        repository,
        """
        SELECT threads.*, channels.discord_channel_id
        FROM threads
        JOIN channels ON channels.channel_key = threads.channel_key
        WHERE threads.thread_key = ?
        """,
        (container_key,),
        f"container {container_key}",
    )
    return dict(row)


async def _fetch_message_container(client, guild, repository: Repository, container_key: str):
    row = _container_row(repository, container_key)
    discord_id = row["discord_thread_id"]
    if not discord_id:
        raise RuntimeError(f"container has no Discord id: {container_key}")
    return await _fetch_channel(client, guild, discord_id)


async def _execute_operation(
    *,
    operation: dict[str, Any],
    client,
    guild,
    repository: Repository,
    config: EngineConfig,
    governor: WriteGovernor,
    report: SyncReport,
) -> None:
    op_type = operation["operation_type"]
    payload = operation["payload"]
    reason = "CinvanaAI Discord Engine queued publication"

    if op_type == "CREATE_CATEGORY":
        row = _single_row(
            repository,
            "SELECT * FROM categories WHERE category_key = ?",
            (payload["category_key"],),
            f"category {payload['category_key']}",
        )
        await governor.wait()
        category = await guild.create_category(row["name"], reason=reason)
        repository.mark_created(
            table="categories",
            key_column="category_key",
            key=row["category_key"],
            discord_column="discord_category_id",
            discord_id=str(category.id),
        )
        report.categories_created += 1
        return

    if op_type == "EDIT_CATEGORY_NAME":
        row = _single_row(
            repository,
            "SELECT * FROM categories WHERE category_key = ?",
            (payload["category_key"],),
            f"category {payload['category_key']}",
        )
        category = await _fetch_channel(client, guild, row["discord_category_id"])
        await governor.wait()
        await category.edit(name=row["name"], reason=reason)
        repository.mark_published_name_current(
            table="categories",
            key_column="category_key",
            key=row["category_key"],
        )
        report.names_updated += 1
        return

    if op_type == "CREATE_CHANNEL":
        row = _single_row(
            repository,
            """
            SELECT channels.*, categories.discord_category_id
            FROM channels
            JOIN categories ON categories.category_key = channels.category_key
            WHERE channels.channel_key = ?
            """,
            (payload["channel_key"],),
            f"channel {payload['channel_key']}",
        )
        if not row["discord_category_id"]:
            raise RuntimeError(f"channel category is not published: {row['category_key']}")
        category = await _fetch_channel(client, guild, row["discord_category_id"])
        await governor.wait()
        channel = await guild.create_text_channel(
            row["name"],
            category=category,
            reason=reason,
        )
        repository.mark_created(
            table="channels",
            key_column="channel_key",
            key=row["channel_key"],
            discord_column="discord_channel_id",
            discord_id=str(channel.id),
        )
        repository.mark_channel_streams_created(
            channel_key=row["channel_key"],
            discord_channel_id=str(channel.id),
        )
        report.channels_created += 1
        return

    if op_type == "EDIT_CHANNEL_NAME":
        row = _single_row(
            repository,
            "SELECT * FROM channels WHERE channel_key = ?",
            (payload["channel_key"],),
            f"channel {payload['channel_key']}",
        )
        channel = await _fetch_channel(client, guild, row["discord_channel_id"])
        await governor.wait()
        await channel.edit(name=row["name"], reason=reason)
        repository.mark_published_name_current(
            table="channels",
            key_column="channel_key",
            key=row["channel_key"],
        )
        report.names_updated += 1
        return

    if op_type == "CREATE_THREAD_STARTER":
        row = _single_row(
            repository,
            """
            SELECT messages.*, channels.discord_channel_id,
                   bindings.source_payload_json
            FROM messages
            JOIN threads ON threads.thread_key = messages.thread_key
            JOIN channels ON channels.channel_key = threads.channel_key
            LEFT JOIN content_bindings bindings
              ON bindings.message_key = messages.message_key
             AND bindings.source_system = 'conversation_engine'
            WHERE messages.message_key = ?
            """,
            (payload["message_key"],),
            f"thread starter {payload['message_key']}",
        )
        if not row["discord_channel_id"]:
            raise RuntimeError(f"starter channel is not published: {payload['channel_key']}")
        channel = await _fetch_channel(client, guild, row["discord_channel_id"])
        content = _message_content(dict(row), config)
        await governor.wait()
        message = await channel.send(content)
        repository.mark_message_created(
            message_key=row["message_key"],
            discord_message_id=str(message.id),
            desired_content_hash=row["desired_content_hash"],
            desired_ordinal=int(row["ordinal"]),
        )
        report.starter_messages_created += 1
        return

    if op_type == "CREATE_THREAD":
        row = _single_row(
            repository,
            """
            SELECT threads.*, channels.discord_channel_id,
                   starter.discord_message_id AS starter_discord_message_id
            FROM threads
            JOIN channels ON channels.channel_key = threads.channel_key
            LEFT JOIN messages starter ON starter.message_key = ?
            WHERE threads.thread_key = ?
            """,
            (payload["starter_message_key"], payload["thread_key"]),
            f"thread {payload['thread_key']}",
        )
        if not row["discord_channel_id"]:
            raise RuntimeError(f"thread channel is not published: {row['channel_key']}")
        if not row["starter_discord_message_id"]:
            raise RuntimeError(f"thread starter is not published: {payload['starter_message_key']}")
        channel = await _fetch_channel(client, guild, row["discord_channel_id"])
        starter_message = await channel.fetch_message(int(row["starter_discord_message_id"]))
        await governor.wait()
        thread = await _create_thread(
            channel,
            starter_message,
            name=row["name"],
            reason=reason,
        )
        repository.mark_created(
            table="threads",
            key_column="thread_key",
            key=row["thread_key"],
            discord_column="discord_thread_id",
            discord_id=str(thread.id),
        )
        report.threads_created += 1
        return

    if op_type == "EDIT_THREAD_NAME":
        row = _single_row(
            repository,
            "SELECT * FROM threads WHERE thread_key = ?",
            (payload["thread_key"],),
            f"thread {payload['thread_key']}",
        )
        thread = await _fetch_channel(client, guild, row["discord_thread_id"])
        await governor.wait()
        await thread.edit(name=row["name"], reason=reason)
        repository.mark_published_name_current(
            table="threads",
            key_column="thread_key",
            key=row["thread_key"],
        )
        report.names_updated += 1
        return

    if op_type == "CREATE_MESSAGE":
        message_row = _message_row(repository, payload["message_key"])
        container = await _fetch_message_container(
            client,
            guild,
            repository,
            payload["container_key"],
        )
        content = _message_content(message_row, config)
        await governor.wait()
        message = await container.send(content)
        repository.mark_message_created(
            message_key=message_row["message_key"],
            discord_message_id=str(message.id),
            desired_content_hash=message_row["desired_content_hash"],
            desired_ordinal=int(payload["slot_index"]),
        )
        report.replies_created += 1
        return

    if op_type == "EDIT_MESSAGE":
        slot = _single_row(
            repository,
            "SELECT * FROM published_slots WHERE slot_key = ?",
            (payload["slot_key"],),
            f"slot {payload['slot_key']}",
        )
        message_row = _message_row(repository, payload["message_key"])
        container = await _fetch_message_container(
            client,
            guild,
            repository,
            slot["container_key"],
        )
        discord_message = await container.fetch_message(int(slot["discord_message_id"]))
        content = _message_content(message_row, config)
        await governor.wait()
        await discord_message.edit(content=content)
        repository.record_slot_edited(
            slot_key=slot["slot_key"],
            desired_message_key=message_row["message_key"],
            desired_content_hash=message_row["desired_content_hash"],
            desired_ordinal=int(payload["slot_index"]),
        )
        report.messages_edited += 1
        return

    if op_type == "DELETE_MESSAGE":
        slot = repository.connection.execute(
            "SELECT * FROM published_slots WHERE slot_key = ?",
            (payload["slot_key"],),
        ).fetchone()
        if slot is not None:
            container = await _fetch_message_container(
                client,
                guild,
                repository,
                slot["container_key"],
            )
            discord_message = await container.fetch_message(int(slot["discord_message_id"]))
            await governor.wait()
            await discord_message.delete()
            repository.record_slot_deleted(slot_key=slot["slot_key"])
            report.messages_deleted += 1
        return

    if op_type == "DELETE_THREAD":
        row = repository.connection.execute(
            "SELECT * FROM threads WHERE thread_key = ?",
            (payload["thread_key"],),
        ).fetchone()
        if row is not None and row["discord_thread_id"]:
            thread = await _fetch_channel(client, guild, row["discord_thread_id"])
            await governor.wait()
            await thread.delete(reason=reason)
            report.threads_deleted += 1
        repository.delete_structure_record(
            table="threads",
            key_column="thread_key",
            key=payload["thread_key"],
        )
        return

    if op_type == "DELETE_CHANNEL":
        row = repository.connection.execute(
            "SELECT * FROM channels WHERE channel_key = ?",
            (payload["channel_key"],),
        ).fetchone()
        if row is not None and row["discord_channel_id"]:
            channel = await _fetch_channel(client, guild, row["discord_channel_id"])
            await governor.wait()
            await channel.delete(reason=reason)
            report.channels_deleted += 1
        repository.delete_structure_record(
            table="channels",
            key_column="channel_key",
            key=payload["channel_key"],
        )
        return

    if op_type == "DELETE_CATEGORY":
        row = repository.connection.execute(
            "SELECT * FROM categories WHERE category_key = ?",
            (payload["category_key"],),
        ).fetchone()
        if row is not None and row["discord_category_id"]:
            category = await _fetch_channel(client, guild, row["discord_category_id"])
            await governor.wait()
            await category.delete(reason=reason)
            report.categories_deleted += 1
        repository.delete_structure_record(
            table="categories",
            key_column="category_key",
            key=payload["category_key"],
        )
        return

    raise RuntimeError(f"unsupported publication operation: {op_type}")


async def sync_calendar(
    *,
    config: EngineConfig,
    guild_key: str,
    max_actions: int,
    min_seconds_between_writes: float | None = None,
) -> dict[str, Any]:
    write_interval = (
        config.min_seconds_between_writes
        if min_seconds_between_writes is None
        else max(0.0, min_seconds_between_writes)
    )
    report = SyncReport(
        guild_key=guild_key,
        max_actions=max_actions,
        min_seconds_between_writes=write_interval,
    )
    governor = WriteGovernor(write_interval)

    database = EngineDatabase(config.state_db)
    database.initialize()
    connection = database.connect()
    repository = Repository(connection)
    try:
        report.plan = plan_publication_queue(
            repository,
            guild_key=guild_key,
        ).to_dict()
        connection.commit()

        if repository.is_paused("publication"):
            control = repository.control("publication")
            value = report.to_dict(repository.remaining_actions(guild_key))
            value["publication_paused"] = True
            value["control"] = control
            value["ready_actions"] = repository.ready_actions(guild_key)
            value["projection"] = repository.projection_summary(guild_key)
            value["queue"] = repository.queue_summary(guild_key)
            return value

        guild_record = repository.guild(guild_key)
        bot_head = repository.bot_head(guild_record["bot_head_key"])
        token = config.value(bot_head["token_env"])
        if not token:
            raise RuntimeError(f"{bot_head['token_env']} is required for live sync")
        if not guild_record["discord_guild_id"]:
            raise RuntimeError(f"guild {guild_key!r} has no discord_guild_id configured")

        discord = _ensure_discord()
        intents = discord.Intents.default()
        intents.guilds = True
        intents.messages = True
        client = discord.Client(intents=intents)

        @client.event
        async def on_ready():
            try:
                guild = client.get_guild(int(guild_record["discord_guild_id"]))
                if guild is None:
                    guild = await client.fetch_guild(int(guild_record["discord_guild_id"]))
                if guild is None:
                    raise RuntimeError(
                        f"bot cannot see guild {guild_record['discord_guild_id']}"
                    )

                while report.actions < max_actions:
                    operation = repository.next_pending_operation(guild_key)
                    if operation is None:
                        break
                    try:
                        repository.start_operation(operation["operation_id"])
                        connection.commit()
                        await _execute_operation(
                            operation=operation,
                            client=client,
                            guild=guild,
                            repository=repository,
                            config=config,
                            governor=governor,
                            report=report,
                        )
                        repository.complete_operation(operation["operation_id"])
                        repository.event(
                            "publication_queue.operation_completed",
                            guild_key=guild_key,
                            entity_type=operation["target_kind"],
                            entity_key=operation["target_key"],
                            details={
                                "operation_id": operation["operation_id"],
                                "operation_type": operation["operation_type"],
                                "generation_id": operation["generation_id"],
                            },
                        )
                        if operation["container_key"]:
                            repository.refresh_thread_projection_state(
                                thread_key=operation["container_key"],
                                guild_key=guild_key,
                            )
                        connection.commit()
                        report.actions += 1
                    except Exception as exc:
                        report.failed = True
                        report.error = str(exc)
                        repository.fail_operation(operation["operation_id"], str(exc))
                        repository.event(
                            "publication_queue.operation_failed",
                            guild_key=guild_key,
                            entity_type=operation["target_kind"],
                            entity_key=operation["target_key"],
                            details={
                                "operation_id": operation["operation_id"],
                                "operation_type": operation["operation_type"],
                                "error": str(exc),
                            },
                        )
                        connection.commit()
                        break
            finally:
                await client.close()

        await client.start(token)
        value = report.to_dict(repository.remaining_actions(guild_key))
        value["ready_actions"] = repository.ready_actions(guild_key)
        value["projection"] = repository.projection_summary(guild_key)
        value["queue"] = repository.queue_summary(guild_key)
        return value
    finally:
        connection.close()
