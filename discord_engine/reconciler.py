from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .repository import Repository


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _operation_id(
    *,
    guild_key: str,
    generation_id: int,
    operation_type: str,
    target_key: str,
    sequence: int,
    payload: dict[str, Any],
) -> str:
    basis = _canonical_json(
        {
            "guild_key": guild_key,
            "operation_type": operation_type,
            "target_key": target_key,
            "sequence": sequence,
            "payload": payload,
        }
    )
    return f"op_{hashlib.sha256(basis.encode('utf-8')).hexdigest()[:32]}"


def _generation_clause(alias: str, generation_id: int) -> tuple[str, tuple[int, ...]]:
    if generation_id > 0:
        return f"{alias}.desired_generation_id = ?", (generation_id,)
    return f"{alias}.desired_generation_id IS NULL", ()


@dataclass
class ReconciliationReport:
    guild_key: str
    generation_id: int
    canceled_stale_operations: int = 0
    enqueued_operations: int = 0
    relinked_slots: int = 0
    containers_checked: int = 0
    operations_by_type: dict[str, int] = field(default_factory=dict)

    def record_enqueue(self, operation_type: str, added: bool) -> None:
        if not added:
            return
        self.enqueued_operations += 1
        self.operations_by_type[operation_type] = (
            self.operations_by_type.get(operation_type, 0) + 1
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "guild_key": self.guild_key,
            "generation_id": self.generation_id,
            "canceled_stale_operations": self.canceled_stale_operations,
            "enqueued_operations": self.enqueued_operations,
            "relinked_slots": self.relinked_slots,
            "containers_checked": self.containers_checked,
            "operations_by_type": self.operations_by_type,
        }


class PublicationReconciler:
    def __init__(self, repository: Repository, *, guild_key: str):
        self.repository = repository
        self.connection = repository.connection
        self.guild_key = guild_key
        self.generation_id = repository.latest_generation_id(guild_key)
        self.report = ReconciliationReport(
            guild_key=guild_key,
            generation_id=self.generation_id,
        )
        self._sequence = 0

    def plan(self) -> ReconciliationReport:
        self._plan_categories()
        self._plan_channels()
        self._mark_channel_streams()
        self._plan_threads()
        self._plan_message_containers()
        self._plan_stale_structure_removals()
        self.report.canceled_stale_operations = (
            self.repository.cancel_pending_queue_for_other_generations(
                guild_key=self.guild_key,
                generation_id=self.generation_id,
            )
        )
        self.repository.event(
            "publication_queue.planned",
            guild_key=self.guild_key,
            details=self.report.to_dict(),
        )
        return self.report

    def _enqueue(
        self,
        *,
        priority: int,
        operation_type: str,
        target_kind: str,
        target_key: str,
        container_key: str | None = None,
        payload: dict[str, Any],
    ) -> None:
        self._sequence += 1
        operation_id = _operation_id(
            guild_key=self.guild_key,
            generation_id=self.generation_id,
            operation_type=operation_type,
            target_key=target_key,
            sequence=self._sequence,
            payload=payload,
        )
        added = self.repository.enqueue_operation(
            operation_id=operation_id,
            guild_key=self.guild_key,
            generation_id=self.generation_id,
            priority=priority,
            sequence=self._sequence,
            operation_type=operation_type,
            target_kind=target_kind,
            target_key=target_key,
            container_key=container_key,
            payload=payload,
        )
        self.report.record_enqueue(operation_type, added)

    def _plan_categories(self) -> None:
        clause, args = _generation_clause("categories", self.generation_id)
        rows = self.connection.execute(
            f"""
            SELECT *
            FROM categories
            WHERE guild_key = ?
              AND {clause}
            ORDER BY name, category_key
            """,
            (self.guild_key, *args),
        ).fetchall()
        for row in rows:
            if row["discord_category_id"] is None:
                self._enqueue(
                    priority=10,
                    operation_type="CREATE_CATEGORY",
                    target_kind="category",
                    target_key=row["category_key"],
                    payload={"category_key": row["category_key"]},
                )
            elif row["published_name"] and row["published_name"] != row["name"]:
                self._enqueue(
                    priority=11,
                    operation_type="EDIT_CATEGORY_NAME",
                    target_kind="category",
                    target_key=row["category_key"],
                    payload={"category_key": row["category_key"]},
                )

    def _plan_channels(self) -> None:
        clause, args = _generation_clause("channels", self.generation_id)
        rows = self.connection.execute(
            f"""
            SELECT channels.*, categories.discord_category_id
            FROM channels
            JOIN categories ON categories.category_key = channels.category_key
            WHERE channels.guild_key = ?
              AND {clause}
            ORDER BY channels.name, channels.channel_key
            """,
            (self.guild_key, *args),
        ).fetchall()
        for row in rows:
            if row["discord_channel_id"] is None:
                self._enqueue(
                    priority=20,
                    operation_type="CREATE_CHANNEL",
                    target_kind="channel",
                    target_key=row["channel_key"],
                    payload={
                        "channel_key": row["channel_key"],
                        "category_key": row["category_key"],
                    },
                )
            elif row["published_name"] and row["published_name"] != row["name"]:
                self._enqueue(
                    priority=21,
                    operation_type="EDIT_CHANNEL_NAME",
                    target_kind="channel",
                    target_key=row["channel_key"],
                    payload={"channel_key": row["channel_key"]},
                )

    def _mark_channel_streams(self) -> None:
        thread_clause, thread_args = _generation_clause("threads", self.generation_id)
        rows = self.connection.execute(
            f"""
            SELECT threads.thread_key, channels.channel_key, channels.discord_channel_id
            FROM threads
            JOIN channels ON channels.channel_key = threads.channel_key
            WHERE threads.guild_key = ?
              AND {thread_clause}
              AND COALESCE(threads.container_kind, 'thread') = 'channel_stream'
              AND threads.discord_thread_id IS NULL
              AND channels.discord_channel_id IS NOT NULL
            """,
            (self.guild_key, *thread_args),
        ).fetchall()
        for row in rows:
            changed = self.repository.mark_channel_streams_created(
                channel_key=row["channel_key"],
                discord_channel_id=row["discord_channel_id"],
            )
            self.report.relinked_slots += changed

    def _plan_threads(self) -> None:
        clause, args = _generation_clause("threads", self.generation_id)
        rows = self.connection.execute(
            f"""
            SELECT threads.*, channels.discord_channel_id,
                   starter.message_key AS starter_message_key,
                   starter.discord_message_id AS starter_discord_message_id
            FROM threads
            JOIN channels ON channels.channel_key = threads.channel_key
            LEFT JOIN messages starter
              ON starter.thread_key = threads.thread_key
             AND starter.ordinal = 0
             AND starter.role = 'thread_starter'
            WHERE threads.guild_key = ?
              AND {clause}
              AND COALESCE(threads.container_kind, 'thread') = 'thread'
            ORDER BY threads.day_date, threads.thread_key
            """,
            (self.guild_key, *args),
        ).fetchall()
        for row in rows:
            if row["discord_thread_id"] is None:
                if row["starter_message_key"] and row["starter_discord_message_id"] is None:
                    self._enqueue(
                        priority=30,
                        operation_type="CREATE_THREAD_STARTER",
                        target_kind="message",
                        target_key=row["starter_message_key"],
                        container_key=row["thread_key"],
                        payload={
                            "thread_key": row["thread_key"],
                            "channel_key": row["channel_key"],
                            "message_key": row["starter_message_key"],
                        },
                    )
                self._enqueue(
                    priority=31,
                    operation_type="CREATE_THREAD",
                    target_kind="thread",
                    target_key=row["thread_key"],
                    container_key=row["thread_key"],
                    payload={
                        "thread_key": row["thread_key"],
                        "channel_key": row["channel_key"],
                        "starter_message_key": row["starter_message_key"],
                    },
                )
            elif row["published_name"] and row["published_name"] != row["name"]:
                self._enqueue(
                    priority=32,
                    operation_type="EDIT_THREAD_NAME",
                    target_kind="thread",
                    target_key=row["thread_key"],
                    container_key=row["thread_key"],
                    payload={"thread_key": row["thread_key"]},
                )

    def _plan_message_containers(self) -> None:
        clause, args = _generation_clause("threads", self.generation_id)
        rows = self.connection.execute(
            f"""
            SELECT thread_key
            FROM threads
            WHERE guild_key = ?
              AND {clause}
            ORDER BY day_date, thread_key
            """,
            (self.guild_key, *args),
        ).fetchall()
        for row in rows:
            self._plan_container_messages(row["thread_key"])

    def _plan_container_messages(self, container_key: str) -> None:
        self.report.containers_checked += 1
        desired = [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT messages.*, bindings.source_payload_json
                FROM messages
                LEFT JOIN content_bindings bindings
                  ON bindings.message_key = messages.message_key
                 AND bindings.source_system = 'conversation_engine'
                WHERE messages.thread_key = ?
                  AND messages.role <> 'thread_starter'
                ORDER BY messages.ordinal, messages.message_key
                """,
                (container_key,),
            )
        ]
        slots = [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT slots.*
                FROM published_slots slots
                WHERE slots.container_key = ?
                  AND slots.status = 'active'
                  AND slots.slot_index > 0
                ORDER BY slots.slot_index, slots.slot_key
                """,
                (container_key,),
            )
        ]
        shared = min(len(desired), len(slots))
        for index in range(shared):
            wanted = desired[index]
            slot = slots[index]
            target_ordinal = int(wanted["ordinal"])
            hash_matches = slot["published_content_hash"] == wanted["desired_content_hash"]
            key_matches = slot["desired_message_key"] == wanted["message_key"]
            ordinal_matches = int(slot["slot_index"]) == target_ordinal
            if hash_matches and ordinal_matches:
                if not key_matches or wanted["discord_message_id"] is None:
                    self.repository.relink_slot_without_write(
                        slot_key=slot["slot_key"],
                        desired_message_key=wanted["message_key"],
                        desired_content_hash=wanted["desired_content_hash"],
                        desired_ordinal=target_ordinal,
                    )
                    self.report.relinked_slots += 1
                continue
            self._enqueue(
                priority=50,
                operation_type="EDIT_MESSAGE",
                target_kind="message",
                target_key=slot["slot_key"],
                container_key=container_key,
                payload={
                    "slot_key": slot["slot_key"],
                    "message_key": wanted["message_key"],
                    "container_key": container_key,
                    "slot_index": target_ordinal,
                },
            )
        for wanted in desired[shared:]:
            self._enqueue(
                priority=51,
                operation_type="CREATE_MESSAGE",
                target_kind="message",
                target_key=wanted["message_key"],
                container_key=container_key,
                payload={
                    "message_key": wanted["message_key"],
                    "container_key": container_key,
                    "slot_index": int(wanted["ordinal"]),
                },
            )
        for slot in reversed(slots[shared:]):
            self._enqueue(
                priority=52,
                operation_type="DELETE_MESSAGE",
                target_kind="message",
                target_key=slot["slot_key"],
                container_key=container_key,
                payload={
                    "slot_key": slot["slot_key"],
                    "discord_message_id": slot["discord_message_id"],
                    "container_key": container_key,
                },
            )

    def _plan_stale_structure_removals(self) -> None:
        if self.generation_id <= 0:
            return
        current = self.generation_id
        stale_threads = self.connection.execute(
            """
            SELECT threads.thread_key
            FROM threads
            JOIN channels ON channels.channel_key = threads.channel_key
            WHERE threads.guild_key = ?
              AND COALESCE(threads.container_kind, 'thread') = 'thread'
              AND threads.discord_thread_id IS NOT NULL
              AND COALESCE(threads.desired_generation_id, -1) <> ?
              AND COALESCE(channels.desired_generation_id, -1) = ?
            ORDER BY threads.day_date, threads.thread_key
            """,
            (self.guild_key, current, current),
        ).fetchall()
        for row in stale_threads:
            self._enqueue(
                priority=80,
                operation_type="DELETE_THREAD",
                target_kind="thread",
                target_key=row["thread_key"],
                container_key=row["thread_key"],
                payload={"thread_key": row["thread_key"]},
            )
        stale_channels = self.connection.execute(
            """
            SELECT channel_key
            FROM channels
            WHERE guild_key = ?
              AND discord_channel_id IS NOT NULL
              AND COALESCE(desired_generation_id, -1) <> ?
            ORDER BY name, channel_key
            """,
            (self.guild_key, current),
        ).fetchall()
        for row in stale_channels:
            self._enqueue(
                priority=90,
                operation_type="DELETE_CHANNEL",
                target_kind="channel",
                target_key=row["channel_key"],
                payload={"channel_key": row["channel_key"]},
            )
        stale_categories = self.connection.execute(
            """
            SELECT category_key
            FROM categories
            WHERE guild_key = ?
              AND discord_category_id IS NOT NULL
              AND COALESCE(desired_generation_id, -1) <> ?
              AND NOT EXISTS (
                  SELECT 1
                  FROM channels
                  WHERE channels.category_key = categories.category_key
                    AND COALESCE(channels.desired_generation_id, -1) = ?
              )
            ORDER BY name, category_key
            """,
            (self.guild_key, current, current),
        ).fetchall()
        for row in stale_categories:
            self._enqueue(
                priority=100,
                operation_type="DELETE_CATEGORY",
                target_kind="category",
                target_key=row["category_key"],
                payload={"category_key": row["category_key"]},
            )


def plan_publication_queue(
    repository: Repository,
    *,
    guild_key: str,
) -> ReconciliationReport:
    return PublicationReconciler(repository, guild_key=guild_key).plan()
