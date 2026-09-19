from __future__ import annotations

import hashlib
import json
import sqlite3
from typing import Any

from .timeutil import utc_now


def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return None if row is None else dict(row)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


class Repository:
    def __init__(self, connection: sqlite3.Connection):
        self.connection = connection

    def event(
        self,
        event_type: str,
        *,
        guild_key: str | None = None,
        entity_type: str = "system",
        entity_key: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO sync_events(
                occurred_at, guild_key, entity_type, entity_key, event_type, details_json
            ) VALUES(?, ?, ?, ?, ?, ?)
            """,
            (
                utc_now(),
                guild_key,
                entity_type,
                entity_key,
                event_type,
                json.dumps(details or {}, sort_keys=True),
            ),
        )

    def configure_bot_head(
        self,
        *,
        bot_head_key: str,
        display_name: str,
        token_env: str,
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO bot_heads(
                bot_head_key, display_name, token_env, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?)
            ON CONFLICT(bot_head_key) DO UPDATE SET
                display_name = excluded.display_name,
                token_env = excluded.token_env,
                updated_at = excluded.updated_at
            """,
            (bot_head_key, display_name, token_env, now, now),
        )
        self.event(
            "bot_head.configured",
            entity_type="bot_head",
            entity_key=bot_head_key,
            details={"display_name": display_name, "token_env": token_env},
        )

    def configure_guild(
        self,
        *,
        guild_key: str,
        name: str,
        purpose: str,
        server_model: str,
        bot_head_key: str,
        discord_guild_id: str | None,
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO guilds(
                guild_key, name, purpose, server_model, discord_guild_id,
                bot_head_key, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_key) DO UPDATE SET
                name = excluded.name,
                purpose = excluded.purpose,
                server_model = excluded.server_model,
                discord_guild_id = excluded.discord_guild_id,
                bot_head_key = excluded.bot_head_key,
                updated_at = excluded.updated_at
            """,
            (
                guild_key,
                name,
                purpose,
                server_model,
                discord_guild_id,
                bot_head_key,
                now,
                now,
            ),
        )
        self.event(
            "guild.configured",
            guild_key=guild_key,
            entity_type="guild",
            entity_key=guild_key,
            details={
                "name": name,
                "server_model": server_model,
                "discord_guild_id_set": bool(discord_guild_id),
            },
        )

    def guild(self, guild_key: str) -> dict[str, Any]:
        value = _dict(
            self.connection.execute(
                "SELECT * FROM guilds WHERE guild_key = ?",
                (guild_key,),
            ).fetchone()
        )
        if value is None:
            raise ValueError(f"unknown guild_key: {guild_key}")
        return value

    def bot_head(self, bot_head_key: str) -> dict[str, Any]:
        value = _dict(
            self.connection.execute(
                "SELECT * FROM bot_heads WHERE bot_head_key = ?",
                (bot_head_key,),
            ).fetchone()
        )
        if value is None:
            raise ValueError(f"unknown bot_head_key: {bot_head_key}")
        return value

    def upsert_category(
        self,
        *,
        category_key: str,
        guild_key: str,
        logical_key: str,
        name: str,
        desired_generation_id: int | None = None,
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO categories(
                category_key, guild_key, logical_key, name, desired_generation_id,
                status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, 'planned', ?, ?)
            ON CONFLICT(category_key) DO UPDATE SET
                name = excluded.name,
                desired_generation_id = COALESCE(
                    excluded.desired_generation_id,
                    categories.desired_generation_id
                ),
                updated_at = excluded.updated_at
            """,
            (
                category_key,
                guild_key,
                logical_key,
                name,
                desired_generation_id,
                now,
                now,
            ),
        )

    def upsert_channel(
        self,
        *,
        channel_key: str,
        guild_key: str,
        category_key: str,
        logical_key: str,
        name: str,
        desired_generation_id: int | None = None,
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO channels(
                channel_key, guild_key, category_key, logical_key, name,
                desired_generation_id, channel_type, status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, 'text', 'planned', ?, ?)
            ON CONFLICT(channel_key) DO UPDATE SET
                category_key = excluded.category_key,
                name = excluded.name,
                desired_generation_id = COALESCE(
                    excluded.desired_generation_id,
                    channels.desired_generation_id
                ),
                updated_at = excluded.updated_at
            """,
            (
                channel_key,
                guild_key,
                category_key,
                logical_key,
                name,
                desired_generation_id,
                now,
                now,
            ),
        )

    def upsert_thread(
        self,
        *,
        thread_key: str,
        guild_key: str,
        channel_key: str,
        logical_key: str,
        name: str,
        day_date: str,
        container_kind: str = "thread",
        desired_generation_id: int | None = None,
    ) -> None:
        if container_kind not in {"thread", "channel_stream"}:
            raise ValueError(f"unsupported container_kind: {container_kind}")
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO threads(
                thread_key, guild_key, channel_key, logical_key, name,
                day_date, container_kind, desired_generation_id, status, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET
                channel_key = excluded.channel_key,
                name = excluded.name,
                day_date = excluded.day_date,
                container_kind = excluded.container_kind,
                desired_generation_id = COALESCE(
                    excluded.desired_generation_id,
                    threads.desired_generation_id
                ),
                updated_at = excluded.updated_at
            """,
            (
                thread_key,
                guild_key,
                channel_key,
                logical_key,
                name,
                day_date,
                container_kind,
                desired_generation_id,
                now,
                now,
            ),
        )

    def upsert_message(
        self,
        *,
        message_key: str,
        guild_key: str,
        thread_key: str,
        logical_key: str,
        ordinal: int,
        role: str,
        content_kind: str,
        literal_text: str | None,
        desired_content_hash: str | None = None,
        desired_generation_id: int | None = None,
    ) -> None:
        now = utc_now()
        if desired_content_hash is None and literal_text is not None:
            desired_content_hash = _sha256_text(literal_text)
        self.connection.execute(
            """
            INSERT INTO messages(
                message_key, guild_key, thread_key, logical_key, ordinal, role,
                content_kind, literal_text, desired_content_hash, desired_generation_id, status,
                created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'planned', ?, ?)
            ON CONFLICT(message_key) DO UPDATE SET
                thread_key = excluded.thread_key,
                ordinal = excluded.ordinal,
                role = excluded.role,
                content_kind = excluded.content_kind,
                literal_text = excluded.literal_text,
                desired_content_hash = excluded.desired_content_hash,
                desired_generation_id = COALESCE(
                    excluded.desired_generation_id,
                    messages.desired_generation_id
                ),
                updated_at = excluded.updated_at
            """,
            (
                message_key,
                guild_key,
                thread_key,
                logical_key,
                ordinal,
                role,
                content_kind,
                literal_text,
                desired_content_hash,
                desired_generation_id,
                now,
                now,
            ),
        )

    def upsert_content_binding(
        self,
        *,
        message_key: str,
        source_system: str,
        payload: dict[str, Any],
    ) -> None:
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO content_bindings(
                message_key, source_system, source_payload_json, created_at
            ) VALUES(?, ?, ?, ?)
            ON CONFLICT(message_key, source_system) DO UPDATE SET
                source_payload_json = excluded.source_payload_json
            """,
            (message_key, source_system, _canonical_json(payload), now),
        )

    def reset_unpublished_projected_messages(self, guild_key: str) -> int:
        rows = self.connection.execute(
            """
            SELECT message_key
            FROM messages
            WHERE guild_key = ?
              AND role IN ('example', 'content')
            """,
            (guild_key,),
        ).fetchall()
        if not rows:
            return 0
        self.connection.executemany(
            "DELETE FROM messages WHERE message_key = ?",
            [(row["message_key"],) for row in rows],
        )
        self.event(
            "projection.projected_messages_removed",
            guild_key=guild_key,
            details={"count": len(rows)},
        )
        return len(rows)

    def mark_created(
        self,
        *,
        table: str,
        key_column: str,
        key: str,
        discord_column: str,
        discord_id: str,
    ) -> None:
        if table not in {"categories", "channels", "threads", "messages"}:
            raise ValueError(f"unsupported table: {table}")
        if key_column not in {"category_key", "channel_key", "thread_key", "message_key"}:
            raise ValueError(f"unsupported key column: {key_column}")
        if discord_column not in {
            "discord_category_id",
            "discord_channel_id",
            "discord_thread_id",
            "discord_message_id",
        }:
            raise ValueError(f"unsupported discord column: {discord_column}")
        published_name_assignment = (
            ", published_name = name"
            if table in {"categories", "channels", "threads"}
            else ""
        )
        self.connection.execute(
            f"""
            UPDATE {table}
            SET {discord_column} = ?,
                status = 'created',
                last_error = NULL,
                updated_at = ?
                {published_name_assignment}
            WHERE {key_column} = ?
            """,
            (discord_id, utc_now(), key),
        )

    def mark_channel_streams_created(self, *, channel_key: str, discord_channel_id: str) -> int:
        cursor = self.connection.execute(
            """
            UPDATE threads
            SET discord_thread_id = ?,
                status = 'created',
                published_name = name,
                last_error = NULL,
                updated_at = ?
            WHERE channel_key = ?
              AND COALESCE(container_kind, 'thread') = 'channel_stream'
              AND discord_thread_id IS NULL
            """,
            (discord_channel_id, utc_now(), channel_key),
        )
        return cursor.rowcount

    def mark_message_created(
        self,
        *,
        message_key: str,
        discord_message_id: str,
        desired_content_hash: str | None,
        desired_ordinal: int,
    ) -> None:
        row = self.connection.execute(
            """
            SELECT guild_key, thread_key
            FROM messages
            WHERE message_key = ?
            """,
            (message_key,),
        ).fetchone()
        if row is None:
            raise ValueError(f"unknown message_key: {message_key}")
        now = utc_now()
        self.connection.execute(
            """
            UPDATE messages
            SET discord_message_id = NULL,
                status = 'planned',
                published_content_hash = NULL,
                published_ordinal = NULL,
                updated_at = ?
            WHERE discord_message_id = ?
              AND message_key <> ?
            """,
            (now, discord_message_id, message_key),
        )
        self.connection.execute(
            """
            UPDATE messages
            SET discord_message_id = ?,
                status = 'created',
                last_error = NULL,
                published_content_hash = ?,
                published_ordinal = ?,
                updated_at = ?
            WHERE message_key = ?
            """,
            (
                discord_message_id,
                desired_content_hash,
                desired_ordinal,
                now,
                message_key,
            ),
        )
        self.connection.execute(
            """
            INSERT INTO published_slots(
                slot_key, guild_key, container_key, slot_index, discord_message_id,
                desired_message_key, published_content_hash, published_ordinal,
                status, last_error, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, 'active', NULL, ?, ?)
            ON CONFLICT(guild_key, discord_message_id) DO UPDATE SET
                container_key = excluded.container_key,
                slot_index = excluded.slot_index,
                desired_message_key = excluded.desired_message_key,
                published_content_hash = excluded.published_content_hash,
                published_ordinal = excluded.published_ordinal,
                status = 'active',
                last_error = NULL,
                updated_at = excluded.updated_at
            """,
            (
                f"{row['guild_key']}:slot:{discord_message_id}",
                row["guild_key"],
                row["thread_key"],
                desired_ordinal,
                discord_message_id,
                message_key,
                desired_content_hash,
                desired_ordinal,
                now,
                now,
            ),
        )

    def record_slot_edited(
        self,
        *,
        slot_key: str,
        desired_message_key: str,
        desired_content_hash: str | None,
        desired_ordinal: int,
    ) -> None:
        slot = self.connection.execute(
            """
            SELECT *
            FROM published_slots
            WHERE slot_key = ?
              AND status = 'active'
            """,
            (slot_key,),
        ).fetchone()
        if slot is None:
            raise ValueError(f"unknown active slot_key: {slot_key}")
        message = self.connection.execute(
            """
            SELECT guild_key, thread_key
            FROM messages
            WHERE message_key = ?
            """,
            (desired_message_key,),
        ).fetchone()
        if message is None:
            raise ValueError(f"unknown desired_message_key: {desired_message_key}")
        now = utc_now()
        previous_message_key = slot["desired_message_key"]
        if previous_message_key and previous_message_key != desired_message_key:
            self.connection.execute(
                """
                UPDATE messages
                SET discord_message_id = NULL,
                    status = 'planned',
                    published_content_hash = NULL,
                    published_ordinal = NULL,
                    updated_at = ?
                WHERE message_key = ?
                """,
                (now, previous_message_key),
            )
        self.connection.execute(
            """
            UPDATE published_slots
            SET container_key = ?,
                slot_index = ?,
                desired_message_key = ?,
                published_content_hash = ?,
                published_ordinal = ?,
                status = 'active',
                last_error = NULL,
                updated_at = ?
            WHERE slot_key = ?
            """,
            (
                message["thread_key"],
                desired_ordinal,
                desired_message_key,
                desired_content_hash,
                desired_ordinal,
                now,
                slot_key,
            ),
        )
        self.connection.execute(
            """
            UPDATE messages
            SET discord_message_id = ?,
                status = 'created',
                last_error = NULL,
                published_content_hash = ?,
                published_ordinal = ?,
                updated_at = ?
            WHERE message_key = ?
            """,
            (
                slot["discord_message_id"],
                desired_content_hash,
                desired_ordinal,
                now,
                desired_message_key,
            ),
        )

    def relink_slot_without_write(
        self,
        *,
        slot_key: str,
        desired_message_key: str,
        desired_content_hash: str | None,
        desired_ordinal: int,
    ) -> None:
        self.record_slot_edited(
            slot_key=slot_key,
            desired_message_key=desired_message_key,
            desired_content_hash=desired_content_hash,
            desired_ordinal=desired_ordinal,
        )

    def record_slot_deleted(self, *, slot_key: str) -> None:
        slot = self.connection.execute(
            """
            SELECT *
            FROM published_slots
            WHERE slot_key = ?
            """,
            (slot_key,),
        ).fetchone()
        if slot is None:
            return
        now = utc_now()
        if slot["desired_message_key"]:
            self.connection.execute(
                """
                UPDATE messages
                SET discord_message_id = NULL,
                    status = 'planned',
                    published_content_hash = NULL,
                    published_ordinal = NULL,
                    updated_at = ?
                WHERE message_key = ?
                """,
                (now, slot["desired_message_key"]),
            )
        self.connection.execute(
            "DELETE FROM published_slots WHERE slot_key = ?",
            (slot_key,),
        )

    def latest_generation_id(self, guild_key: str) -> int:
        row = self.connection.execute(
            """
            SELECT MAX(run_id) AS generation_id
            FROM projection_runs
            WHERE guild_key = ?
              AND completed_at IS NOT NULL
            """,
            (guild_key,),
        ).fetchone()
        return int(row["generation_id"] or 0)

    def cancel_pending_queue_for_other_generations(
        self,
        *,
        guild_key: str,
        generation_id: int,
    ) -> int:
        cursor = self.connection.execute(
            """
            UPDATE publication_queue
            SET state = 'canceled',
                updated_at = ?,
                completed_at = ?
            WHERE guild_key = ?
              AND generation_id <> ?
              AND state IN ('pending', 'running', 'failed')
            """,
            (utc_now(), utc_now(), guild_key, generation_id),
        )
        return cursor.rowcount

    def enqueue_operation(
        self,
        *,
        operation_id: str,
        guild_key: str,
        generation_id: int,
        priority: int,
        sequence: int,
        operation_type: str,
        target_kind: str,
        target_key: str,
        container_key: str | None,
        payload: dict[str, Any],
    ) -> bool:
        existing = self.connection.execute(
            """
            SELECT state
            FROM publication_queue
            WHERE operation_id = ?
            """,
            (operation_id,),
        ).fetchone()
        if existing is not None and existing["state"] == "completed":
            return False
        is_new_operation = existing is None
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO publication_queue(
                operation_id, guild_key, generation_id, priority, sequence,
                operation_type, target_kind, target_key, container_key,
                payload_json, state, attempts, created_at, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?)
            ON CONFLICT(operation_id) DO UPDATE SET
                generation_id = excluded.generation_id,
                priority = excluded.priority,
                sequence = excluded.sequence,
                operation_type = excluded.operation_type,
                target_kind = excluded.target_kind,
                target_key = excluded.target_key,
                container_key = excluded.container_key,
                payload_json = excluded.payload_json,
                state = CASE
                    WHEN publication_queue.state = 'completed' THEN publication_queue.state
                    ELSE 'pending'
                END,
                last_error = NULL,
                updated_at = excluded.updated_at
            """,
            (
                operation_id,
                guild_key,
                generation_id,
                priority,
                sequence,
                operation_type,
                target_kind,
                target_key,
                container_key,
                _canonical_json(payload),
                now,
                now,
            ),
        )
        return is_new_operation

    def next_pending_operation(self, guild_key: str) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT *
            FROM publication_queue
            WHERE guild_key = ?
              AND state = 'pending'
            ORDER BY priority, sequence, created_at, operation_id
            LIMIT 1
            """,
            (guild_key,),
        ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["payload"] = json.loads(value.pop("payload_json"))
        return value

    def start_operation(self, operation_id: str) -> None:
        self.connection.execute(
            """
            UPDATE publication_queue
            SET state = 'running',
                attempts = attempts + 1,
                updated_at = ?
            WHERE operation_id = ?
              AND state = 'pending'
            """,
            (utc_now(), operation_id),
        )

    def complete_operation(self, operation_id: str) -> None:
        now = utc_now()
        self.connection.execute(
            """
            UPDATE publication_queue
            SET state = 'completed',
                last_error = NULL,
                updated_at = ?,
                completed_at = ?
            WHERE operation_id = ?
            """,
            (now, now, operation_id),
        )

    def fail_operation(self, operation_id: str, error: str) -> None:
        self.connection.execute(
            """
            UPDATE publication_queue
            SET state = 'failed',
                last_error = ?,
                updated_at = ?
            WHERE operation_id = ?
            """,
            (error[:1000], utc_now(), operation_id),
        )

    def queue_summary(self, guild_key: str) -> dict[str, int]:
        rows = self.connection.execute(
            """
            SELECT state, COUNT(*) AS count
            FROM publication_queue
            WHERE guild_key = ?
            GROUP BY state
            """,
            (guild_key,),
        ).fetchall()
        return {row["state"]: row["count"] for row in rows}

    def published_slot_count(self, guild_key: str) -> int:
        return int(
            self.connection.execute(
                """
                SELECT COUNT(*)
                FROM published_slots
                WHERE guild_key = ?
                  AND status = 'active'
                """,
                (guild_key,),
            ).fetchone()[0]
        )

    def mark_published_name_current(
        self,
        *,
        table: str,
        key_column: str,
        key: str,
    ) -> None:
        if table not in {"categories", "channels", "threads"}:
            raise ValueError(f"unsupported table: {table}")
        if key_column not in {"category_key", "channel_key", "thread_key"}:
            raise ValueError(f"unsupported key column: {key_column}")
        self.connection.execute(
            f"""
            UPDATE {table}
            SET published_name = name,
                status = 'created',
                last_error = NULL,
                updated_at = ?
            WHERE {key_column} = ?
            """,
            (utc_now(), key),
        )

    def delete_structure_record(
        self,
        *,
        table: str,
        key_column: str,
        key: str,
    ) -> None:
        if table not in {"categories", "channels", "threads"}:
            raise ValueError(f"unsupported table: {table}")
        if key_column not in {"category_key", "channel_key", "thread_key"}:
            raise ValueError(f"unsupported key column: {key_column}")
        self.connection.execute(
            f"DELETE FROM {table} WHERE {key_column} = ?",
            (key,),
        )

    def mark_failed(self, *, table: str, key_column: str, key: str, error: str) -> None:
        if table not in {"categories", "channels", "threads", "messages"}:
            raise ValueError(f"unsupported table: {table}")
        if key_column not in {"category_key", "channel_key", "thread_key", "message_key"}:
            raise ValueError(f"unsupported key column: {key_column}")
        self.connection.execute(
            f"""
            UPDATE {table}
            SET status = 'failed', last_error = ?, updated_at = ?
            WHERE {key_column} = ?
            """,
            (error[:1000], utc_now(), key),
        )

    def stats(self, guild_key: str | None = None) -> dict[str, Any]:
        args = () if guild_key is None else (guild_key,)
        where = "" if guild_key is None else "WHERE guild_key = ?"
        stats: dict[str, Any] = {}
        for table in ("categories", "channels", "threads", "messages"):
            rows = self.connection.execute(
                f"""
                SELECT status, COUNT(*) AS count
                FROM {table}
                {where}
                GROUP BY status
                """,
                args,
            ).fetchall()
            stats[table] = {row["status"]: row["count"] for row in rows}
            stats[table]["total"] = sum(stats[table].values())
        stats["guilds"] = self.connection.execute("SELECT COUNT(*) FROM guilds").fetchone()[0]
        stats["bot_heads"] = self.connection.execute("SELECT COUNT(*) FROM bot_heads").fetchone()[0]
        return stats

    def control(self, target: str) -> dict[str, Any] | None:
        return _dict(
            self.connection.execute(
                "SELECT * FROM controls WHERE target = ?",
                (target,),
            ).fetchone()
        )

    def controls(self) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                "SELECT * FROM controls ORDER BY target"
            )
        ]

    def set_control(self, target: str, paused: bool, reason: str) -> None:
        if target != "publication":
            raise ValueError(f"unsupported control target: {target}")
        if not reason.strip():
            raise ValueError("control changes require a reason")
        self.connection.execute(
            """
            INSERT INTO controls(target, paused, reason, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(target) DO UPDATE SET
                paused = excluded.paused,
                reason = excluded.reason,
                updated_at = excluded.updated_at
            """,
            (target, int(paused), reason.strip(), utc_now()),
        )
        self.event(
            "control.changed",
            entity_type="control",
            entity_key=target,
            details={"paused": paused, "reason": reason.strip()},
        )

    def is_paused(self, target: str) -> bool:
        row = self.connection.execute(
            "SELECT paused FROM controls WHERE target = ?",
            (target,),
        ).fetchone()
        return True if row is None else bool(row["paused"])

    def begin_projection_run(
        self,
        *,
        guild_key: str,
        source_system: str,
        stats: dict[str, Any],
    ) -> int:
        cursor = self.connection.execute(
            """
            INSERT INTO projection_runs(guild_key, source_system, started_at, stats_json)
            VALUES(?, ?, ?, ?)
            """,
            (guild_key, source_system, utc_now(), _canonical_json(stats)),
        )
        return int(cursor.lastrowid)

    def complete_projection_run(self, run_id: int, stats: dict[str, Any]) -> None:
        self.connection.execute(
            """
            UPDATE projection_runs
            SET completed_at = ?, stats_json = ?
            WHERE run_id = ?
            """,
            (utc_now(), _canonical_json(stats), run_id),
        )

    def projection_summary(self, guild_key: str) -> dict[str, Any]:
        rows = self.connection.execute(
            """
            SELECT publication_state, COUNT(*) AS count
            FROM thread_projection_state
            WHERE guild_key = ?
            GROUP BY publication_state
            """,
            (guild_key,),
        ).fetchall()
        latest = _dict(
            self.connection.execute(
                """
                SELECT run_id, source_system, started_at, completed_at, stats_json
                FROM projection_runs
                WHERE guild_key = ?
                ORDER BY run_id DESC
                LIMIT 1
                """,
                (guild_key,),
            ).fetchone()
        )
        if latest is not None:
            latest["stats"] = json.loads(latest.pop("stats_json"))
        return {
            "states": {row["publication_state"]: row["count"] for row in rows},
            "latest_run": latest,
        }

    def refresh_thread_projection_state(self, *, thread_key: str, guild_key: str) -> dict[str, Any]:
        desired_rows = [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT message_key, ordinal, role, content_kind, desired_content_hash
                FROM messages
                WHERE thread_key = ?
                ORDER BY ordinal, message_key
                """,
                (thread_key,),
            )
        ]
        desired_signature = _sha256_text(_canonical_json(desired_rows))

        published_rows = [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT slots.desired_message_key AS message_key,
                       slots.published_ordinal AS ordinal,
                       messages.role,
                       messages.content_kind,
                       slots.published_content_hash AS desired_content_hash
                FROM published_slots slots
                LEFT JOIN messages ON messages.message_key = slots.desired_message_key
                WHERE slots.container_key = ?
                  AND slots.status = 'active'
                ORDER BY slots.slot_index, slots.slot_key
                """,
                (thread_key,),
            )
        ]
        if not desired_rows and not published_rows:
            published_signature = desired_signature
        elif not desired_rows:
            published_signature = None
        elif len(published_rows) != len(desired_rows):
            published_signature = None
        elif any(
            row["message_key"] is None
            or row["ordinal"] is None
            or row["role"] is None
            or row["content_kind"] is None
            or row["desired_content_hash"] is None
            for row in published_rows
        ):
            published_signature = None
        else:
            published_signature = _sha256_text(_canonical_json(published_rows))

        if published_signature is None:
            state = "unpublished"
        elif published_signature == desired_signature:
            state = "current"
        else:
            state = "stale"
        impact_count = self.connection.execute(
            """
            SELECT COUNT(*)
            FROM published_slots
            WHERE container_key = ?
              AND status = 'active'
            """,
            (thread_key,),
        ).fetchone()[0]
        now = utc_now()
        self.connection.execute(
            """
            INSERT INTO thread_projection_state(
                thread_key, guild_key, desired_signature, published_signature,
                publication_state, impact_count, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(thread_key) DO UPDATE SET
                desired_signature = excluded.desired_signature,
                published_signature = excluded.published_signature,
                publication_state = excluded.publication_state,
                impact_count = excluded.impact_count,
                updated_at = excluded.updated_at
            """,
            (
                thread_key,
                guild_key,
                desired_signature,
                published_signature,
                state,
                impact_count,
                now,
            ),
        )
        return {
            "thread_key": thread_key,
            "publication_state": state,
            "impact_count": impact_count,
        }

    def refresh_all_thread_projection_states(self, guild_key: str) -> dict[str, int]:
        states: dict[str, int] = {}
        rows = self.connection.execute(
            "SELECT thread_key FROM threads WHERE guild_key = ? ORDER BY day_date",
            (guild_key,),
        ).fetchall()
        for row in rows:
            state = self.refresh_thread_projection_state(
                thread_key=row["thread_key"],
                guild_key=guild_key,
            )
            key = state["publication_state"]
            states[key] = states.get(key, 0) + 1
        return states

    def calendar_sample(self, guild_key: str, limit: int) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT threads.day_date, categories.name AS category_name,
                   channels.name AS channel_name, threads.name AS thread_name,
                   COUNT(messages.message_key) AS intended_messages,
                   SUM(CASE WHEN messages.status = 'created' THEN 1 ELSE 0 END) AS created_messages
            FROM threads
            JOIN channels ON channels.channel_key = threads.channel_key
            JOIN categories ON categories.category_key = channels.category_key
            JOIN messages ON messages.thread_key = threads.thread_key
            WHERE threads.guild_key = ?
            GROUP BY threads.thread_key
            ORDER BY threads.day_date
            LIMIT ?
            """,
            (guild_key, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def pending_categories(self, guild_key: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT * FROM categories
                WHERE guild_key = ? AND discord_category_id IS NULL
                ORDER BY name
                """,
                (guild_key,),
            )
        ]

    def pending_channels(self, guild_key: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT channels.*, categories.discord_category_id
                FROM channels
                JOIN categories ON categories.category_key = channels.category_key
                WHERE channels.guild_key = ?
                  AND channels.discord_channel_id IS NULL
                  AND categories.discord_category_id IS NOT NULL
                ORDER BY channels.name
                """,
                (guild_key,),
            )
        ]

    def pending_threads(self, guild_key: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT threads.*, channels.discord_channel_id,
                       starter.message_key AS starter_message_key,
                       starter.literal_text AS starter_text,
                       starter.desired_content_hash AS starter_desired_content_hash,
                       starter.discord_message_id AS starter_discord_message_id
                FROM threads
                JOIN channels ON channels.channel_key = threads.channel_key
                JOIN messages starter
                  ON starter.thread_key = threads.thread_key
                 AND starter.ordinal = 0
                WHERE threads.guild_key = ?
                  AND COALESCE(threads.container_kind, 'thread') = 'thread'
                  AND threads.discord_thread_id IS NULL
                  AND channels.discord_channel_id IS NOT NULL
                ORDER BY threads.day_date
                """,
                (guild_key,),
            )
        ]

    def pending_replies(self, guild_key: str) -> list[dict[str, Any]]:
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT messages.*, threads.discord_thread_id
                , bindings.source_payload_json
                FROM messages
                JOIN threads ON threads.thread_key = messages.thread_key
                LEFT JOIN content_bindings bindings
                  ON bindings.message_key = messages.message_key
                 AND bindings.source_system = 'conversation_engine'
                WHERE messages.guild_key = ?
                  AND messages.ordinal > 0
                  AND messages.discord_message_id IS NULL
                  AND threads.discord_thread_id IS NOT NULL
                ORDER BY threads.day_date, messages.ordinal
                """,
                (guild_key,),
            )
        ]

    def remaining_actions(self, guild_key: str) -> dict[str, int]:
        return {
            "categories": self.connection.execute(
                """
                SELECT COUNT(*) FROM categories
                WHERE guild_key = ? AND discord_category_id IS NULL
                """,
                (guild_key,),
            ).fetchone()[0],
            "channels": self.connection.execute(
                """
                SELECT COUNT(*) FROM channels
                WHERE guild_key = ? AND discord_channel_id IS NULL
                """,
                (guild_key,),
            ).fetchone()[0],
            "threads": self.connection.execute(
                """
                SELECT COUNT(*) FROM threads
                WHERE guild_key = ?
                  AND COALESCE(container_kind, 'thread') = 'thread'
                  AND discord_thread_id IS NULL
                """,
                (guild_key,),
            ).fetchone()[0],
            "messages": self.connection.execute(
                """
                SELECT COUNT(*) FROM messages
                WHERE guild_key = ? AND discord_message_id IS NULL
                """,
                (guild_key,),
            ).fetchone()[0],
        }

    def ready_actions(self, guild_key: str) -> dict[str, int]:
        return {
            "categories": len(self.pending_categories(guild_key)),
            "channels": len(self.pending_channels(guild_key)),
            "threads": len(self.pending_threads(guild_key)),
            "replies": len(self.pending_replies(guild_key)),
        }
