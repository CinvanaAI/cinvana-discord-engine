from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path

from .timeutil import utc_now


SCHEMA_VERSION = 4


SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_migrations (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS bot_heads (
    bot_head_key TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    token_env TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS guilds (
    guild_key TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    purpose TEXT NOT NULL,
    server_model TEXT NOT NULL CHECK (server_model IN ('chronological_private', 'mapping')),
    discord_guild_id TEXT,
    bot_head_key TEXT NOT NULL REFERENCES bot_heads(bot_head_key),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS categories (
    category_key TEXT PRIMARY KEY,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    logical_key TEXT NOT NULL,
    name TEXT NOT NULL,
    published_name TEXT,
    desired_generation_id INTEGER,
    discord_category_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('planned', 'created', 'failed')),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (guild_key, logical_key)
);

CREATE TABLE IF NOT EXISTS channels (
    channel_key TEXT PRIMARY KEY,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    category_key TEXT NOT NULL REFERENCES categories(category_key) ON DELETE CASCADE,
    logical_key TEXT NOT NULL,
    name TEXT NOT NULL,
    published_name TEXT,
    desired_generation_id INTEGER,
    channel_type TEXT NOT NULL CHECK (channel_type IN ('text')),
    discord_channel_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('planned', 'created', 'failed')),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (guild_key, logical_key)
);

CREATE TABLE IF NOT EXISTS threads (
    thread_key TEXT PRIMARY KEY,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    channel_key TEXT NOT NULL REFERENCES channels(channel_key) ON DELETE CASCADE,
    logical_key TEXT NOT NULL,
    container_kind TEXT NOT NULL DEFAULT 'thread' CHECK (container_kind IN ('thread', 'channel_stream')),
    name TEXT NOT NULL,
    published_name TEXT,
    desired_generation_id INTEGER,
    day_date TEXT NOT NULL,
    discord_thread_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('planned', 'created', 'failed')),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (guild_key, logical_key)
);

CREATE TABLE IF NOT EXISTS messages (
    message_key TEXT PRIMARY KEY,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    thread_key TEXT NOT NULL REFERENCES threads(thread_key) ON DELETE CASCADE,
    logical_key TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    role TEXT NOT NULL CHECK (role IN ('thread_starter', 'example', 'content')),
    content_kind TEXT NOT NULL CHECK (content_kind IN ('literal', 'ce_pointer')),
    literal_text TEXT,
    desired_content_hash TEXT,
    desired_generation_id INTEGER,
    published_content_hash TEXT,
    published_ordinal INTEGER,
    discord_message_id TEXT,
    status TEXT NOT NULL CHECK (status IN ('planned', 'created', 'failed')),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (guild_key, logical_key),
    UNIQUE (thread_key, ordinal)
);

CREATE TABLE IF NOT EXISTS content_bindings (
    binding_id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_key TEXT NOT NULL REFERENCES messages(message_key) ON DELETE CASCADE,
    source_system TEXT NOT NULL,
    source_payload_json TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE (message_key, source_system)
);

CREATE TABLE IF NOT EXISTS controls (
    target TEXT PRIMARY KEY,
    paused INTEGER NOT NULL CHECK (paused IN (0, 1)),
    reason TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projection_runs (
    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    source_system TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    stats_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS thread_projection_state (
    thread_key TEXT PRIMARY KEY REFERENCES threads(thread_key) ON DELETE CASCADE,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    desired_signature TEXT NOT NULL,
    published_signature TEXT,
    publication_state TEXT NOT NULL CHECK (
        publication_state IN ('unpublished', 'current', 'stale')
    ),
    impact_count INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS published_slots (
    slot_key TEXT PRIMARY KEY,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    container_key TEXT NOT NULL REFERENCES threads(thread_key) ON DELETE CASCADE,
    slot_index INTEGER NOT NULL CHECK (slot_index >= 0),
    discord_message_id TEXT NOT NULL,
    desired_message_key TEXT REFERENCES messages(message_key) ON DELETE SET NULL,
    published_content_hash TEXT,
    published_ordinal INTEGER,
    status TEXT NOT NULL CHECK (status IN ('active', 'deleted', 'failed')),
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE (guild_key, container_key, slot_index),
    UNIQUE (guild_key, discord_message_id)
);

CREATE TABLE IF NOT EXISTS publication_queue (
    operation_id TEXT PRIMARY KEY,
    guild_key TEXT NOT NULL REFERENCES guilds(guild_key) ON DELETE CASCADE,
    generation_id INTEGER NOT NULL,
    priority INTEGER NOT NULL,
    sequence INTEGER NOT NULL,
    operation_type TEXT NOT NULL,
    target_kind TEXT NOT NULL,
    target_key TEXT NOT NULL,
    container_key TEXT,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'completed', 'canceled', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    completed_at TEXT,
    UNIQUE (guild_key, generation_id, operation_type, target_key, sequence)
);

CREATE TABLE IF NOT EXISTS sync_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
    occurred_at TEXT NOT NULL,
    guild_key TEXT,
    entity_type TEXT NOT NULL,
    entity_key TEXT,
    event_type TEXT NOT NULL,
    details_json TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_categories_guild_status
    ON categories(guild_key, status, name);
CREATE INDEX IF NOT EXISTS idx_channels_guild_status
    ON channels(guild_key, status, name);
CREATE INDEX IF NOT EXISTS idx_threads_guild_status
    ON threads(guild_key, status, day_date);
CREATE INDEX IF NOT EXISTS idx_messages_guild_status
    ON messages(guild_key, status, ordinal);
CREATE INDEX IF NOT EXISTS idx_messages_guild_role
    ON messages(guild_key, role, ordinal);
CREATE INDEX IF NOT EXISTS idx_thread_projection_state
    ON thread_projection_state(guild_key, publication_state);
CREATE INDEX IF NOT EXISTS idx_published_slots_container
    ON published_slots(guild_key, container_key, slot_index);
CREATE INDEX IF NOT EXISTS idx_published_slots_desired_message
    ON published_slots(desired_message_key);
CREATE INDEX IF NOT EXISTS idx_publication_queue_ready
    ON publication_queue(guild_key, state, priority, sequence);
CREATE INDEX IF NOT EXISTS idx_publication_queue_generation
    ON publication_queue(guild_key, generation_id, state);
"""


class EngineDatabase:
    def __init__(self, path: Path):
        self.path = path

    def connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.path)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("PRAGMA journal_mode = WAL")
        return connection

    def initialize(self) -> None:
        with closing(self.connect()) as connection, connection:
            connection.executescript(SCHEMA)
            self._ensure_columns(connection)
            now = utc_now()
            connection.execute(
                """
                INSERT INTO schema_migrations(version, name, applied_at)
                VALUES(?, 'initial_discord_topology', ?)
                ON CONFLICT(version) DO NOTHING
                """,
                (1, now),
            )
            connection.execute(
                """
                INSERT INTO schema_migrations(version, name, applied_at)
                VALUES(?, 'desired_projection_controls', ?)
                ON CONFLICT(version) DO NOTHING
                """,
                (2, now),
            )
            connection.execute(
                """
                INSERT INTO schema_migrations(version, name, applied_at)
                VALUES(?, 'mapping_channel_streams', ?)
                ON CONFLICT(version) DO NOTHING
                """,
                (3, now),
            )
            connection.execute(
                """
                INSERT INTO schema_migrations(version, name, applied_at)
                VALUES(?, 'publication_reconciliation_queue', ?)
                ON CONFLICT(version) DO NOTHING
                """,
                (4, now),
            )
            connection.execute(
                """
                INSERT INTO published_slots(
                    slot_key, guild_key, container_key, slot_index, discord_message_id,
                    desired_message_key, published_content_hash, published_ordinal,
                    status, created_at, updated_at
                )
                SELECT
                    message_key || ':slot',
                    guild_key,
                    thread_key,
                    COALESCE(published_ordinal, ordinal),
                    discord_message_id,
                    message_key,
                    COALESCE(published_content_hash, desired_content_hash),
                    COALESCE(published_ordinal, ordinal),
                    'active',
                    ?,
                    ?
                FROM messages
                WHERE discord_message_id IS NOT NULL
                ON CONFLICT DO NOTHING
                """,
                (now, now),
            )
            connection.execute(
                """
                INSERT INTO controls(target, paused, reason, updated_at)
                VALUES('publication', 1, 'Publication intentionally paused until Discord live sync is approved.', ?)
                ON CONFLICT(target) DO NOTHING
                """,
                (now,),
            )

    def _ensure_columns(self, connection: sqlite3.Connection) -> None:
        table_additions = {
            "categories": {
                "published_name": "ALTER TABLE categories ADD COLUMN published_name TEXT",
                "desired_generation_id": "ALTER TABLE categories ADD COLUMN desired_generation_id INTEGER",
            },
            "channels": {
                "published_name": "ALTER TABLE channels ADD COLUMN published_name TEXT",
                "desired_generation_id": "ALTER TABLE channels ADD COLUMN desired_generation_id INTEGER",
            },
            "threads": {
                "container_kind": "ALTER TABLE threads ADD COLUMN container_kind TEXT NOT NULL DEFAULT 'thread'",
                "published_name": "ALTER TABLE threads ADD COLUMN published_name TEXT",
                "desired_generation_id": "ALTER TABLE threads ADD COLUMN desired_generation_id INTEGER",
            },
            "messages": {
                "desired_content_hash": "ALTER TABLE messages ADD COLUMN desired_content_hash TEXT",
                "desired_generation_id": "ALTER TABLE messages ADD COLUMN desired_generation_id INTEGER",
                "published_content_hash": "ALTER TABLE messages ADD COLUMN published_content_hash TEXT",
                "published_ordinal": "ALTER TABLE messages ADD COLUMN published_ordinal INTEGER",
            },
        }
        for table, additions in table_additions.items():
            columns = {
                row["name"]
                for row in connection.execute(f"PRAGMA table_info({table})")
            }
            for column, statement in additions.items():
                if column not in columns:
                    connection.execute(statement)

    def verify(self) -> dict[str, object]:
        with closing(self.connect()) as connection, connection:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            foreign_keys = [dict(row) for row in connection.execute("PRAGMA foreign_key_check")]
            migrations = [
                dict(row)
                for row in connection.execute(
                    "SELECT version, name, applied_at FROM schema_migrations ORDER BY version"
                )
            ]
            return {
                "path": str(self.path),
                "integrity_check": integrity,
                "foreign_key_violations": foreign_keys,
                "migrations": migrations,
            }
