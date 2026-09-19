from __future__ import annotations

import hashlib
import asyncio
import json
from datetime import date

from discord_engine.ce_client import CEChunk, CEMappedChunk, ConversationEngineClient
from discord_engine.discord_runtime import sync_calendar
from discord_engine.planner import parse_date, plan_calendar
from discord_engine.projection import project_chunks, project_public_mapping

from test_calendar_planner import make_repo


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def fake_chunk(*, message_id: str, chunk_index: int, content: str, day: date) -> CEChunk:
    return CEChunk(
        message_id=message_id,
        result_id=f"result_{message_id}",
        audience="private",
        chunk_index=chunk_index,
        start=0,
        end=len(content),
        max_chars=2000,
        source={
            "kind": "source_revision",
            "id": f"rev_{message_id}",
            "sha256": sha256_text(content),
        },
        content=content,
        content_sha256=sha256_text(content),
        chat_id="chat_a",
        position=1,
        speaker="Codex",
        message_timestamp=f"{day.isoformat()}T12:00:00-04:00",
        day=day,
    )


def fake_mapped_chunk(
    *,
    message_id: str,
    chunk_index: int,
    content: str,
    day: date,
    title: str,
) -> CEMappedChunk:
    base = fake_chunk(
        message_id=message_id,
        chunk_index=chunk_index,
        content=content,
        day=day,
    )
    return CEMappedChunk(
        **base.__dict__,
        mapping_title=title,
        mapping_result_id=f"mapping_{message_id}",
    )


class FakeCERepository:
    def __init__(self):
        self.results = {
            "result_ranges": {
                "output": {
                    "kind": "ranges",
                    "max_chars": 2000,
                    "source": {
                        "kind": "source_revision",
                        "id": "rev_long",
                        "sha256": sha256_text("abcdefghij"),
                    },
                    "chunks": [
                        {"index": 1, "start": 0, "end": 5},
                        {"index": 2, "start": 5, "end": 10},
                    ],
                }
            }
        }

    def result(self, result_id):
        return self.results[result_id]


def configure_mapping_guild(repo):
    repo.configure_bot_head(
        bot_head_key="public_mapping_manager",
        display_name="Public Mapping Manager",
        token_env="DISCORD_PUBLIC_MAPPING_BOT_TOKEN",
    )
    repo.configure_guild(
        guild_key="public_mapping",
        name="CinvanaAI Public Mapping Archive",
        purpose="Public Mapping Index archive",
        server_model="mapping",
        bot_head_key="public_mapping_manager",
        discord_guild_id=None,
    )


def test_publication_defaults_to_paused(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        control = repo.control("publication")
        assert control is not None
        assert control["paused"] == 1
        assert repo.is_paused("publication") is True
    finally:
        connection.close()


def test_paused_sync_reports_write_governor_without_credentials(tmp_path):
    config, _, connection, _ = make_repo(tmp_path)
    connection.close()

    report = asyncio.run(
        sync_calendar(
            config=config,
            guild_key="private_chronological",
            max_actions=1,
            min_seconds_between_writes=1.25,
        )
    )

    assert report["publication_paused"] is True
    assert report["actions"] == 0
    assert report["min_seconds_between_writes"] == 1.25


def test_ce_client_expands_discord_pointer_outputs():
    client = ConversationEngineClient(".")
    repository = FakeCERepository()
    texts = {
        "rev_short": "short message",
        "rev_long": "abcdefghij",
    }

    def resolve_reference_text(_repository, source):
        return texts[source["id"]]

    source_pointer = {
        "kind": "source_pointer",
        "max_chars": 2000,
        "source": {
            "kind": "source_revision",
            "id": "rev_short",
            "sha256": sha256_text("short message"),
        },
    }
    source_pointer_chunks = client._expand_discord_output(
        repository,
        source_pointer,
        source_cache={},
        resolve_reference_text=resolve_reference_text,
    )
    assert source_pointer_chunks == [
        {
            "source": source_pointer["source"],
            "chunk_index": 1,
            "start": 0,
            "end": len("short message"),
            "max_chars": 2000,
            "content": "short message",
        }
    ]

    result_pointer_chunks = client._expand_discord_output(
        repository,
        {"kind": "result_pointer", "source_result_id": "result_ranges"},
        source_cache={},
        resolve_reference_text=resolve_reference_text,
    )
    assert [item["content"] for item in result_pointer_chunks] == ["abcde", "fghij"]


def test_ce_projection_replaces_examples_with_bound_content(tmp_path):
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

        report = project_chunks(
            repo,
            guild_key="private_chronological",
            source_system="conversation_engine",
            start=parse_date("2025-05-01"),
            end=parse_date("2025-05-01"),
            chunks=[
                fake_chunk(
                    message_id="msg_a",
                    chunk_index=1,
                    content="Projected CE text",
                    day=parse_date("2025-05-01"),
                )
            ],
        )
        connection.commit()

        assert report.placeholders_removed == 3
        rows = connection.execute(
            """
            SELECT ordinal, role, content_kind, literal_text, desired_content_hash
            FROM messages
            ORDER BY ordinal
            """
        ).fetchall()
        assert [(row["ordinal"], row["role"], row["content_kind"]) for row in rows] == [
            (0, "thread_starter", "literal"),
            (1, "content", "ce_pointer"),
        ]
        assert rows[1]["literal_text"] is None
        assert rows[1]["desired_content_hash"] == sha256_text("Projected CE text")

        binding = connection.execute(
            "SELECT source_payload_json FROM content_bindings"
        ).fetchone()
        payload = json.loads(binding["source_payload_json"])
        assert payload["ce_message_id"] == "msg_a"
        assert payload["content_sha256"] == sha256_text("Projected CE text")

        state = connection.execute(
            "SELECT publication_state FROM thread_projection_state"
        ).fetchone()
        assert state["publication_state"] == "unpublished"
    finally:
        connection.close()


def test_public_mapping_projection_targets_channels_and_threads(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        configure_mapping_guild(repo)
        mapping_index = {
            "_definition_id": "def_test",
            "_definition_version": 2,
            "index_version": 2,
            "titles": [
                {"id": "title_cat", "title": "Category"},
                {"id": "title_channel", "title": "Direct Channel"},
                {"id": "title_thread", "title": "Leaf Thread"},
            ],
            "placements": [
                {
                    "id": "placement_cat",
                    "title_id": "title_cat",
                    "parent_id": None,
                    "depth": 0,
                    "folder_name": "Category",
                    "active_rel_path": "Category",
                },
                {
                    "id": "placement_channel",
                    "title_id": "title_channel",
                    "parent_id": "placement_cat",
                    "depth": 1,
                    "folder_name": "Direct Channel",
                    "active_rel_path": "Category/Direct Channel",
                },
                {
                    "id": "placement_thread",
                    "title_id": "title_thread",
                    "parent_id": "placement_channel",
                    "depth": 2,
                    "folder_name": "Leaf Thread",
                    "active_rel_path": "Category/Direct Channel/Leaf Thread",
                },
            ],
        }
        report = project_public_mapping(
            repo,
            guild_key="public_mapping",
            source_system="conversation_engine",
            mapping_index=mapping_index,
            mapped_chunks=[
                fake_mapped_chunk(
                    message_id="msg_channel",
                    chunk_index=1,
                    content="direct channel text",
                    day=parse_date("2026-06-01"),
                    title="Direct Channel",
                ),
                fake_mapped_chunk(
                    message_id="msg_thread",
                    chunk_index=1,
                    content="thread text",
                    day=parse_date("2026-06-01"),
                    title="Leaf Thread",
                ),
            ],
        )
        connection.commit()

        assert report.categories == 1
        assert report.channels == 1
        assert report.threads == 1
        assert report.channel_streams == 1
        assert report.starter_messages == 1
        assert report.mapped_chunks_projected == 2

        containers = connection.execute(
            """
            SELECT logical_key, container_kind
            FROM threads
            WHERE guild_key = 'public_mapping'
            ORDER BY container_kind, logical_key
            """
        ).fetchall()
        assert {row["container_kind"] for row in containers} == {
            "channel_stream",
            "thread",
        }

        rows = connection.execute(
            """
            SELECT messages.role, messages.content_kind, threads.container_kind
            FROM messages
            JOIN threads ON threads.thread_key = messages.thread_key
            WHERE messages.guild_key = 'public_mapping'
            ORDER BY messages.role, messages.message_key
            """
        ).fetchall()
        assert [(row["role"], row["content_kind"], row["container_kind"]) for row in rows] == [
            ("content", "ce_pointer", "channel_stream"),
            ("content", "ce_pointer", "thread"),
            ("thread_starter", "literal", "thread"),
        ]
    finally:
        connection.close()
