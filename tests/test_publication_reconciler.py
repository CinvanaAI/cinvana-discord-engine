from __future__ import annotations

from discord_engine.config import EngineConfig
from discord_engine.planner import parse_date, plan_calendar
from discord_engine.reconciler import plan_publication_queue

from test_calendar_planner import make_repo
from test_ce_projection import sha256_text


def publish_single_day(repo):
    plan_calendar(
        repo,
        guild_key="private_chronological",
        start=parse_date("2025-05-01"),
        end=parse_date("2025-05-01"),
        example_count=0,
    )
    repo.mark_created(
        table="categories",
        key_column="category_key",
        key="private_chronological:category:year:2025",
        discord_column="discord_category_id",
        discord_id="10",
    )
    repo.mark_created(
        table="channels",
        key_column="channel_key",
        key="private_chronological:channel:month:2025-05",
        discord_column="discord_channel_id",
        discord_id="20",
    )
    repo.mark_message_created(
        message_key="private_chronological:message:day:2025-05-01:starter",
        discord_message_id="30",
        desired_content_hash=sha256_text("2025-05-01"),
        desired_ordinal=0,
    )
    repo.mark_created(
        table="threads",
        key_column="thread_key",
        key="private_chronological:thread:day:2025-05-01",
        discord_column="discord_thread_id",
        discord_id="40",
    )


def upsert_content(repo, key: str, ordinal: int, content: str):
    repo.upsert_message(
        message_key=f"private_chronological:message:{key}",
        guild_key="private_chronological",
        thread_key="private_chronological:thread:day:2025-05-01",
        logical_key=key,
        ordinal=ordinal,
        role="content",
        content_kind="literal",
        literal_text=content,
    )


def seed_published_content(repo):
    for index, content in enumerate(("one", "two", "three"), start=1):
        key = f"content:{index}"
        upsert_content(repo, key, index, content)
        repo.mark_message_created(
            message_key=f"private_chronological:message:{key}",
            discord_message_id=f"10{index}",
            desired_content_hash=sha256_text(content),
            desired_ordinal=index,
        )


def queued_types(connection):
    return [
        row["operation_type"]
        for row in connection.execute(
            """
            SELECT operation_type
            FROM publication_queue
            WHERE state = 'pending'
            ORDER BY priority, sequence
            """
        )
    ]


def test_unchanged_projection_relinks_slots_without_discord_writes(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        publish_single_day(repo)
        seed_published_content(repo)
        connection.execute("DELETE FROM messages WHERE role = 'content'")
        for index, content in enumerate(("one", "two", "three"), start=1):
            upsert_content(repo, f"content:{index}", index, content)
        report = plan_publication_queue(repo, guild_key="private_chronological")
        connection.commit()

        assert report.relinked_slots == 3
        assert report.enqueued_operations == 0
        assert queued_types(connection) == []
    finally:
        connection.close()


def test_insertions_plan_edit_shift_then_one_create(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        publish_single_day(repo)
        seed_published_content(repo)
        connection.execute("DELETE FROM messages WHERE role = 'content'")
        for index, (key, content) in enumerate(
            (
                ("content:new", "new"),
                ("content:1", "one"),
                ("content:2", "two"),
                ("content:3", "three"),
            ),
            start=1,
        ):
            upsert_content(repo, key, index, content)
        report = plan_publication_queue(repo, guild_key="private_chronological")
        connection.commit()

        assert report.operations_by_type == {
            "EDIT_MESSAGE": 3,
            "CREATE_MESSAGE": 1,
        }
        assert queued_types(connection) == [
            "EDIT_MESSAGE",
            "EDIT_MESSAGE",
            "EDIT_MESSAGE",
            "CREATE_MESSAGE",
        ]
    finally:
        connection.close()


def test_new_generation_cancels_stale_queue_items(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        old_run = repo.begin_projection_run(
            guild_key="private_chronological",
            source_system="test",
            stats={"status": "old"},
        )
        repo.complete_projection_run(old_run, {"status": "old"})
        repo.enqueue_operation(
            operation_id="old_op",
            guild_key="private_chronological",
            generation_id=old_run,
            priority=50,
            sequence=1,
            operation_type="CREATE_MESSAGE",
            target_kind="message",
            target_key="old",
            container_key=None,
            payload={"message_key": "old"},
        )
        new_run = repo.begin_projection_run(
            guild_key="private_chronological",
            source_system="test",
            stats={"status": "new"},
        )
        repo.complete_projection_run(new_run, {"status": "new"})

        report = plan_publication_queue(repo, guild_key="private_chronological")
        state = connection.execute(
            "SELECT state FROM publication_queue WHERE operation_id = 'old_op'"
        ).fetchone()["state"]

        assert report.generation_id == new_run
        assert report.canceled_stale_operations == 1
        assert state == "canceled"
    finally:
        connection.close()


def test_repeated_generation_reuses_unchanged_pending_queue(tmp_path):
    _, _, connection, repo = make_repo(tmp_path)
    try:
        first_run = repo.begin_projection_run(
            guild_key="private_chronological",
            source_system="test",
            stats={"status": "first"},
        )
        repo.complete_projection_run(first_run, {"status": "first"})
        plan_calendar(
            repo,
            guild_key="private_chronological",
            start=parse_date("2025-05-01"),
            end=parse_date("2025-05-01"),
            example_count=0,
            desired_generation_id=first_run,
        )
        first_report = plan_publication_queue(
            repo,
            guild_key="private_chronological",
        )

        second_run = repo.begin_projection_run(
            guild_key="private_chronological",
            source_system="test",
            stats={"status": "second"},
        )
        repo.complete_projection_run(second_run, {"status": "second"})
        plan_calendar(
            repo,
            guild_key="private_chronological",
            start=parse_date("2025-05-01"),
            end=parse_date("2025-05-01"),
            example_count=0,
            desired_generation_id=second_run,
        )
        second_report = plan_publication_queue(
            repo,
            guild_key="private_chronological",
        )

        states = {
            row["state"]: row["count"]
            for row in connection.execute(
                """
                SELECT state, COUNT(*) AS count
                FROM publication_queue
                GROUP BY state
                """
            )
        }
        generations = [
            row["generation_id"]
            for row in connection.execute(
                "SELECT DISTINCT generation_id FROM publication_queue"
            )
        ]

        assert first_report.enqueued_operations == 4
        assert second_report.enqueued_operations == 0
        assert second_report.canceled_stale_operations == 0
        assert states == {"pending": 4}
        assert generations == [second_run]
    finally:
        connection.close()


def test_reconcile_interval_defaults_to_ten_minutes(tmp_path, monkeypatch):
    monkeypatch.delenv("DISCORD_RECONCILE_INTERVAL_SECONDS", raising=False)
    config = EngineConfig.load(tmp_path / "Discord Bot")
    assert config.reconcile_interval_seconds == 600
