from __future__ import annotations

import hashlib
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable

from .ce_client import CEChunk, CEMappedChunk, ConversationEngineClient
from .planner import iter_days, plan_calendar
from .repository import Repository


@dataclass(frozen=True)
class ProjectionReport:
    guild_key: str
    source_system: str
    start: date
    end: date
    chunks_projected: int
    days_with_content: int
    placeholders_removed: int
    categories: int
    channels: int
    threads: int
    messages: int
    projection_states: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "guild_key": self.guild_key,
            "source_system": self.source_system,
            "start": self.start.isoformat(),
            "end": self.end.isoformat(),
            "chunks_projected": self.chunks_projected,
            "days_with_content": self.days_with_content,
            "placeholders_removed": self.placeholders_removed,
            "categories": self.categories,
            "channels": self.channels,
            "threads": self.threads,
            "messages": self.messages,
            "projection_states": self.projection_states,
        }


@dataclass(frozen=True)
class MappingProjectionReport:
    guild_key: str
    source_system: str
    mapping_index_version: int
    mapping_definition_id: str | None
    categories: int
    channels: int
    threads: int
    channel_streams: int
    starter_messages: int
    mapped_chunks_projected: int
    unique_ce_chunks: int
    unique_ce_messages: int
    mapped_targets_with_content: int
    placeholders_removed: int
    projection_states: dict[str, int]

    def to_dict(self) -> dict[str, object]:
        return {
            "guild_key": self.guild_key,
            "source_system": self.source_system,
            "mapping_index_version": self.mapping_index_version,
            "mapping_definition_id": self.mapping_definition_id,
            "categories": self.categories,
            "channels": self.channels,
            "threads": self.threads,
            "channel_streams": self.channel_streams,
            "starter_messages": self.starter_messages,
            "mapped_chunks_projected": self.mapped_chunks_projected,
            "unique_ce_chunks": self.unique_ce_chunks,
            "unique_ce_messages": self.unique_ce_messages,
            "mapped_targets_with_content": self.mapped_targets_with_content,
            "placeholders_removed": self.placeholders_removed,
            "projection_states": self.projection_states,
        }


@dataclass(frozen=True)
class MappingTarget:
    placement_id: str
    target_kind: str
    category_key: str
    channel_key: str
    container_key: str
    title: str
    active_rel_path: str


def _short_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:10]


def _display_name(value: str, *, fallback: str) -> str:
    cleaned = " ".join((value or fallback).split())
    if not cleaned:
        cleaned = fallback
    if len(cleaned) <= 100:
        return cleaned
    suffix = _short_hash(cleaned)
    return f"{cleaned[:89].rstrip()}-{suffix}"


def _channel_name(value: str, *, fallback: str) -> str:
    normalized = unicodedata.normalize("NFKD", value or fallback)
    ascii_text = normalized.encode("ascii", "ignore").decode("ascii").lower()
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_text).strip("-")
    if not slug:
        slug = fallback.lower()
    if len(slug) <= 90:
        return slug
    return f"{slug[:79].rstrip('-')}-{_short_hash(value)}"


def _title_by_id(mapping_index: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {item["id"]: item for item in mapping_index.get("titles", [])}


def _placement_title(placement: dict[str, Any], titles: dict[str, dict[str, Any]]) -> str:
    title_id = placement.get("title_id")
    title = titles.get(title_id, {}).get("title")
    return title or placement.get("folder_name") or placement["id"]


def _build_mapping_topology(
    repository: Repository,
    *,
    guild_key: str,
    mapping_index: dict[str, Any],
    desired_generation_id: int,
) -> tuple[dict[str, MappingTarget], dict[str, int]]:
    titles = _title_by_id(mapping_index)
    placements = {
        item["id"]: item
        for item in mapping_index.get("placements", [])
    }
    title_to_target: dict[str, MappingTarget] = {}
    counts = {
        "categories": 0,
        "channels": 0,
        "threads": 0,
        "channel_streams": 0,
        "starter_messages": 0,
    }
    used_channel_names: dict[str, set[str]] = defaultdict(set)

    for placement in sorted(placements.values(), key=lambda item: (item["depth"], item["id"])):
        if placement["depth"] != 0:
            continue
        title = _placement_title(placement, titles)
        repository.upsert_category(
            category_key=f"{guild_key}:category:mapping:{placement['id']}",
            guild_key=guild_key,
            logical_key=f"mapping:{placement['id']}",
            name=_display_name(placement.get("folder_name") or title, fallback=placement["id"]),
            desired_generation_id=desired_generation_id,
        )
        counts["categories"] += 1

    for placement in sorted(placements.values(), key=lambda item: (item["depth"], item["id"])):
        if placement["depth"] != 1:
            continue
        parent = placements.get(placement.get("parent_id"))
        if parent is None or parent.get("depth") != 0:
            raise ValueError(f"mapping channel has no category parent: {placement['id']}")
        title = _placement_title(placement, titles)
        category_key = f"{guild_key}:category:mapping:{parent['id']}"
        channel_key = f"{guild_key}:channel:mapping:{placement['id']}"
        stream_key = f"{guild_key}:container:mapping:{placement['id']}:channel_stream"
        channel_name = _channel_name(placement.get("folder_name") or title, fallback=placement["id"])
        if channel_name in used_channel_names[category_key]:
            channel_name = _channel_name(
                f"{placement.get('folder_name') or title}-{placement['id']}",
                fallback=placement["id"],
            )
        used_channel_names[category_key].add(channel_name)
        repository.upsert_channel(
            channel_key=channel_key,
            guild_key=guild_key,
            category_key=category_key,
            logical_key=f"mapping:{placement['id']}",
            name=channel_name,
            desired_generation_id=desired_generation_id,
        )
        repository.upsert_thread(
            thread_key=stream_key,
            guild_key=guild_key,
            channel_key=channel_key,
            logical_key=f"mapping:{placement['id']}:channel_stream",
            name=_display_name(placement.get("folder_name") or title, fallback=placement["id"]),
            day_date=f"mapping:{placement['id']}",
            container_kind="channel_stream",
            desired_generation_id=desired_generation_id,
        )
        title_to_target[title] = MappingTarget(
            placement_id=placement["id"],
            target_kind="channel",
            category_key=category_key,
            channel_key=channel_key,
            container_key=stream_key,
            title=title,
            active_rel_path=placement["active_rel_path"],
        )
        counts["channels"] += 1
        counts["channel_streams"] += 1

    for placement in sorted(placements.values(), key=lambda item: (item["depth"], item["id"])):
        if placement["depth"] != 2:
            continue
        channel = placements.get(placement.get("parent_id"))
        if channel is None or channel.get("depth") != 1:
            raise ValueError(f"mapping thread has no channel parent: {placement['id']}")
        category = placements.get(channel.get("parent_id"))
        if category is None or category.get("depth") != 0:
            raise ValueError(f"mapping thread has no category grandparent: {placement['id']}")
        title = _placement_title(placement, titles)
        category_key = f"{guild_key}:category:mapping:{category['id']}"
        channel_key = f"{guild_key}:channel:mapping:{channel['id']}"
        thread_key = f"{guild_key}:thread:mapping:{placement['id']}"
        repository.upsert_thread(
            thread_key=thread_key,
            guild_key=guild_key,
            channel_key=channel_key,
            logical_key=f"mapping:{placement['id']}",
            name=_display_name(placement.get("folder_name") or title, fallback=placement["id"]),
            day_date=f"mapping:{placement['id']}",
            container_kind="thread",
            desired_generation_id=desired_generation_id,
        )
        repository.upsert_message(
            message_key=f"{guild_key}:message:mapping:{placement['id']}:starter",
            guild_key=guild_key,
            thread_key=thread_key,
            logical_key=f"mapping:{placement['id']}:starter",
            ordinal=0,
            role="thread_starter",
            content_kind="literal",
            literal_text=_display_name(placement.get("folder_name") or title, fallback=placement["id"]),
            desired_generation_id=desired_generation_id,
        )
        title_to_target[title] = MappingTarget(
            placement_id=placement["id"],
            target_kind="thread",
            category_key=category_key,
            channel_key=channel_key,
            container_key=thread_key,
            title=title,
            active_rel_path=placement["active_rel_path"],
        )
        counts["threads"] += 1
        counts["starter_messages"] += 1

    return title_to_target, counts


def project_ce_private_calendar(
    repository: Repository,
    *,
    ce_root,
    guild_key: str,
    start: date,
    end: date,
) -> ProjectionReport:
    client = ConversationEngineClient(ce_root)
    chunks = client.private_discord_chunks(start=start, end=end)
    return project_chunks(
        repository,
        guild_key=guild_key,
        source_system="conversation_engine",
        start=start,
        end=end,
        chunks=chunks,
    )


def project_ce_public_mapping(
    repository: Repository,
    *,
    ce_root,
    guild_key: str,
    start: date | None = None,
    end: date | None = None,
) -> MappingProjectionReport:
    client = ConversationEngineClient(ce_root)
    mapping_index = client.mapping_index()
    chunks = client.public_mapped_discord_chunks(start=start, end=end)
    return project_public_mapping(
        repository,
        guild_key=guild_key,
        source_system="conversation_engine",
        mapping_index=mapping_index,
        mapped_chunks=chunks,
    )


def project_public_mapping(
    repository: Repository,
    *,
    guild_key: str,
    source_system: str,
    mapping_index: dict[str, Any],
    mapped_chunks: Iterable[CEMappedChunk],
) -> MappingProjectionReport:
    run_id = repository.begin_projection_run(
        guild_key=guild_key,
        source_system=source_system,
        stats={"status": "running", "projection": "public_mapping"},
    )
    targets, topology_counts = _build_mapping_topology(
        repository,
        guild_key=guild_key,
        mapping_index=mapping_index,
        desired_generation_id=run_id,
    )
    placeholders_removed = repository.reset_unpublished_projected_messages(guild_key)
    chunk_list = list(mapped_chunks)
    missing_titles = sorted({item.mapping_title for item in chunk_list if item.mapping_title not in targets})
    if missing_titles:
        raise ValueError(f"mapping titles have no Mapping Index placement: {missing_titles[:10]}")
    ordered_chunks = sorted(
        chunk_list,
        key=lambda item: (
            targets[item.mapping_title].container_key,
            item.message_timestamp,
            item.chat_id,
            item.position,
            item.chunk_index,
        ),
    )
    ordinal_by_container: dict[str, int] = defaultdict(lambda: 1)
    targets_with_content: set[str] = set()
    unique_ce_chunks: set[tuple[str, int]] = set()
    unique_ce_messages: set[str] = set()
    for chunk in ordered_chunks:
        target = targets.get(chunk.mapping_title)
        if target is None:
            raise ValueError(f"mapping title has no Mapping Index placement: {chunk.mapping_title}")
        ordinal = ordinal_by_container[target.container_key]
        ordinal_by_container[target.container_key] += 1
        targets_with_content.add(target.container_key)
        unique_ce_chunks.add((chunk.message_id, chunk.chunk_index))
        unique_ce_messages.add(chunk.message_id)
        message_key = (
            f"{guild_key}:message:ce:{chunk.message_id}:{chunk.audience}:"
            f"{chunk.chunk_index}:map:{target.placement_id}"
        )
        repository.upsert_message(
            message_key=message_key,
            guild_key=guild_key,
            thread_key=target.container_key,
            logical_key=(
                f"ce:{chunk.message_id}:{chunk.audience}:{chunk.chunk_index}:"
                f"map:{target.placement_id}"
            ),
            ordinal=ordinal,
            role="content",
            content_kind="ce_pointer",
            literal_text=None,
            desired_content_hash=chunk.content_sha256,
            desired_generation_id=run_id,
        )
        repository.upsert_content_binding(
            message_key=message_key,
            source_system=source_system,
            payload={
                "ce_message_id": chunk.message_id,
                "ce_result_id": chunk.result_id,
                "ce_stage": "discord_public",
                "audience": chunk.audience,
                "chunk_index": chunk.chunk_index,
                "source": chunk.source,
                "range": {"start": chunk.start, "end": chunk.end},
                "content_sha256": chunk.content_sha256,
                "message_timestamp": chunk.message_timestamp,
                "chat_id": chunk.chat_id,
                "position": chunk.position,
                "speaker": chunk.speaker,
                "mapping_title": chunk.mapping_title,
                "mapping_result_id": chunk.mapping_result_id,
                "mapping_placement_id": target.placement_id,
                "mapping_active_rel_path": target.active_rel_path,
                "discord_target_kind": target.target_kind,
            },
        )

    projection_states = repository.refresh_all_thread_projection_states(guild_key)
    report = MappingProjectionReport(
        guild_key=guild_key,
        source_system=source_system,
        mapping_index_version=int(
            mapping_index.get("_definition_version")
            or mapping_index.get("index_version")
            or 0
        ),
        mapping_definition_id=mapping_index.get("_definition_id"),
        categories=topology_counts["categories"],
        channels=topology_counts["channels"],
        threads=topology_counts["threads"],
        channel_streams=topology_counts["channel_streams"],
        starter_messages=topology_counts["starter_messages"],
        mapped_chunks_projected=len(ordered_chunks),
        unique_ce_chunks=len(unique_ce_chunks),
        unique_ce_messages=len(unique_ce_messages),
        mapped_targets_with_content=len(targets_with_content),
        placeholders_removed=placeholders_removed,
        projection_states=projection_states,
    )
    repository.complete_projection_run(run_id, report.to_dict())
    repository.event(
        "projection.completed",
        guild_key=guild_key,
        details=report.to_dict(),
    )
    return report


def project_chunks(
    repository: Repository,
    *,
    guild_key: str,
    source_system: str,
    start: date,
    end: date,
    chunks: Iterable[CEChunk],
) -> ProjectionReport:
    run_id = repository.begin_projection_run(
        guild_key=guild_key,
        source_system=source_system,
        stats={
            "status": "running",
            "start": start.isoformat(),
            "end": end.isoformat(),
        },
    )
    calendar = plan_calendar(
        repository,
        guild_key=guild_key,
        start=start,
        end=end,
        example_count=0,
        desired_generation_id=run_id,
    )
    placeholders_removed = repository.reset_unpublished_projected_messages(guild_key)

    ordered_chunks = sorted(
        chunks,
        key=lambda item: (
            item.day,
            item.message_timestamp,
            item.chat_id,
            item.position,
            item.chunk_index,
        ),
    )
    ordinal_by_day: dict[str, int] = defaultdict(lambda: 1)
    days_with_content: set[str] = set()
    for chunk in ordered_chunks:
        day_text = chunk.day.isoformat()
        thread_key = f"{guild_key}:thread:day:{day_text}"
        ordinal = ordinal_by_day[day_text]
        ordinal_by_day[day_text] += 1
        days_with_content.add(day_text)
        message_key = (
            f"{guild_key}:message:ce:{chunk.message_id}:"
            f"{chunk.audience}:{chunk.chunk_index}"
        )
        repository.upsert_message(
            message_key=message_key,
            guild_key=guild_key,
            thread_key=thread_key,
            logical_key=f"ce:{chunk.message_id}:{chunk.audience}:{chunk.chunk_index}",
            ordinal=ordinal,
            role="content",
            content_kind="ce_pointer",
            literal_text=None,
            desired_content_hash=chunk.content_sha256,
            desired_generation_id=run_id,
        )
        repository.upsert_content_binding(
            message_key=message_key,
            source_system=source_system,
            payload={
                "ce_message_id": chunk.message_id,
                "ce_result_id": chunk.result_id,
                "ce_stage": "discord_original",
                "audience": chunk.audience,
                "chunk_index": chunk.chunk_index,
                "source": chunk.source,
                "range": {"start": chunk.start, "end": chunk.end},
                "content_sha256": chunk.content_sha256,
                "message_timestamp": chunk.message_timestamp,
                "chat_id": chunk.chat_id,
                "position": chunk.position,
                "speaker": chunk.speaker,
            },
        )

    for day in iter_days(start, end):
        thread_key = f"{guild_key}:thread:day:{day.isoformat()}"
        repository.refresh_thread_projection_state(
            thread_key=thread_key,
            guild_key=guild_key,
        )
    projection_states = repository.refresh_all_thread_projection_states(guild_key)
    report = ProjectionReport(
        guild_key=guild_key,
        source_system=source_system,
        start=start,
        end=end,
        chunks_projected=len(ordered_chunks),
        days_with_content=len(days_with_content),
        placeholders_removed=placeholders_removed,
        categories=calendar.categories,
        channels=calendar.channels,
        threads=calendar.threads,
        messages=calendar.messages + len(ordered_chunks),
        projection_states=projection_states,
    )
    repository.complete_projection_run(run_id, report.to_dict())
    repository.event(
        "projection.completed",
        guild_key=guild_key,
        details=report.to_dict(),
    )
    return report
