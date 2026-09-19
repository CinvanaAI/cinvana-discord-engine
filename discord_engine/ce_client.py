from __future__ import annotations

import sys
import json
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterator


@dataclass(frozen=True)
class CEChunk:
    message_id: str
    result_id: str
    audience: str
    chunk_index: int
    start: int
    end: int
    max_chars: int
    source: dict[str, Any]
    content: str
    content_sha256: str
    chat_id: str
    position: int
    speaker: str
    message_timestamp: str
    day: date


@dataclass(frozen=True)
class CEMappedChunk(CEChunk):
    mapping_title: str
    mapping_result_id: str


def _parse_day(value: str) -> date:
    normalized = value.replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).date()


class ConversationEngineClient:
    def __init__(self, ce_root: str | Path):
        self.ce_root = Path(ce_root).expanduser().resolve()

    def _ensure_import_path(self) -> None:
        root_text = str(self.ce_root)
        if root_text not in sys.path:
            sys.path.insert(0, root_text)

    @contextmanager
    def repository(self):
        self._ensure_import_path()
        from conversation_engine.config import EnginePaths
        from conversation_engine.db import Database
        from conversation_engine.repository import Repository

        database = Database(EnginePaths.from_root(self.ce_root))
        connection = database.connect(read_only=True)
        try:
            yield Repository(connection)
        finally:
            connection.close()

    def private_discord_chunks(
        self,
        *,
        start: date,
        end: date,
    ) -> list[CEChunk]:
        self._ensure_import_path()
        from conversation_engine.jsonutil import sha256_text
        from conversation_engine.resolution import resolve_reference_text

        chunks: list[CEChunk] = []
        source_cache: dict[tuple[str, str, str | None], str] = {}
        with self.repository() as repository:
            rows = repository.connection.execute(
                """
                SELECT heads.message_id, results.result_id, results.output_json,
                       revisions.chat_id, revisions.position, revisions.speaker,
                       revisions.message_timestamp
                FROM stage_heads heads
                JOIN stage_results results
                  ON results.result_id = heads.result_id
                JOIN current_message_revisions revisions
                  ON revisions.message_id = heads.message_id
                WHERE heads.stage = 'discord_original'
                ORDER BY revisions.message_timestamp,
                         revisions.chat_id,
                         revisions.position
                """
            ).fetchall()
            for row in rows:
                day = _parse_day(row["message_timestamp"])
                if day < start or day > end:
                    continue
                for item in self._expand_discord_output(
                    repository,
                    json.loads(row["output_json"]),
                    source_cache=source_cache,
                    resolve_reference_text=resolve_reference_text,
                ):
                    chunks.append(
                        CEChunk(
                            message_id=row["message_id"],
                            result_id=row["result_id"],
                            audience="private",
                            chunk_index=item["chunk_index"],
                            start=item["start"],
                            end=item["end"],
                            max_chars=item["max_chars"],
                            source=item["source"],
                            content=item["content"],
                            content_sha256=sha256_text(item["content"]),
                            chat_id=row["chat_id"],
                            position=int(row["position"]),
                            speaker=row["speaker"],
                            message_timestamp=row["message_timestamp"],
                            day=day,
                        )
                    )
        chunks.sort(
            key=lambda item: (
                item.day,
                item.message_timestamp,
                item.chat_id,
                item.position,
                item.chunk_index,
            )
        )
        return chunks

    def mapping_index(self) -> dict[str, Any]:
        with self.repository() as repository:
            definition = repository.current_definition("mapping_index", "discord_public")
            if definition is None:
                raise RuntimeError("CE has no current discord_public Mapping Index")
            payload = dict(definition["payload"])
            payload["_definition_id"] = definition["definition_id"]
            payload["_definition_version"] = definition["version"]
            payload["_payload_sha256"] = definition["payload_sha256"]
            return payload

    def public_mapped_discord_chunks(
        self,
        *,
        start: date | None = None,
        end: date | None = None,
    ) -> list[CEMappedChunk]:
        self._ensure_import_path()
        from conversation_engine.jsonutil import sha256_text
        from conversation_engine.resolution import resolve_reference_text

        chunks: list[CEMappedChunk] = []
        source_cache: dict[tuple[str, str, str | None], str] = {}
        with self.repository() as repository:
            rows = repository.connection.execute(
                """
                SELECT heads.message_id, results.result_id, results.output_json,
                       revisions.chat_id, revisions.position, revisions.speaker,
                       revisions.message_timestamp,
                       mappings.title AS mapping_title,
                       mappings.result_id AS mapping_result_id
                FROM stage_heads heads
                JOIN stage_results results
                  ON results.result_id = heads.result_id
                JOIN current_message_revisions revisions
                  ON revisions.message_id = heads.message_id
                JOIN current_message_mappings mappings
                  ON mappings.message_id = heads.message_id
                WHERE heads.stage = 'discord_public'
                ORDER BY mappings.title,
                         revisions.message_timestamp,
                         revisions.chat_id,
                         revisions.position
                """
            ).fetchall()
            for row in rows:
                day = _parse_day(row["message_timestamp"])
                if start is not None and day < start:
                    continue
                if end is not None and day > end:
                    continue
                for item in self._expand_discord_output(
                    repository,
                    json.loads(row["output_json"]),
                    source_cache=source_cache,
                    resolve_reference_text=resolve_reference_text,
                ):
                    chunks.append(
                        CEMappedChunk(
                            message_id=row["message_id"],
                            result_id=row["result_id"],
                            audience="public",
                            chunk_index=item["chunk_index"],
                            start=item["start"],
                            end=item["end"],
                            max_chars=item["max_chars"],
                            source=item["source"],
                            content=item["content"],
                            content_sha256=sha256_text(item["content"]),
                            chat_id=row["chat_id"],
                            position=int(row["position"]),
                            speaker=row["speaker"],
                            message_timestamp=row["message_timestamp"],
                            day=day,
                            mapping_title=row["mapping_title"],
                            mapping_result_id=row["mapping_result_id"],
                        )
                    )
        chunks.sort(
            key=lambda item: (
                item.mapping_title,
                item.message_timestamp,
                item.chat_id,
                item.position,
                item.chunk_index,
            )
        )
        return chunks

    def _expand_discord_output(
        self,
        repository: Any,
        output: dict[str, Any],
        *,
        source_cache: dict[tuple[str, str, str | None], str],
        resolve_reference_text: Any,
        seen: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        kind = output.get("kind")
        if kind == "result_pointer":
            source_result_id = output.get("source_result_id")
            if not isinstance(source_result_id, str):
                raise RuntimeError("Discord result pointer has no source_result_id")
            chain = seen if seen is not None else set()
            if source_result_id in chain:
                raise RuntimeError(f"Discord result pointer cycle at {source_result_id}")
            chain.add(source_result_id)
            source_result = repository.result(source_result_id)
            try:
                return self._expand_discord_output(
                    repository,
                    source_result["output"],
                    source_cache=source_cache,
                    resolve_reference_text=resolve_reference_text,
                    seen=chain,
                )
            finally:
                chain.remove(source_result_id)

        source = output.get("source")
        if not isinstance(source, dict):
            raise RuntimeError(f"Discord result has no source reference: {kind}")
        max_chars = int(output.get("max_chars") or 0)
        source_text = self._resolve_cached_source_text(
            repository,
            source,
            source_cache=source_cache,
            resolve_reference_text=resolve_reference_text,
        )

        if kind == "source_pointer":
            return [
                {
                    "source": source,
                    "chunk_index": 1,
                    "start": 0,
                    "end": len(source_text),
                    "max_chars": max_chars,
                    "content": source_text,
                }
            ]

        if kind != "ranges":
            raise RuntimeError(f"Unsupported Discord result kind: {kind}")
        chunks = output.get("chunks")
        if not isinstance(chunks, list):
            raise RuntimeError("Discord ranges result has no chunks array")
        expanded = []
        for chunk in chunks:
            start = int(chunk["start"])
            end = int(chunk["end"])
            expanded.append(
                {
                    "source": source,
                    "chunk_index": int(chunk["index"]),
                    "start": start,
                    "end": end,
                    "max_chars": max_chars,
                    "content": source_text[start:end],
                }
            )
        return expanded

    def _resolve_cached_source_text(
        self,
        repository: Any,
        source: dict[str, Any],
        *,
        source_cache: dict[tuple[str, str, str | None], str],
        resolve_reference_text: Any,
    ) -> str:
        cache_key = (
            str(source.get("kind")),
            str(source.get("id")),
            source.get("sha256"),
        )
        source_text = source_cache.get(cache_key)
        if source_text is None:
            source_text = resolve_reference_text(repository, source)
            source_cache[cache_key] = source_text
        return source_text

    def resolve_payload(self, payload: dict[str, Any]) -> str:
        self._ensure_import_path()
        from conversation_engine.resolution import resolve_reference_text

        source = payload["source"]
        span = payload["range"]
        with self.repository() as repository:
            source_text = resolve_reference_text(repository, source)
        return source_text[int(span["start"]) : int(span["end"])]
