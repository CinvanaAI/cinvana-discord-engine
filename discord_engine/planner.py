from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

from .repository import Repository


@dataclass(frozen=True)
class CalendarPlan:
    guild_key: str
    start: date
    end: date
    categories: int
    channels: int
    threads: int
    messages: int


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def iter_days(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def plan_calendar(
    repository: Repository,
    *,
    guild_key: str,
    start: date,
    end: date,
    example_count: int = 3,
    desired_generation_id: int | None = None,
) -> CalendarPlan:
    if end < start:
        raise ValueError("end date must be on or after start date")
    if example_count < 0:
        raise ValueError("example count must not be negative")

    years: set[str] = set()
    months: set[str] = set()
    thread_count = 0
    message_count = 0

    for day in iter_days(start, end):
        year = f"{day.year:04d}"
        month = f"{day.year:04d}-{day.month:02d}"
        day_text = day.isoformat()

        category_key = f"{guild_key}:category:year:{year}"
        channel_key = f"{guild_key}:channel:month:{month}"
        thread_key = f"{guild_key}:thread:day:{day_text}"

        repository.upsert_category(
            category_key=category_key,
            guild_key=guild_key,
            logical_key=f"year:{year}",
            name=year,
            desired_generation_id=desired_generation_id,
        )
        repository.upsert_channel(
            channel_key=channel_key,
            guild_key=guild_key,
            category_key=category_key,
            logical_key=f"month:{month}",
            name=month,
            desired_generation_id=desired_generation_id,
        )
        repository.upsert_thread(
            thread_key=thread_key,
            guild_key=guild_key,
            channel_key=channel_key,
            logical_key=f"day:{day_text}",
            name=day_text,
            day_date=day_text,
            desired_generation_id=desired_generation_id,
        )
        repository.upsert_message(
            message_key=f"{guild_key}:message:day:{day_text}:starter",
            guild_key=guild_key,
            thread_key=thread_key,
            logical_key=f"day:{day_text}:starter",
            ordinal=0,
            role="thread_starter",
            content_kind="literal",
            literal_text=day_text,
            desired_generation_id=desired_generation_id,
        )
        message_count += 1
        for index in range(1, example_count + 1):
            repository.upsert_message(
                message_key=f"{guild_key}:message:day:{day_text}:example:{index}",
                guild_key=guild_key,
                thread_key=thread_key,
                logical_key=f"day:{day_text}:example:{index}",
                ordinal=index,
                role="example",
                content_kind="literal",
                literal_text=f"example {index}",
                desired_generation_id=desired_generation_id,
            )
            message_count += 1

        years.add(year)
        months.add(month)
        thread_count += 1

    return CalendarPlan(
        guild_key=guild_key,
        start=start,
        end=end,
        categories=len(years),
        channels=len(months),
        threads=thread_count,
        messages=message_count,
    )
