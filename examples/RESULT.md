# Recorded first use

This output was produced by the included example with network connections disabled. Synthetic provider or worker replies are identified by the example; no real model quality or billing is implied.

From the installed checkout:

```sh
python -m examples.offline_demo
```

[Complete recorded output](result.json)

```text
{
  "mode": "Synthetic CE chunks; actual archive projection",
  "source_messages": [
    "First, review the fixture.",
    "Then, record the result."
  ],
  "projection": {
    "guild_key": "demo",
    "source_system": "synthetic-ce",
    "start": "2026-01-01",
    "end": "2026-01-02",
    "chunks_projected": 2,
    "days_with_content": 2,
    "placeholders_removed": 0,
    "categories": 1,
    "channels": 1,
    "threads": 2,
    "messages": 4,
    "projection_states": {
      "unpublished": 2
    }
  },
  "publication_paused": true,
  "remaining_actions": {
    "categories": 1,
    "channels": 1,
    "threads": 2,
    "messages": 4
  },
  "sample": [
    {
      "day_date": "2026-01-01",
      "category_name": "2026",
      "channel_name": "2026-01",
      "thread_name": "2026-01-01",
      "intended_messages": 2,
      "created_messages": 0
    },
    {
      "day_date": "2026-01-02",
      "category_name": "2026",
      "channel_name": "2026-01",
      "thread_name": "2026-01-02",
      "intended_messages": 2,
      "created_messages": 0
    }
  ],
  "discord_calls": 0
}
```

Generated timestamps and synthetic identifiers can change between runs. The demonstrated behavior and input fixture remain inspectable in the adjacent example files.
