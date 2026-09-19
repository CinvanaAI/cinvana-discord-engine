# Security and publication boundary

This engine can hold bot tokens, guild IDs, a complete desired and published
Discord topology, Conversation Engine pointers, queued mutations, message
content, and operational reports. All live runtime material is private.

- Keep tokens and guild IDs only in the ignored runtime configuration.
- Publication begins paused; review `dry-run` output before unpausing it.
- The write governor and Discord `Retry-After` handling are safety controls, not
  permission to run against an unreviewed server.
- Treat Conversation Engine content and Discord API responses as untrusted.
- Tests use fakes and disposable databases and make no network calls.

If a bot token is exposed, rotate it before cleaning repository history.

