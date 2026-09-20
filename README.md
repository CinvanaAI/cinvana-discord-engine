# Conversation Archive for Discord

Turn a local conversation collection into an ordered Discord archive and preview the exact changes before sending them.

## Try it

Python 3.12+. Run from this checkout:

```sh
python -m pip install -e .
python -m examples.offline_demo
```

**Input:** Two synthetic Conversation Engine chunks on consecutive days.

**Result:** A year/category, month/channel and two dated threads are planned with real source bindings, then converted into a persistent publication queue. Publication remains paused and Discord receives zero calls.

See [the captured example](examples/RESULT.md) for the observed output and reproduction command.

## How it works

Projection builds desired state from source pointers. Publication resolves that state into ordered Discord slots and cancels obsolete queued work when a newer generation arrives.

Source: [discord_engine/reconciler.py](discord_engine/reconciler.py), [discord_engine/cli.py](discord_engine/cli.py), [tests/test_ce_projection.py](tests/test_ce_projection.py).

## Use it for your work

Start with `cinvana-discord-engine --root YOUR_ARCHIVE init` and `dry-run`. Install `.[discord]` for live integration. Read [configuration](discord_engine/config.py), [source reads](discord_engine/ce_client.py) and [publication controls](discord_engine/cli.py) before explicitly unpausing.

## Scope

The offline example verifies projection and planning. Live archive reads require a Conversation Engine installation and its configured root; publication additionally requires a visitor-owned bot, guild IDs and suitable Discord permissions.

Owned code is available under the [MIT license](LICENSE.md).

## From source chunks to a queue

The expanded [synthetic output](examples/result.json) exposes the actual queued operations, their priorities and payloads. Two dated chunks create one year category, one month channel and two day threads. Each thread also needs its starter message; that is why the projected message count is four while there are only two source chunks. The queue first plans `CREATE_THREAD_STARTER`, then `CREATE_THREAD`, followed by separate `CREATE_MESSAGE` operations for the content slots.

[Projection](discord_engine/projection.py) records desired topology and source bindings. [Reconciliation](discord_engine/reconciler.py) compares desired containers with known physical slots, relinks matching content locally, plans edits/creates/deletes, and cancels obsolete queued generations. [The repository](discord_engine/repository.py) persists both the desired state and queue. Replanning before execution can reuse the same pending operation; queue existence is not delivery evidence.

The demo stops with publication paused. It does not instantiate an API client. For an owned archive, `init`, explicit guild configuration and a bounded calendar/projection plan precede `dry-run` and `plan-publication`. Inspect the chosen guild and planned removals as well as additions. `sync-calendar`, reconciliation and daemon operations can perform real writes when configured and unpaused; their service behavior is not validated by this offline fixture.

## Present scope and family

This [source snapshot](ORIGIN.md) supports an inspectable projection/planning workflow. The expanded queue example is a public continuation, with no recovered production-run claim. Live integration still needs a compatible [Conversation Engine](https://github.com/CinvanaAI/cinvana-conversation-engine), access to its configured source root, a visitor-owned bot and appropriate Discord permissions. Source hashes bind content versions; calendar counts alone cannot establish that an archive is complete.

The independent [Discord Topology Reconciler](https://github.com/CinvanaAI/discord-topology-reconciler) exposes the positional planning idea through small in-memory types. The parent's SQLite/source and publication machinery remains here. Live recipient testing, permissions and restart/rate-limit exercises would be the next evidence for a deployment; none was performed for this edition.
