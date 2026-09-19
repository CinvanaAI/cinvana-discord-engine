# Conversation Archive for Discord

Turn a local conversation collection into an ordered Discord archive and preview the exact changes before sending them.

## Try it

Python 3.12+. Run from this checkout:

```sh
python -m pip install -e .
python -m examples.offline_demo
```

**Input:** Two synthetic Conversation Engine chunks on consecutive days.

**Result:** A year/category, month/channel and two dated threads are planned with real source bindings. Publication remains paused and Discord receives zero calls.

See [the captured example](examples/RESULT.md) for the observed output and reproduction command.

## How it works

Projection builds desired state from source pointers. Publication resolves that state into ordered Discord slots and cancels obsolete queued work when a newer generation arrives.

Source: [discord_engine/reconciler.py](discord_engine/reconciler.py), [discord_engine/cli.py](discord_engine/cli.py), [tests/test_ce_projection.py](tests/test_ce_projection.py).

## Use it for your work

Start with `cinvana-discord-engine --root YOUR_ARCHIVE init` and `dry-run`. Install `.[discord]` for live integration. Read [configuration](discord_engine/config.py), [source reads](discord_engine/ce_client.py) and [publication controls](discord_engine/cli.py) before explicitly unpausing.

## Scope

The offline example verifies projection and planning. Live archive reads require a Conversation Engine installation and its configured root; publication additionally requires a visitor-owned bot, guild IDs and suitable Discord permissions.

Owned code is available under the [MIT license](LICENSE.md).
