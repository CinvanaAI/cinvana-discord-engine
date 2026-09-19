from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path


DEFAULT_GUILD_KEY = "private_chronological"
DEFAULT_BOT_HEAD = "private_chronological_manager"
DEFAULT_PUBLIC_MAPPING_GUILD_KEY = "public_mapping"
DEFAULT_PUBLIC_MAPPING_BOT_HEAD = "public_mapping_manager"


ENV_TEMPLATE = """# CinvanaAI Discord Engine local config.
# Fill these values locally. Do not paste tokens into chat.

DISCORD_PRIVATE_CHRONO_BOT_TOKEN=
DISCORD_PRIVATE_CHRONO_GUILD_ID=

DISCORD_PUBLIC_MAPPING_BOT_TOKEN=
DISCORD_PUBLIC_MAPPING_GUILD_ID=

DISCORD_PRIVATE_CHRONO_GUILD_KEY=private_chronological
DISCORD_PRIVATE_CHRONO_GUILD_NAME=CinvanaAI Private Chronological Archive
DISCORD_PRIVATE_CHRONO_BOT_HEAD=private_chronological_manager
DISCORD_PRIVATE_CHRONO_BOT_NAME=Private Chronological Manager

DISCORD_PUBLIC_MAPPING_GUILD_KEY=public_mapping
DISCORD_PUBLIC_MAPPING_GUILD_NAME=CinvanaAI Public Mapping Archive
DISCORD_PUBLIC_MAPPING_BOT_HEAD=public_mapping_manager
DISCORD_PUBLIC_MAPPING_BOT_NAME=Public Mapping Manager

DISCORD_SYNC_MAX_ACTIONS=25
DISCORD_SYNC_MIN_SECONDS_BETWEEN_WRITES=1.0
DISCORD_RECONCILE_INTERVAL_SECONDS=600
CINVANA_CE_ROOT=
"""


def parse_env_file(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@dataclass(frozen=True)
class EngineConfig:
    root: Path
    runtime: Path
    config_dir: Path
    env_file: Path
    state_db: Path
    reports_dir: Path
    values: dict[str, str]

    @classmethod
    def load(cls, root: str | Path | None = None) -> "EngineConfig":
        resolved_root = Path(root or Path.cwd()).expanduser().resolve()
        runtime = resolved_root / "Runtime"
        config_dir = runtime / "Config"
        env_file = config_dir / "discord_engine.env"
        values = parse_env_file(env_file)
        for key, value in os.environ.items():
            if key.startswith("DISCORD_") or key == "CINVANA_CE_ROOT":
                values[key] = value
        return cls(
            root=resolved_root,
            runtime=runtime,
            config_dir=config_dir,
            env_file=env_file,
            state_db=runtime / "State" / "discord_engine.sqlite3",
            reports_dir=runtime / "Reports",
            values=values,
        )

    def create_runtime(self) -> None:
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.state_db.parent.mkdir(parents=True, exist_ok=True)
        self.reports_dir.mkdir(parents=True, exist_ok=True)
        if not self.env_file.exists():
            self.env_file.write_text(ENV_TEMPLATE, encoding="utf-8", newline="\n")
        example = self.root / "config" / "discord_engine.env.example"
        if example.is_file():
            target = self.config_dir / "discord_engine.env.example"
            if not target.exists():
                shutil.copy2(example, target)

    def value(self, key: str, default: str | None = None) -> str | None:
        value = self.values.get(key)
        if value is None or value == "":
            return default
        return value

    def required(self, key: str) -> str:
        value = self.value(key)
        if value is None:
            raise RuntimeError(f"{key} is required")
        return value

    @property
    def default_guild_key(self) -> str:
        return self.value("DISCORD_PRIVATE_CHRONO_GUILD_KEY", DEFAULT_GUILD_KEY) or DEFAULT_GUILD_KEY

    @property
    def default_bot_head(self) -> str:
        return self.value("DISCORD_PRIVATE_CHRONO_BOT_HEAD", DEFAULT_BOT_HEAD) or DEFAULT_BOT_HEAD

    @property
    def public_mapping_guild_key(self) -> str:
        return (
            self.value("DISCORD_PUBLIC_MAPPING_GUILD_KEY", DEFAULT_PUBLIC_MAPPING_GUILD_KEY)
            or DEFAULT_PUBLIC_MAPPING_GUILD_KEY
        )

    @property
    def public_mapping_bot_head(self) -> str:
        return (
            self.value("DISCORD_PUBLIC_MAPPING_BOT_HEAD", DEFAULT_PUBLIC_MAPPING_BOT_HEAD)
            or DEFAULT_PUBLIC_MAPPING_BOT_HEAD
        )

    @property
    def default_max_actions(self) -> int:
        raw = self.value("DISCORD_SYNC_MAX_ACTIONS", "25") or "25"
        try:
            return max(1, int(raw))
        except ValueError:
            return 25

    @property
    def min_seconds_between_writes(self) -> float:
        raw = self.value("DISCORD_SYNC_MIN_SECONDS_BETWEEN_WRITES", "1.0") or "1.0"
        try:
            return max(0.0, float(raw))
        except ValueError:
            return 1.0

    @property
    def reconcile_interval_seconds(self) -> int:
        raw = self.value("DISCORD_RECONCILE_INTERVAL_SECONDS", "600") or "600"
        try:
            return max(60, int(raw))
        except ValueError:
            return 600

    @property
    def ce_root(self) -> Path:
        configured = self.value("CINVANA_CE_ROOT")
        return Path(configured).expanduser().resolve() if configured else (self.root.parent / "Conversation Engine").resolve()
