"""Project-level configuration for genie-world."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class GenieWorldConfig:
    warehouse_id: str | None = None
    llm_model: str = "databricks-claude-sonnet-4-6"
    storage_path: str | None = None
    mlflow_experiment_id: str | None = None
    max_workers: int = 4

    @classmethod
    def from_env(cls) -> GenieWorldConfig:
        max_workers_str = os.environ.get("GENIE_WORLD_MAX_WORKERS", "4")
        return cls(
            warehouse_id=os.environ.get("GENIE_WORLD_WAREHOUSE_ID"),
            llm_model=os.environ.get("GENIE_WORLD_LLM_MODEL", "databricks-claude-sonnet-4-6"),
            storage_path=os.environ.get("GENIE_WORLD_STORAGE_PATH"),
            mlflow_experiment_id=os.environ.get("GENIE_WORLD_MLFLOW_EXPERIMENT_ID"),
            max_workers=int(max_workers_str),
        )


_global_config: GenieWorldConfig = GenieWorldConfig()


def get_config() -> GenieWorldConfig:
    return _global_config


def set_config(config: GenieWorldConfig) -> None:
    global _global_config
    _global_config = config
