import os
import pytest
from genie_world.core.config import GenieWorldConfig


class TestGenieWorldConfig:
    def test_defaults(self):
        config = GenieWorldConfig()
        assert config.warehouse_id is None
        assert config.llm_model == "databricks-claude-sonnet-4-6"
        assert config.storage_path is None
        assert config.mlflow_experiment_id is None
        assert config.max_workers == 4

    def test_from_env(self, monkeypatch):
        monkeypatch.setenv("GENIE_WORLD_WAREHOUSE_ID", "wh-123")
        monkeypatch.setenv("GENIE_WORLD_LLM_MODEL", "databricks-claude-opus-4-6")
        monkeypatch.setenv("GENIE_WORLD_STORAGE_PATH", "/Volumes/my/path")
        monkeypatch.setenv("GENIE_WORLD_MLFLOW_EXPERIMENT_ID", "exp-456")
        monkeypatch.setenv("GENIE_WORLD_MAX_WORKERS", "8")
        config = GenieWorldConfig.from_env()
        assert config.warehouse_id == "wh-123"
        assert config.llm_model == "databricks-claude-opus-4-6"
        assert config.storage_path == "/Volumes/my/path"
        assert config.mlflow_experiment_id == "exp-456"
        assert config.max_workers == 8

    def test_override(self):
        config = GenieWorldConfig(warehouse_id="wh-999", max_workers=2)
        assert config.warehouse_id == "wh-999"
        assert config.max_workers == 2

    def test_global_config(self, monkeypatch):
        from genie_world.core.config import get_config, set_config
        custom = GenieWorldConfig(warehouse_id="custom-wh")
        set_config(custom)
        assert get_config().warehouse_id == "custom-wh"
        set_config(GenieWorldConfig())
