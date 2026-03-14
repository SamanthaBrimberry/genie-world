import json
import os
import tempfile
import pytest
from pydantic import BaseModel
from genie_world.core.storage import LocalStorage, save_artifact, load_artifact


class SampleModel(BaseModel):
    name: str
    value: int


class TestLocalStorage:
    def test_save_and_load(self, tmp_path):
        storage = LocalStorage(base_path=str(tmp_path))
        model = SampleModel(name="test", value=42)

        storage.save("my_artifact.json", model)
        loaded = storage.load("my_artifact.json", SampleModel)

        assert loaded.name == "test"
        assert loaded.value == 42

    def test_load_missing_returns_none(self, tmp_path):
        storage = LocalStorage(base_path=str(tmp_path))
        result = storage.load("nonexistent.json", SampleModel)
        assert result is None

    def test_list_artifacts(self, tmp_path):
        storage = LocalStorage(base_path=str(tmp_path))
        storage.save("a.json", SampleModel(name="a", value=1))
        storage.save("b.json", SampleModel(name="b", value=2))

        artifacts = storage.list_artifacts()
        assert sorted(artifacts) == ["a.json", "b.json"]


class TestConvenienceFunctions:
    def test_save_and_load_with_local_path(self, tmp_path):
        model = SampleModel(name="test", value=99)
        path = str(tmp_path / "output.json")

        save_artifact(model, path)
        loaded = load_artifact(path, SampleModel)

        assert loaded.name == "test"
        assert loaded.value == 99
