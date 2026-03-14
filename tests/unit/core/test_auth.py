import os
import pytest
from unittest.mock import MagicMock, patch
from genie_world.core.auth import (
    get_workspace_client, set_obo_token, get_obo_token, is_running_on_databricks_apps,
)


class TestOboToken:
    def test_default_is_none(self):
        assert get_obo_token() is None

    def test_set_and_get(self):
        set_obo_token("test-token")
        assert get_obo_token() == "test-token"
        set_obo_token(None)


class TestIsRunningOnDatabricksApps:
    def test_false_when_no_env(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_APP_PORT", raising=False)
        assert is_running_on_databricks_apps() is False

    def test_true_when_env_set(self, monkeypatch):
        monkeypatch.setenv("DATABRICKS_APP_PORT", "8080")
        assert is_running_on_databricks_apps() is True


class TestGetWorkspaceClient:
    @patch("genie_world.core.auth.WorkspaceClient")
    def test_returns_obo_client_when_token_set(self, mock_ws_class, monkeypatch):
        monkeypatch.setenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")
        set_obo_token("obo-token-123")
        client = get_workspace_client()
        mock_ws_class.assert_called_once_with(
            host="https://test.cloud.databricks.com",
            token="obo-token-123",
            auth_type="pat",
        )
        set_obo_token(None)

    @patch("genie_world.core.auth.WorkspaceClient")
    def test_returns_default_client_when_no_token(self, mock_ws_class):
        set_obo_token(None)
        client = get_workspace_client()
        mock_ws_class.assert_called_once_with()
