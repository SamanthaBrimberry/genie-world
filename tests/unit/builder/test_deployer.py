import json
import pytest
from unittest.mock import patch, MagicMock
from genie_world.builder.deployer import create_space


class TestCreateSpace:
    @patch("genie_world.builder.deployer.get_workspace_client")
    def test_creates_space_successfully(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.config.host = "https://test.cloud.databricks.com"
        mock_client.api_client.do.return_value = {"space_id": "abc123"}

        result = create_space(
            config={"version": 2},
            display_name="Test Space",
            warehouse_id="wh-123",
            parent_path="/Workspace/Users/me/",
        )

        assert result["space_id"] == "abc123"
        assert "space_url" in result
        assert "abc123" in result["space_url"]

    @patch("genie_world.builder.deployer.get_workspace_client")
    def test_raises_on_oversized_config(self, mock_get_client):
        huge_config = {"data": "x" * (4 * 1024 * 1024)}  # > 3.5 MB

        with pytest.raises(ValueError, match="3.5 MB"):
            create_space(huge_config, "Test", "wh-123", "/Workspace/")

    def test_raises_on_empty_display_name(self):
        with pytest.raises(ValueError, match="Display name"):
            create_space({"version": 2}, "", "wh-123", "/Workspace/")
