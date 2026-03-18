import json
import pytest
from unittest.mock import MagicMock, patch, call
from genie_world.core.genie_client import GenieClient, GenieResponse


class TestGenieResponse:
    def test_minimal(self):
        r = GenieResponse(question="test", status="COMPLETED", duration_seconds=1.0)
        assert r.generated_sql is None
        assert r.states == []

    def test_full(self):
        r = GenieResponse(
            question="How many?", status="COMPLETED",
            generated_sql="SELECT COUNT(*) FROM t",
            result={"columns": [{"name": "count"}], "data": [["42"]], "row_count": 1},
            duration_seconds=3.5,
            states=["FETCHING_METADATA", "ASKING_AI", "EXECUTING_QUERY", "COMPLETED"],
            conversation_id="conv-123",
        )
        assert r.generated_sql == "SELECT COUNT(*) FROM t"
        assert len(r.states) == 4


class TestGenieClientAsk:
    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_successful_question(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock start-conversation
        mock_client.api_client.do.side_effect = [
            # POST start-conversation
            {"conversation_id": "conv-1", "message_id": "msg-1"},
            # GET poll - ASKING_AI
            {"status": "ASKING_AI", "attachments": []},
            # GET poll - COMPLETED with SQL
            {
                "status": "COMPLETED",
                "attachments": [
                    {
                        "query": {"query": "SELECT COUNT(*) FROM t", "description": "Count rows"},
                        "attachment_id": "att-1",
                    }
                ],
            },
            # GET query-result
            {
                "statement_response": {
                    "status": {"state": "SUCCEEDED"},
                    "manifest": {"schema": {"columns": [{"name": "count", "type_name": "LONG"}]}},
                    "result": {"data_array": [["42"]]},
                }
            },
        ]

        gc = GenieClient("space-123")
        resp = gc.ask("How many rows?")

        assert resp.status == "COMPLETED"
        assert resp.generated_sql == "SELECT COUNT(*) FROM t"
        assert resp.result is not None
        assert resp.result["row_count"] == 1

    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_failed_question(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.side_effect = [
            {"conversation_id": "conv-1", "message_id": "msg-1"},
            {"status": "FAILED", "error": {"message": "Could not understand query"}},
        ]

        gc = GenieClient("space-123")
        resp = gc.ask("gibberish query")

        assert resp.status == "FAILED"
        assert resp.generated_sql is None
        assert "Could not understand" in resp.error

    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_text_response_no_sql(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.side_effect = [
            {"conversation_id": "conv-1", "message_id": "msg-1"},
            {
                "status": "COMPLETED",
                "attachments": [
                    {"text": {"content": "I don't have enough information to answer that."}}
                ],
            },
        ]

        gc = GenieClient("space-123")
        resp = gc.ask("What is the meaning of life?")

        assert resp.status == "COMPLETED"
        assert resp.generated_sql is None


class TestGenieClientConfig:
    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_get_config(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.return_value = {
            "space_id": "space-123",
            "serialized_space": '{"version": 2, "data_sources": {"tables": []}}',
        }

        gc = GenieClient("space-123")
        config = gc.get_config()

        assert config["version"] == 2

    @patch("genie_world.core.genie_client.get_workspace_client")
    def test_update_config(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.api_client.do.return_value = {"space_id": "space-123"}

        gc = GenieClient("space-123")
        result = gc.update_config({"version": 2, "_internal": "strip me"})

        # Verify _internal field was stripped
        call_args = mock_client.api_client.do.call_args
        body = call_args.kwargs.get("body", call_args[1].get("body", {}))
        serialized = json.loads(body["serialized_space"])
        assert "_internal" not in serialized
