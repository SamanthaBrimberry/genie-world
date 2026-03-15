import json
import pytest
from unittest.mock import MagicMock, patch
from genie_world.core.llm import (
    call_llm,
    parse_json_from_llm_response,
    _repair_json,
)


class TestRepairJson:
    def test_removes_trailing_comma(self):
        result = _repair_json('{"a": 1, "b": 2,}')
        assert json.loads(result) == {"a": 1, "b": 2}

    def test_fixes_missing_comma_between_objects(self):
        result = _repair_json('{"a": 1}\n{"b": 2}')
        # After repair, should be parseable (as part of a larger structure)
        assert "," in result


class TestParseJsonFromLlmResponse:
    def test_parses_plain_json(self):
        result = parse_json_from_llm_response('{"key": "value"}')
        assert result == {"key": "value"}

    def test_parses_markdown_code_block(self):
        content = '```json\n{"key": "value"}\n```'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_parses_json_with_preamble(self):
        content = 'Here is the result:\n{"key": "value"}'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}

    def test_raises_on_empty(self):
        with pytest.raises(ValueError):
            parse_json_from_llm_response("")

    def test_repairs_trailing_comma(self):
        content = '{"key": "value",}'
        result = parse_json_from_llm_response(content)
        assert result == {"key": "value"}


class TestCallLlm:
    @patch("genie_world.core.llm.get_workspace_client")
    def test_calls_serving_endpoint_via_raw_api(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Raw API returns a plain dict
        mock_client.api_client.do.return_value = {
            "choices": [{"message": {"content": "test response"}}]
        }

        result = call_llm(
            messages=[{"role": "user", "content": "hello"}],
            model="test-model",
        )

        assert result == "test response"
        mock_client.api_client.do.assert_called_once_with(
            "POST",
            "/serving-endpoints/test-model/invocations",
            body={"messages": [{"role": "user", "content": "hello"}]},
        )

    @patch("genie_world.core.llm.get_workspace_client")
    def test_raises_on_empty_content(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.return_value = {
            "choices": [{"message": {"content": ""}}]
        }

        with pytest.raises(ValueError, match="empty content"):
            call_llm(
                messages=[{"role": "user", "content": "hello"}],
                model="test-model",
            )

    @patch("genie_world.core.llm.get_workspace_client")
    def test_raises_on_no_choices(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        mock_client.api_client.do.return_value = {"choices": []}

        with pytest.raises(ValueError, match="no choices"):
            call_llm(
                messages=[{"role": "user", "content": "hello"}],
                model="test-model",
            )

    @patch("genie_world.core.llm.get_workspace_client")
    def test_raises_on_api_error(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.api_client.do.side_effect = Exception("429 Rate Limited")

        with pytest.raises(Exception, match="Rate Limited"):
            call_llm(
                messages=[{"role": "user", "content": "hello"}],
                model="test-model",
            )
