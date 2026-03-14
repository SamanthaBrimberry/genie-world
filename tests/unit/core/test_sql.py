import pytest
from unittest.mock import MagicMock, patch
from genie_world.core.sql import validate_sql_read_only, execute_sql, SqlValidationError


class TestValidateSqlReadOnly:
    def test_allows_select(self):
        validate_sql_read_only("SELECT * FROM my_table")

    def test_allows_with_cte(self):
        validate_sql_read_only("WITH cte AS (SELECT 1) SELECT * FROM cte")

    def test_blocks_drop(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DROP TABLE my_table")

    def test_blocks_delete(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("DELETE FROM my_table WHERE id = 1")

    def test_blocks_insert(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("INSERT INTO my_table VALUES (1)")

    def test_blocks_update(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("UPDATE my_table SET col = 1")

    def test_blocks_statement_chaining(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("SELECT 1; DROP TABLE my_table")

    def test_rejects_non_select(self):
        with pytest.raises(SqlValidationError):
            validate_sql_read_only("SHOW TABLES")


class TestExecuteSql:
    def test_returns_error_when_no_warehouse(self):
        result = execute_sql("SELECT 1", warehouse_id=None)
        assert result["error"] is not None
        assert result["row_count"] == 0

    def test_returns_error_for_dangerous_sql(self):
        result = execute_sql("DROP TABLE foo", warehouse_id="wh-123")
        assert result["error"] is not None

    @patch("genie_world.core.sql.get_workspace_client")
    def test_executes_and_parses_result(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client

        # Mock the response structure
        mock_col = MagicMock()
        mock_col.name = "id"
        mock_col.type_name = "INT"

        mock_response = MagicMock()
        mock_response.status.state.value = "SUCCEEDED"
        mock_response.manifest.schema.columns = [mock_col]
        mock_response.manifest.truncated = False
        mock_response.result.data_array = [["1"], ["2"]]

        mock_client.statement_execution.execute_statement.return_value = mock_response

        result = execute_sql("SELECT id FROM t", warehouse_id="wh-123")
        assert result["error"] is None
        assert result["row_count"] == 2
        assert result["columns"] == [{"name": "id", "type_name": "INT"}]
        assert result["data"] == [["1"], ["2"]]
