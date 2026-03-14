"""Statement Execution API wrapper with read-only SQL validation.

Ported from dbx-genie-rx's sql_executor.py.
"""

from __future__ import annotations

import logging
import re

from genie_world.core.auth import get_workspace_client

logger = logging.getLogger(__name__)

MAX_ROWS = 1000
WAIT_TIMEOUT = "30s"

_BLOCKED_SQL_PATTERNS = [
    r"\b(DROP|DELETE|TRUNCATE|UPDATE|INSERT|ALTER|CREATE|GRANT|REVOKE)\b",
    r"\b(EXEC|EXECUTE|CALL)\b",
    r";\s*\w",
]


class SqlValidationError(Exception):
    """Raised when SQL validation fails."""

    pass


def validate_sql_read_only(sql: str) -> None:
    """Validate that SQL is a read-only SELECT or WITH query."""
    sql_upper = sql.upper().strip()

    if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
        raise SqlValidationError(
            "Only SELECT queries are allowed. Query must start with SELECT or WITH."
        )

    for pattern in _BLOCKED_SQL_PATTERNS:
        if re.search(pattern, sql_upper, re.IGNORECASE):
            raise SqlValidationError(
                "Query contains disallowed SQL operation. "
                "Only read-only SELECT queries are permitted."
            )


def execute_sql(
    sql: str,
    warehouse_id: str | None = None,
    row_limit: int = MAX_ROWS,
) -> dict:
    """Execute SQL on a Databricks SQL Warehouse.

    Returns dict with keys: columns, data, row_count, truncated, error.
    """
    if not warehouse_id:
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": "No warehouse_id provided",
        }

    try:
        validate_sql_read_only(sql)
    except SqlValidationError as e:
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": str(e),
        }

    client = get_workspace_client()

    try:
        response = client.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            wait_timeout=WAIT_TIMEOUT,
            row_limit=row_limit,
        )

        if response.status and response.status.state:
            state = response.status.state.value
            if state == "FAILED":
                error_msg = (
                    response.status.error.message
                    if response.status.error
                    else "Execution failed"
                )
                return {
                    "columns": [],
                    "data": [],
                    "row_count": 0,
                    "truncated": False,
                    "error": error_msg,
                }

        columns = []
        if response.manifest and response.manifest.schema:
            columns = [
                {"name": col.name, "type_name": col.type_name}
                for col in response.manifest.schema.columns or []
            ]

        data = []
        truncated = False
        if response.result and response.result.data_array:
            data = response.result.data_array
        if response.manifest:
            truncated = response.manifest.truncated or False

        return {
            "columns": columns,
            "data": data,
            "row_count": len(data),
            "truncated": truncated,
            "error": None,
        }

    except Exception as e:
        logger.error(f"SQL execution failed: {e}")
        return {
            "columns": [],
            "data": [],
            "row_count": 0,
            "truncated": False,
            "error": str(e),
        }
