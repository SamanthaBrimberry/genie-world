"""SQL validation with LLM-powered fix and retry."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm
from genie_world.core.sql import execute_sql
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


def _build_fix_prompt(
    sql: str, error: str, question: str, profile: SchemaProfile
) -> list[dict]:
    """Build prompt asking LLM to fix a failing SQL query."""
    tables_info = "\n".join(
        f"  {t.catalog}.{t.schema_name}.{t.table}: "
        + ", ".join(f"{c.name} ({c.data_type})" for c in t.columns)
        for t in profile.tables
    )

    return [
        {"role": "system", "content": (
            "You are a SQL expert. Fix the SQL query based on the error. "
            "Return ONLY the corrected SQL — no explanation, no markdown."
        )},
        {"role": "user", "content": (
            f"Question: {question}\n\n"
            f"SQL:\n{sql}\n\n"
            f"Error:\n{error}\n\n"
            f"Available tables and columns:\n{tables_info}\n\n"
            "Return the corrected SQL only."
        )},
    ]


@trace(name="validate_and_fix_sql", span_type="CHAIN")
def validate_and_fix_sql(
    sql: str,
    question: str,
    profile: SchemaProfile,
    warehouse_id: str,
    max_retries: int = 3,
) -> tuple[str, list[str]]:
    """Execute SQL, retry with LLM fix on failure.

    Returns (final_sql, list_of_warning_strings).
    """
    warnings: list[str] = []
    current_sql = sql

    for attempt in range(1 + max_retries):
        result = execute_sql(current_sql, warehouse_id=warehouse_id)

        if result["error"] is None:
            return current_sql, warnings

        error_msg = result["error"]
        logger.info(f"SQL validation attempt {attempt + 1} failed: {error_msg}")

        if attempt < max_retries:
            # Ask LLM to fix
            try:
                prompt = _build_fix_prompt(current_sql, error_msg, question, profile)
                fixed = call_llm(prompt)
                current_sql = fixed.strip()
            except Exception as e:
                logger.warning(f"LLM fix attempt failed: {e}")
                warnings.append(f"LLM fix failed: {e}")
                break

    # All retries exhausted
    warnings.append(f"SQL validation failed after {max_retries} retries: {error_msg}")
    return current_sql, warnings
