"""LLM generator: SQL snippets (filters, expressions, measures)."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)

_EMPTY_SNIPPETS = {"filters": [], "expressions": [], "measures": []}


def _build_snippets_prompt(profile: SchemaProfile) -> list[dict]:
    """Build prompt for generating SQL snippets."""
    tables_info = []
    for t in profile.tables:
        cols = "\n".join(
            f"    - {c.name} ({c.data_type})"
            + (f" — top values: {', '.join(c.top_values[:5])}" if c.top_values else "")
            + (f" — {c.description}" if c.description else "")
            for c in t.columns
        )
        tables_info.append(f"  {t.table}:\n{cols}")

    tables_text = "\n".join(tables_info)

    system_msg = (
        "You are a Databricks Genie Space configuration expert. Generate SQL snippets "
        "that help Genie answer common business questions. Return ONLY valid JSON."
    )

    user_msg = (
        f"Tables:\n{tables_text}\n\n"
        "Generate SQL snippets in three categories:\n\n"
        "1. **filters**: Common WHERE clause conditions (e.g., date ranges, status filters)\n"
        "   Each: {sql, display_name, synonyms: [...], comment, instruction}\n\n"
        "2. **expressions**: Reusable calculated columns (e.g., YEAR(date), DATEDIFF, CASE WHEN)\n"
        "   Each: {alias, sql, display_name, synonyms: [...], comment, instruction}\n\n"
        "3. **measures**: Standard aggregations (e.g., SUM, COUNT DISTINCT, AVG)\n"
        "   Each: {alias, sql, display_name, synonyms: [...], comment, instruction}\n\n"
        "IMPORTANT RULES:\n"
        "- Keep SQL simple — single expressions only, NO subqueries or nested SELECTs\n"
        "- Filters: simple conditions like column = value, column >= DATE, column IN (...)\n"
        "- Expressions: simple column transformations like YEAR(col), DATEDIFF(DAY, col1, col2)\n"
        "- Measures: simple aggregations like SUM(col), COUNT(DISTINCT col), AVG(col)\n"
        "- Use table_name.column_name format (e.g., campaigns.status, not just status)\n\n"
        "Generate 2-4 items per category.\n\n"
        'Return: {"filters": [...], "expressions": [...], "measures": [...]}'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="generate_snippets", span_type="CHAIN")
def generate_snippets(profile: SchemaProfile) -> dict:
    """Generate SQL snippet configs via LLM.

    Returns {"filters": [...], "expressions": [...], "measures": [...]}.
    On LLM error, returns empty snippets.
    """
    prompt = _build_snippets_prompt(profile)

    try:
        raw = call_llm(prompt)
        result = parse_json_from_llm_response(raw)
    except Exception as exc:
        logger.warning("Snippet generation failed: %s", exc)
        return dict(_EMPTY_SNIPPETS)

    # Validate structure
    return {
        "filters": result.get("filters", []) if isinstance(result.get("filters"), list) else [],
        "expressions": result.get("expressions", []) if isinstance(result.get("expressions"), list) else [],
        "measures": result.get("measures", []) if isinstance(result.get("measures"), list) else [],
    }
