"""LLM-powered description generation for tables and columns with missing metadata."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)


def _needs_enrichment(table: TableProfile) -> bool:
    """Check if a table has any missing descriptions."""
    if not table.description:
        return True
    return any(col.description is None for col in table.columns)


def _build_description_prompt(table: TableProfile) -> list[dict]:
    """Build LLM prompt to generate missing descriptions."""
    missing_cols = [col for col in table.columns if col.description is None]
    col_info = "\n".join(
        f"  - {col.name} ({col.data_type})"
        + (f" — samples: {', '.join(col.sample_values[:3])}" if col.sample_values else "")
        for col in missing_cols
    )

    needs_table_desc = "YES — generate a table_description" if not table.description else "NO — table already has a description"

    system_msg = (
        "You are a data catalog expert. Generate clear, concise descriptions for "
        "database tables and columns based on their names, types, and context. "
        "Return ONLY valid JSON — no prose, no markdown."
    )

    user_msg = (
        f"Table: {table.catalog}.{table.schema_name}.{table.table}\n"
        f"Table description needed: {needs_table_desc}\n\n"
        f"Columns needing descriptions:\n{col_info}\n\n"
        "Return a JSON object with:\n"
        '- "table_description": string (or null if not needed)\n'
        '- "columns": {"column_name": "description", ...}\n\n'
        "Keep descriptions concise (1-2 sentences). Focus on business meaning."
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


@trace(name="enrich_descriptions_for_table", span_type="CHAIN")
def enrich_descriptions_for_table(
    table: TableProfile,
    model: str | None = None,
) -> tuple[TableProfile, list[ProfilingWarning]]:
    """Fill in missing table and column descriptions via LLM.

    Preserves existing descriptions. Only calls LLM if there are gaps.

    Returns (enriched_table, warnings).
    """
    full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
    warnings: list[ProfilingWarning] = []

    if not _needs_enrichment(table):
        return table, warnings

    prompt = _build_description_prompt(table)

    try:
        raw = call_llm(prompt, model=model)
        result = parse_json_from_llm_response(raw)
    except Exception as exc:
        logger.warning("Description enrichment failed for %s: %s", full_name, exc)
        warnings.append(
            ProfilingWarning(table=full_name, tier="descriptions", message=str(exc))
        )
        return table, warnings

    # Apply table description if missing
    table_desc = table.description
    if not table_desc and result.get("table_description"):
        table_desc = result["table_description"]

    # Apply column descriptions where missing
    col_descs = result.get("columns", {})
    enriched_columns = []
    for col in table.columns:
        if col.description is None and col.name in col_descs:
            enriched_columns.append(
                col.model_copy(update={"description": col_descs[col.name]})
            )
        else:
            enriched_columns.append(col)

    return table.model_copy(update={
        "description": table_desc,
        "columns": enriched_columns,
    }), warnings
