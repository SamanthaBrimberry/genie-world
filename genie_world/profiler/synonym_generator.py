"""Synonym generator: uses an LLM to suggest business-friendly synonyms for table columns."""

from __future__ import annotations

import logging

from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.profiler.models import ColumnProfile, ProfilingWarning, TableProfile

logger = logging.getLogger(__name__)


def _build_synonym_prompt(table_name: str, columns: list[ColumnProfile]) -> list[dict]:
    """Build the LLM chat messages requesting column synonyms.

    Args:
        table_name: Fully-qualified or short table name for context.
        columns: List of column profiles whose synonyms we want.

    Returns:
        A list of chat message dicts in OpenAI format.
    """
    col_list = "\n".join(f"  - {col.name} ({col.data_type})" for col in columns)

    system_msg = (
        "You are a data dictionary expert. Your job is to suggest business-friendly "
        "synonyms and alternative names for database columns so that business users "
        "can find them using natural language. Return ONLY valid JSON — no prose, no markdown."
    )

    user_msg = (
        f"Table: {table_name}\n\n"
        f"Columns:\n{col_list}\n\n"
        "For each column, provide up to 3 synonyms that business users might use to "
        "refer to that column. Return a JSON object where each key is the column name "
        "and the value is a list of synonym strings.\n\n"
        'Example format: {"column_name": ["synonym1", "synonym2"]}'
    )

    return [
        {"role": "system", "content": system_msg},
        {"role": "user", "content": user_msg},
    ]


def generate_synonyms_for_table(
    table: TableProfile,
    model: str | None = None,
) -> tuple[TableProfile, list[ProfilingWarning]]:
    """Generate LLM-based synonyms for all columns in a table.

    Calls the LLM with a prompt requesting business synonyms for each column,
    parses the JSON response, and applies the synonyms to column profiles via
    model_copy.

    On any error (LLM failure, JSON parse error) the original table is returned
    unchanged along with a ProfilingWarning.

    Args:
        table: The TableProfile whose columns should be enriched with synonyms.
        model: Optional LLM model/endpoint name. If None, uses the default from config.

    Returns:
        A tuple of (enriched TableProfile, list of ProfilingWarning).
    """
    full_name = f"{table.catalog}.{table.schema_name}.{table.table}"
    warnings: list[ProfilingWarning] = []

    if not table.columns:
        return table, warnings

    prompt = _build_synonym_prompt(full_name, table.columns)

    try:
        raw_response = call_llm(prompt, model=model)
        synonyms_map: dict = parse_json_from_llm_response(raw_response)
    except Exception as exc:
        logger.warning("Synonym generation failed for %s: %s", full_name, exc)
        warnings.append(
            ProfilingWarning(
                table=full_name,
                tier="synonyms",
                message=f"Synonym generation failed: {exc}",
            )
        )
        return table, warnings

    enriched_columns: list[ColumnProfile] = []
    for col in table.columns:
        col_synonyms = synonyms_map.get(col.name)
        if col_synonyms and isinstance(col_synonyms, list):
            enriched_columns.append(col.model_copy(update={"synonyms": col_synonyms}))
        else:
            enriched_columns.append(col)

    return table.model_copy(update={"columns": enriched_columns}), warnings
