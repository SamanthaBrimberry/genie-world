"""Benchmarks suggester: generate targeted config changes from failure diagnoses."""

from __future__ import annotations

import logging

from genie_world.benchmarks.models import (
    BenchmarkResult,
    Diagnosis,
    FailureType,
    Suggestion,
)
from genie_world.builder.sql_validator import validate_and_fix_sql
from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace

logger = logging.getLogger(__name__)


def _get_tables_context(results: BenchmarkResult) -> str:
    """Extract table info from space_config as a concise context string."""
    if not results.space_config:
        return "(no table config)"
    data_sources = results.space_config.get("data_sources", {})
    tables = data_sources.get("tables", [])
    if not tables:
        return "(no tables in config)"

    lines = []
    for t in tables:
        ident = t.get("identifier", "?")
        cols = t.get("column_configs", [])
        col_names = [c.get("column_name", "?") for c in cols[:20]]
        lines.append(f"  {ident}: {', '.join(col_names)}")
    return "\n".join(lines)


def _suggest_add_example(
    diagnosis: Diagnosis,
    results: BenchmarkResult,
    warehouse_id: str,
) -> Suggestion | None:
    """Generate an add-example suggestion for MISSING_EXAMPLE or WRONG_AGGREGATION."""
    tables_ctx = _get_tables_context(results)
    prompt = [
        {
            "role": "system",
            "content": (
                "You are a SQL expert generating example question-SQL pairs for a Genie AI space. "
                "Return a JSON object with 'question' and 'sql' fields only."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Generate a correct SQL example for this type of question:\n"
                f"Question: {diagnosis.question}\n"
                f"Failure detail: {diagnosis.detail}\n"
                f"Available tables: {tables_ctx}\n\n"
                "Return JSON only: {\"question\": \"...\", \"sql\": \"SELECT ...\"}"
            ),
        },
    ]

    llm_response = call_llm(messages=prompt, max_tokens=512)
    parsed = parse_json_from_llm_response(llm_response)

    sql = parsed.get("sql", "")
    question = parsed.get("question", diagnosis.question)

    if sql:
        # Validate SQL; use a minimal profile stub since we don't have full schema here
        try:
            from datetime import datetime
            from genie_world.profiler.models import SchemaProfile
            profile = SchemaProfile(
                schema_version="1.0", catalog="", schema_name="",
                tables=[], relationships=[], profiled_at=datetime.now(),
            )
            validated_sql, warnings = validate_and_fix_sql(
                sql=sql,
                question=question,
                profile=profile,
                warehouse_id=warehouse_id,
            )
        except Exception as e:
            logger.warning("SQL validation skipped: %s", e)
            validated_sql = sql
            warnings = []

        return Suggestion(
            section="example_question_sqls",
            action="add",
            content={"question": [question], "sql": [validated_sql]},
            rationale=(
                f"Adding example to address {diagnosis.failure_type.value}: "
                f"{diagnosis.detail}"
            ),
            addresses_questions=[diagnosis.question],
        )

    return None


def _suggest_text_instruction(
    diagnosis: Diagnosis,
    results: BenchmarkResult,
) -> Suggestion | None:
    """Generate an add-instruction suggestion for WRONG_TABLE or WRONG_COLUMN."""
    tables_ctx = _get_tables_context(results)
    prompt = [
        {
            "role": "system",
            "content": (
                "You are a data expert writing guidance instructions for a Genie AI space. "
                "Return a JSON object with an 'instruction' field only."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Write a clear instruction to prevent this failure:\n"
                f"Question: {diagnosis.question}\n"
                f"Failure type: {diagnosis.failure_type.value}\n"
                f"Detail: {diagnosis.detail}\n"
                f"Available tables: {tables_ctx}\n\n"
                "Return JSON only: {\"instruction\": \"...\"}"
            ),
        },
    ]

    llm_response = call_llm(messages=prompt, max_tokens=256)
    parsed = parse_json_from_llm_response(llm_response)
    instruction = parsed.get("instruction", "")

    if instruction:
        return Suggestion(
            section="text_instructions",
            action="add",
            content={"content": [instruction]},
            rationale=(
                f"Adding instruction to address {diagnosis.failure_type.value}: "
                f"{diagnosis.detail}"
            ),
            addresses_questions=[diagnosis.question],
        )

    return None


def _suggest_sql_filter(
    diagnosis: Diagnosis,
    results: BenchmarkResult,
) -> Suggestion | None:
    """Generate an add-filter suggestion for MISSING_FILTER or WRONG_FILTER."""
    tables_ctx = _get_tables_context(results)
    prompt = [
        {
            "role": "system",
            "content": (
                "You are a SQL expert writing filter snippets for a Genie AI space. "
                "Return a JSON object with 'filter' and 'description' fields only."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Write the correct SQL filter snippet to address this failure:\n"
                f"Question: {diagnosis.question}\n"
                f"Failure type: {diagnosis.failure_type.value}\n"
                f"Detail: {diagnosis.detail}\n"
                f"Available tables: {tables_ctx}\n\n"
                "Return JSON only: {\"filter\": \"WHERE ...\", \"description\": \"...\"}"
            ),
        },
    ]

    llm_response = call_llm(messages=prompt, max_tokens=256)
    parsed = parse_json_from_llm_response(llm_response)
    filter_sql = parsed.get("filter", "")
    description = parsed.get("description", diagnosis.detail)

    if filter_sql:
        return Suggestion(
            section="sql_snippets",
            action="add",
            content={"sql": [filter_sql], "display_name": description},
            rationale=(
                f"Adding filter snippet to address {diagnosis.failure_type.value}: "
                f"{diagnosis.detail}"
            ),
            addresses_questions=[diagnosis.question],
        )

    return None


def _suggest_synonyms(
    diagnosis: Diagnosis,
    results: BenchmarkResult,
) -> Suggestion | None:
    """Generate a synonym update for ENTITY_MISMATCH."""
    tables_ctx = _get_tables_context(results)
    prompt = [
        {
            "role": "system",
            "content": (
                "You are a data expert configuring entity synonyms for a Genie AI space. "
                "Return a JSON object with 'synonyms', 'column', and 'table' fields."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Suggest synonyms to add to resolve this entity mismatch:\n"
                f"Question: {diagnosis.question}\n"
                f"Detail: {diagnosis.detail}\n"
                f"Available tables: {tables_ctx}\n\n"
                "Return JSON only: {\"synonyms\": [...], \"column\": \"...\", \"table\": \"...\"}"
            ),
        },
    ]

    llm_response = call_llm(messages=prompt, max_tokens=256)
    parsed = parse_json_from_llm_response(llm_response)
    synonyms = parsed.get("synonyms", [])
    column = parsed.get("column", "")
    table = parsed.get("table", "")

    if synonyms:
        return Suggestion(
            section="column_configs",
            action="add",
            content={"synonyms": synonyms, "column": column, "table": table},
            rationale=(
                f"Adding synonyms to address entity mismatch: {diagnosis.detail}"
            ),
            addresses_questions=[diagnosis.question],
        )

    return None


def _suggest_date_expression(
    diagnosis: Diagnosis,
    results: BenchmarkResult,
) -> Suggestion | None:
    """Generate expression snippet + instruction update for WRONG_DATE_HANDLING."""
    tables_ctx = _get_tables_context(results)
    prompt = [
        {
            "role": "system",
            "content": (
                "You are a SQL expert fixing date handling issues in a Genie AI space. "
                "Return a JSON object with 'expression' and 'instruction' fields."
            ),
        },
        {
            "role": "user",
            "content": (
                f"Provide the correct date expression and instruction to fix this issue:\n"
                f"Question: {diagnosis.question}\n"
                f"Detail: {diagnosis.detail}\n"
                f"Available tables: {tables_ctx}\n\n"
                "Return JSON only: {\"expression\": \"...\", \"instruction\": \"...\"}"
            ),
        },
    ]

    llm_response = call_llm(messages=prompt, max_tokens=256)
    parsed = parse_json_from_llm_response(llm_response)
    expression = parsed.get("expression", "")
    instruction = parsed.get("instruction", "")

    suggestions = []
    if expression:
        suggestions.append(
            Suggestion(
                section="sql_snippets",
                action="add",
                content={"sql": [expression], "display_name": "Date expression snippet", "alias": "date_expr"},
                rationale=(
                    f"Adding date expression snippet to address wrong date handling: "
                    f"{diagnosis.detail}"
                ),
                addresses_questions=[diagnosis.question],
            )
        )

    if instruction:
        suggestions.append(
            Suggestion(
                section="text_instructions",
                action="add",
                content={"content": [instruction]},
                rationale=(
                    f"Adding date handling instruction: {diagnosis.detail}"
                ),
                addresses_questions=[diagnosis.question],
            )
        )

    return suggestions if suggestions else None


@trace(name="generate_suggestions", span_type="CHAIN")
def generate_suggestions(
    diagnoses: list[Diagnosis],
    results: BenchmarkResult,
    warehouse_id: str,
) -> list[Suggestion]:
    """Generate targeted config change suggestions based on failure diagnoses.

    For each diagnosis, generates a config change:
    - MISSING_EXAMPLE / WRONG_AGGREGATION → add to example_question_sqls
    - WRONG_TABLE / WRONG_COLUMN → update text_instructions
    - MISSING_FILTER / WRONG_FILTER → add to sql_snippets.filters
    - ENTITY_MISMATCH → update column_configs synonyms
    - WRONG_DATE_HANDLING → add expression snippet + instruction update

    Uses call_llm for content and validate_and_fix_sql for SQL validation.
    On any LLM error, returns empty list (graceful degradation).

    Args:
        diagnoses: List of Diagnosis from diagnose_failures().
        results: The original BenchmarkResult (for space_config context).
        warehouse_id: Databricks warehouse for SQL validation.

    Returns:
        List of Suggestion objects with config changes to apply.
    """
    suggestions: list[Suggestion] = []

    for diagnosis in diagnoses:
        try:
            result = _route_diagnosis(diagnosis, results, warehouse_id)
            if result is not None:
                if isinstance(result, list):
                    suggestions.extend(result)
                else:
                    suggestions.append(result)
        except Exception as e:
            logger.warning(
                "Failed to generate suggestion for question %r (failure_type=%s): %s",
                diagnosis.question,
                diagnosis.failure_type,
                e,
            )
            # Continue to next diagnosis rather than aborting all

    return suggestions


def _route_diagnosis(
    diagnosis: Diagnosis,
    results: BenchmarkResult,
    warehouse_id: str,
) -> Suggestion | None:
    """Route a diagnosis to the appropriate suggestion generator."""
    ft = diagnosis.failure_type

    if ft in (FailureType.MISSING_EXAMPLE, FailureType.WRONG_AGGREGATION):
        return _suggest_add_example(diagnosis, results, warehouse_id)

    elif ft in (FailureType.WRONG_TABLE, FailureType.WRONG_COLUMN, FailureType.MISSING_JOIN):
        return _suggest_text_instruction(diagnosis, results)

    elif ft in (FailureType.MISSING_FILTER, FailureType.WRONG_FILTER):
        return _suggest_sql_filter(diagnosis, results)

    elif ft == FailureType.ENTITY_MISMATCH:
        return _suggest_synonyms(diagnosis, results)

    elif ft == FailureType.WRONG_DATE_HANDLING:
        return _suggest_date_expression(diagnosis, results)

    else:
        # AMBIGUOUS_QUERY or unknown: generate a text instruction
        return _suggest_text_instruction(diagnosis, results)
