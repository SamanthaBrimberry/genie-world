"""Benchmarks diagnoser: classify failures via LLM and flag performance issues."""

from __future__ import annotations

import logging

from genie_world.benchmarks.models import (
    BenchmarkLabel,
    BenchmarkResult,
    Diagnosis,
    FailureType,
)
from genie_world.core.llm import call_llm, parse_json_from_llm_response
from genie_world.core.tracing import trace

logger = logging.getLogger(__name__)

# Threshold: flag as slow when genie is more than 10x slower than expected
_SLOW_THRESHOLD = 10.0


def _build_diagnosis_prompt(
    question: str,
    expected_sql: str,
    genie_sql: str | None,
    expected_sample: list,
    genie_sample: list,
    tables: list,
) -> list[dict]:
    """Build the LLM prompt for failure diagnosis."""
    tables_str = str(tables) if tables else "(no tables in config)"
    return [
        {
            "role": "system",
            "content": (
                "You are a data engineering expert diagnosing why a Genie AI system "
                "failed to answer a question correctly. Analyze the question, the expected "
                "SQL, and what Genie actually produced to classify the failure type.\n\n"
                "Failure types:\n"
                "- wrong_table: Genie queried the wrong table(s)\n"
                "- missing_join: Genie missed a required JOIN\n"
                "- wrong_aggregation: Genie used the wrong aggregation (COUNT vs SUM, etc.)\n"
                "- wrong_filter: Genie applied an incorrect filter condition\n"
                "- missing_filter: Genie omitted a required filter\n"
                "- entity_mismatch: Genie used a synonym or alias that doesn't match the schema\n"
                "- wrong_column: Genie selected the wrong column(s)\n"
                "- wrong_date_handling: Genie handled dates/time incorrectly\n"
                "- missing_example: No example exists for this type of question\n"
                "- ambiguous_query: The question is inherently ambiguous\n\n"
                "Respond with JSON only: "
                "{\"failure_type\": \"...\", \"detail\": \"...\", \"affected_config_section\": \"...\"}"
            ),
        },
        {
            "role": "user",
            "content": (
                f"Question: {question}\n\n"
                f"Expected SQL:\n{expected_sql}\n\n"
                f"Genie SQL:\n{genie_sql or '(no SQL generated)'}\n\n"
                f"Expected result sample: {expected_sample}\n\n"
                f"Genie result sample: {genie_sample}\n\n"
                f"Space config tables: {tables_str}\n\n"
                "What is the failure type? Return JSON only."
            ),
        },
    ]


def _check_performance(question: str, result) -> str | None:
    """Return a performance warning string if the question is slow, else None."""
    if (
        result.expected_metrics
        and result.genie_metrics
        and result.expected_metrics.execution_time_ms is not None
        and result.genie_metrics.execution_time_ms is not None
        and result.expected_metrics.execution_time_ms > 0
    ):
        ratio = result.genie_metrics.execution_time_ms / result.expected_metrics.execution_time_ms
        if ratio > _SLOW_THRESHOLD:
            return (
                f"Query is slow: Genie took "
                f"{result.genie_metrics.execution_time_ms:.0f}ms vs expected "
                f"{result.expected_metrics.execution_time_ms:.0f}ms "
                f"({ratio:.1f}x slower)"
            )
    return None


@trace(name="diagnose_failures", span_type="CHAIN")
def diagnose_failures(results: BenchmarkResult) -> list[Diagnosis]:
    """Diagnose failures in benchmark results using LLM classification.

    For each INCORRECT, NO_SQL, or UNCERTAIN question:
    - Sends context to LLM to classify the failure type
    - Parses LLM response to extract failure_type, detail, affected_config_section

    For ALL questions (including CORRECT):
    - Checks performance: flags if genie_metrics > 10x expected_metrics

    Skips EXPECTED_SQL_ERROR questions entirely.

    Args:
        results: BenchmarkResult from the evaluator.

    Returns:
        List of Diagnosis objects, one per diagnosed question.
    """
    diagnoses: list[Diagnosis] = []

    # Extract table info from space_config for context
    tables = []
    if results.space_config:
        data_sources = results.space_config.get("data_sources", {})
        tables = data_sources.get("tables", [])

    for result in results.questions:
        # Skip EXPECTED_SQL_ERROR entirely
        if result.label == BenchmarkLabel.EXPECTED_SQL_ERROR:
            continue

        # Check performance for all non-skipped questions
        perf_warning = _check_performance(result.question, result)

        # For CORRECT questions: only create a Diagnosis if there's a performance warning
        if result.label == BenchmarkLabel.CORRECT:
            if perf_warning:
                # Use a benign failure_type; just carry the performance warning
                diagnoses.append(
                    Diagnosis(
                        question=result.question,
                        failure_type=FailureType.AMBIGUOUS_QUERY,
                        detail="No functional failure; see performance warning.",
                        affected_config_section="performance",
                        performance_warning=perf_warning,
                    )
                )
            continue

        # For INCORRECT, NO_SQL, UNCERTAIN: call LLM for failure classification
        genie_sql = None
        if result.genie_response:
            genie_sql = result.genie_response.generated_sql

        expected_sample = []
        genie_sample = []
        if result.expected_result:
            expected_sample = result.expected_result.get("data", [])[:5]
        if result.genie_response and result.genie_response.result:
            genie_sample = result.genie_response.result.get("data", [])[:5]

        try:
            messages = _build_diagnosis_prompt(
                question=result.question,
                expected_sql=result.expected_sql,
                genie_sql=genie_sql,
                expected_sample=expected_sample,
                genie_sample=genie_sample,
                tables=tables,
            )
            llm_response = call_llm(messages=messages, max_tokens=512)
            parsed = parse_json_from_llm_response(llm_response)

            failure_type_str = parsed.get("failure_type", "ambiguous_query")
            try:
                failure_type = FailureType(failure_type_str)
            except ValueError:
                logger.warning("Unknown failure_type from LLM: %s", failure_type_str)
                failure_type = FailureType.AMBIGUOUS_QUERY

            detail = parsed.get("detail", "No detail provided")
            affected_config_section = parsed.get("affected_config_section", "unknown")

            diagnoses.append(
                Diagnosis(
                    question=result.question,
                    failure_type=failure_type,
                    detail=detail,
                    affected_config_section=affected_config_section,
                    performance_warning=perf_warning,
                )
            )

        except Exception as e:
            logger.error("LLM diagnosis failed for question %r: %s", result.question, e)
            diagnoses.append(
                Diagnosis(
                    question=result.question,
                    failure_type=FailureType.AMBIGUOUS_QUERY,
                    detail=f"Diagnosis failed: {e}",
                    affected_config_section="unknown",
                    performance_warning=perf_warning,
                )
            )

    return diagnoses
