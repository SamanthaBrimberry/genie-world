"""Benchmarks evaluator: hybrid programmatic + LLM comparison of Genie results."""

from __future__ import annotations

import logging
import re

from pydantic import BaseModel

from genie_world.benchmarks.models import BenchmarkLabel, ExecutionMetrics
from genie_world.core.genie_client import GenieResponse
from genie_world.core.sql import execute_sql

logger = logging.getLogger(__name__)

# Relative tolerance: 0.1% difference is acceptable
_REL_TOLERANCE = 0.001
# Absolute tolerance: differences smaller than this are acceptable
_ABS_TOLERANCE = 0.01


class EvaluationResult(BaseModel):
    """Result of evaluating a single question."""

    label: BenchmarkLabel
    confidence: float = 1.0
    expected_result: dict | None = None
    expected_metrics: ExecutionMetrics | None = None
    genie_metrics: ExecutionMetrics | None = None
    comparison_detail: str | None = None


def _normalize_columns(columns: list[str]) -> list[str]:
    """Lowercase column names and strip surrounding backticks, quotes, and whitespace."""
    result = []
    for col in columns:
        col = col.strip()
        # Strip backticks or double-quotes
        if (col.startswith("`") and col.endswith("`")) or (
            col.startswith('"') and col.endswith('"')
        ):
            col = col[1:-1]
        result.append(col.lower())
    return result


def _detect_order_by(sql: str) -> bool:
    """Return True if the SQL has a top-level ORDER BY clause (not inside a subquery).

    Tracks parenthesis depth to distinguish top-level ORDER BY from subquery ORDER BY.
    """
    depth = 0
    # Tokenize into parentheses and keyword spans
    # We scan character by character tracking depth
    sql_upper = sql.upper()
    i = 0
    n = len(sql_upper)

    while i < n:
        ch = sql_upper[i]
        if ch == "(":
            depth += 1
            i += 1
        elif ch == ")":
            depth -= 1
            i += 1
        elif depth == 0 and sql_upper[i : i + 8] == "ORDER BY":
            return True
        else:
            i += 1

    return False


def _values_equal(a: object, b: object) -> bool:
    """Compare two cell values with NULL awareness and numeric tolerance."""
    # Both None (NULL) → equal
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False

    # Try numeric comparison
    try:
        fa = float(str(a))
        fb = float(str(b))
        diff = abs(fa - fb)
        # Absolute tolerance first
        if diff <= _ABS_TOLERANCE:
            return True
        # Relative tolerance
        denom = max(abs(fa), abs(fb))
        if denom > 0 and diff / denom <= _REL_TOLERANCE:
            return True
        return False
    except (ValueError, TypeError):
        pass

    # String comparison (case-insensitive strip)
    return str(a).strip().lower() == str(b).strip().lower()


def _rows_equal_unordered(expected_data: list, genie_data: list) -> bool:
    """Compare two datasets as multisets (order-insensitive)."""
    if len(expected_data) != len(genie_data):
        return False

    # Build a copy of genie rows to mark as matched
    genie_remaining = list(genie_data)

    for exp_row in expected_data:
        matched = False
        for i, genie_row in enumerate(genie_remaining):
            if _rows_match(exp_row, genie_row):
                genie_remaining.pop(i)
                matched = True
                break
        if not matched:
            return False
    return True


def _rows_match(row_a: list, row_b: list) -> bool:
    """Return True if all values in two rows match (with tolerance)."""
    if len(row_a) != len(row_b):
        return False
    return all(_values_equal(a, b) for a, b in zip(row_a, row_b))


def _compare_results(
    expected: dict,
    genie: dict,
    order_sensitive: bool,
) -> tuple[BenchmarkLabel, str]:
    """Programmatically compare expected and Genie result sets.

    Returns:
        (BenchmarkLabel, comparison_detail string)

    Labels:
        CORRECT   — data matches within tolerance
        INCORRECT — clear structural or value mismatch
        UNCERTAIN — partial match that needs LLM review
    """
    exp_cols = [c["name"] if isinstance(c, dict) else c for c in expected.get("columns", [])]
    gen_cols = [c["name"] if isinstance(c, dict) else c for c in genie.get("columns", [])]

    exp_cols_norm = _normalize_columns(exp_cols)
    gen_cols_norm = _normalize_columns(gen_cols)

    # Column mismatch → INCORRECT
    if sorted(exp_cols_norm) != sorted(gen_cols_norm):
        return (
            BenchmarkLabel.INCORRECT,
            f"Column mismatch: expected {exp_cols_norm}, got {gen_cols_norm}",
        )

    exp_data = expected.get("data", [])
    gen_data = genie.get("data", [])
    exp_truncated = expected.get("truncated", False)
    gen_truncated = genie.get("truncated", False)

    # Row count check — skip if either is truncated
    if not exp_truncated and not gen_truncated:
        exp_count = len(exp_data)
        gen_count = len(gen_data)

        if exp_count == 0 and gen_count == 0:
            return BenchmarkLabel.CORRECT, "Both results are empty"

        if exp_count == 0 or gen_count == 0:
            return (
                BenchmarkLabel.INCORRECT,
                f"Row count mismatch: expected {exp_count}, got {gen_count}",
            )

        ratio = max(exp_count, gen_count) / min(exp_count, gen_count)
        if ratio > 2.0:
            return (
                BenchmarkLabel.INCORRECT,
                f"Row count too different: expected {exp_count}, got {gen_count} (ratio {ratio:.1f}x)",
            )

    # Reorder columns in genie data to match expected column order (for value comparison)
    if exp_cols_norm != gen_cols_norm:
        # Reorder genie columns to match expected order
        col_index = {name: i for i, name in enumerate(gen_cols_norm)}
        try:
            reorder = [col_index[c] for c in exp_cols_norm]
            gen_data = [[row[i] for i in reorder] for row in gen_data]
        except (KeyError, IndexError):
            pass  # Fall through to uncertain

    # Compare data
    if order_sensitive:
        if len(exp_data) != len(gen_data):
            if exp_truncated or gen_truncated:
                return BenchmarkLabel.UNCERTAIN, "Row count differs but results are truncated"
            return (
                BenchmarkLabel.INCORRECT,
                f"Row count mismatch (order-sensitive): expected {len(exp_data)}, got {len(gen_data)}",
            )
        match_ordered = all(_rows_match(er, gr) for er, gr in zip(exp_data, gen_data))
        if match_ordered:
            return BenchmarkLabel.CORRECT, "Results match (order-sensitive)"
        # Same count, ordered mismatch — check if it's an order issue (unordered match)
        if _rows_equal_unordered(exp_data, gen_data):
            # Rows exist but in wrong order → definitive INCORRECT for order-sensitive
            return BenchmarkLabel.INCORRECT, "Results match unordered but order is incorrect"
        # Values differ too — UNCERTAIN for LLM review
        return (
            BenchmarkLabel.UNCERTAIN,
            f"Same row count ({len(exp_data)}) but values differ (order-sensitive) — needs LLM review",
        )
    else:
        # Unordered comparison
        if (exp_truncated or gen_truncated) and len(exp_data) != len(gen_data):
            # Can't reliably compare row counts when truncated
            match = _rows_equal_unordered(
                exp_data[: min(len(exp_data), len(gen_data))],
                gen_data[: min(len(exp_data), len(gen_data))],
            )
            if match:
                return BenchmarkLabel.UNCERTAIN, "Partial match (truncated results)"
            return BenchmarkLabel.UNCERTAIN, "Cannot reliably compare truncated results"

        match = _rows_equal_unordered(exp_data, gen_data)

    if match:
        return BenchmarkLabel.CORRECT, "Results match"

    # Determine if it's a clear INCORRECT or UNCERTAIN
    # UNCERTAIN: same row count but some values differ (might be rounding, formatting)
    if not exp_truncated and not gen_truncated and len(exp_data) == len(gen_data):
        return (
            BenchmarkLabel.UNCERTAIN,
            f"Same row count ({len(exp_data)}) but values differ — needs LLM review",
        )

    return BenchmarkLabel.INCORRECT, "Results do not match"


def _llm_compare(
    question: str,
    expected_sql: str,
    expected_result: dict,
    genie_sql: str | None,
    genie_result: dict,
) -> tuple[BenchmarkLabel, float, str]:
    """Use LLM as a fallback judge for UNCERTAIN cases.

    Returns (label, confidence, detail).
    """
    try:
        from genie_world.core.llm import call_llm, parse_json_from_llm_response

        prompt = f"""You are evaluating whether a Genie AI response correctly answers a data question.

Question: {question}

Expected SQL: {expected_sql}
Expected result (sample): {str(expected_result.get("data", [])[:5])}

Genie SQL: {genie_sql or "(none)"}
Genie result (sample): {str(genie_result.get("data", [])[:5])}

Are these results semantically equivalent? Consider numeric rounding, column ordering, and formatting differences.

Respond with JSON: {{"verdict": "correct" or "incorrect", "confidence": 0.0-1.0, "reason": "..."}}"""

        response = call_llm(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=256,
        )
        parsed = parse_json_from_llm_response(response)
        verdict = parsed.get("verdict", "incorrect").lower()
        confidence = float(parsed.get("confidence", 0.5))
        reason = parsed.get("reason", "LLM evaluation")

        label = BenchmarkLabel.CORRECT if verdict == "correct" else BenchmarkLabel.INCORRECT
        return label, confidence, reason

    except Exception as e:
        logger.warning("LLM fallback failed: %s", e)
        return BenchmarkLabel.UNCERTAIN, 0.5, f"LLM fallback failed: {e}"


def evaluate_question(
    question: str,
    expected_sql: str,
    genie_response: GenieResponse,
    warehouse_id: str,
) -> EvaluationResult:
    """Evaluate a single question by comparing expected vs Genie results.

    Flow:
    1. No SQL from Genie → NO_SQL
    2. Execute expected SQL → if error → EXPECTED_SQL_ERROR
    3. Get Genie result (from response.result or execute generated_sql)
    4. Programmatic comparison
    5. LLM fallback for UNCERTAIN

    Args:
        question: The natural-language question.
        expected_sql: The ground-truth SQL.
        genie_response: Full GenieResponse from the API.
        warehouse_id: Databricks warehouse to execute expected SQL against.

    Returns:
        EvaluationResult with label, metrics, and comparison detail.
    """
    # Step 1: Check if Genie produced SQL
    if not genie_response.generated_sql:
        return EvaluationResult(
            label=BenchmarkLabel.NO_SQL,
            comparison_detail="Genie did not generate SQL",
        )

    # Step 2: Execute expected SQL
    expected_exec = execute_sql(expected_sql, warehouse_id=warehouse_id)
    if expected_exec.get("error"):
        return EvaluationResult(
            label=BenchmarkLabel.EXPECTED_SQL_ERROR,
            comparison_detail=f"Expected SQL execution failed: {expected_exec['error']}",
        )

    expected_metrics = ExecutionMetrics(row_count=expected_exec.get("row_count", 0))

    # Step 3: Get Genie result
    genie_result = genie_response.result
    if genie_result is None:
        # Fall back to executing the generated SQL
        genie_exec = execute_sql(genie_response.generated_sql, warehouse_id=warehouse_id)
        if genie_exec.get("error"):
            return EvaluationResult(
                label=BenchmarkLabel.INCORRECT,
                expected_result=expected_exec,
                expected_metrics=expected_metrics,
                comparison_detail=f"Genie SQL execution failed: {genie_exec['error']}",
            )
        genie_result = genie_exec

    genie_metrics = ExecutionMetrics(
        row_count=genie_result.get("row_count", 0),
        execution_time_ms=genie_response.duration_seconds * 1000
        if genie_response.duration_seconds
        else None,
    )

    # Step 4: Programmatic comparison
    order_sensitive = _detect_order_by(expected_sql)
    label, detail = _compare_results(expected_exec, genie_result, order_sensitive)

    # Step 5: LLM fallback for UNCERTAIN
    if label == BenchmarkLabel.UNCERTAIN:
        label, confidence, detail = _llm_compare(
            question=question,
            expected_sql=expected_sql,
            expected_result=expected_exec,
            genie_sql=genie_response.generated_sql,
            genie_result=genie_result,
        )
        return EvaluationResult(
            label=label,
            confidence=confidence,
            expected_result=expected_exec,
            expected_metrics=expected_metrics,
            genie_metrics=genie_metrics,
            comparison_detail=detail,
        )

    return EvaluationResult(
        label=label,
        confidence=1.0,
        expected_result=expected_exec,
        expected_metrics=expected_metrics,
        genie_metrics=genie_metrics,
        comparison_detail=detail,
    )
