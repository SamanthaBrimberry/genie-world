---
sidebar_position: 3
title: Benchmarks
---

# Benchmarks

The Benchmarks block runs questions against a live Genie Space, evaluates accuracy, diagnoses failures, and generates targeted improvements.

## Pipeline

```
run → evaluate → diagnose → suggest → update
```

1. **Runner** — sends questions to the Genie API via `GenieClient`
2. **Evaluator** — hybrid comparison (programmatic matching + LLM fallback for ambiguous cases)
3. **Diagnoser** — classifies failure types (wrong table, missing join, wrong aggregation, etc.) and flags performance warnings on correct answers with slow queries
4. **Suggester** — generates config change suggestions from diagnoses, routing each failure type to a specific config section
5. **Updater** — fetches current config, merges suggestions, applies via PATCH API

## Basic Usage

```python
from genie_world.benchmarks import run_benchmarks, diagnose_failures, generate_suggestions, update_space

# Run benchmarks (with optional custom questions and parallelism)
results = run_benchmarks(
    space_id,
    warehouse_id,
    custom_questions=[{"question": "Total revenue?", "expected_sql": "SELECT SUM(revenue) FROM sales"}],
    max_workers=4,
)
print(f"Accuracy: {results.accuracy:.0%}")

# Diagnose and fix
diagnoses = diagnose_failures(results)
suggestions = generate_suggestions(diagnoses, results, warehouse_id)
update_space(space_id, suggestions, warehouse_id)
```

## Auto-Tune

The `tune_space()` function wraps the full pipeline in an iterative loop:

```python
from genie_world.benchmarks import tune_space

result = tune_space(
    space_id,
    warehouse_id,
    custom_questions=[{"question": "...", "expected_sql": "..."}],
    target_accuracy=0.9,
    max_iterations=3,
    max_workers=4,
    auto_approve=True,
)

print(f"Final accuracy: {result.final_accuracy:.0%}")
print(f"Iterations: {len(result.iterations)}")
```

Each iteration runs the full benchmark → diagnose → suggest → update cycle until the target accuracy is reached or max iterations are exhausted.

### Manual review mode

When `auto_approve=False` (the default), `tune_space()` runs **one** iteration only: benchmark → diagnose → suggest. It does **not** apply the suggestions. The returned `TuneResult` contains the pending suggestions for manual review. After reviewing, call `update_space()` yourself and re-run.

```python
result = tune_space(space_id, warehouse_id)  # auto_approve defaults to False
# Inspect result.suggestions_applied (empty) and result.iterations[0]
# Apply manually after review:
# update_space(space_id, suggestions, warehouse_id)
```

## Accuracy Formula

```
accuracy = correct / (correct + incorrect + no_sql)
```

`uncertain` and `expected_sql_error` results are excluded from the denominator. This means accuracy reflects only questions where the system made a definitive attempt.

## Evaluation Labels

The evaluator assigns one of five labels to each question:

| Label | Meaning |
|-------|---------|
| `correct` | Genie's answer matches the expected result |
| `incorrect` | Genie's answer is wrong |
| `no_sql` | Genie did not generate SQL for the question |
| `expected_sql_error` | The expected SQL itself errors (test-data issue, excluded from accuracy) |
| `uncertain` | The evaluator could not confidently determine correctness (excluded from accuracy) |

## Failure Types

The diagnoser classifies failures into categories:

- `wrong_table` — Genie queried the wrong table
- `missing_join` — A required join was missing
- `wrong_aggregation` — Incorrect GROUP BY or aggregate function
- `wrong_filter` — Incorrect WHERE clause
- `missing_filter` — A required filter was omitted
- `entity_mismatch` — Genie used a synonym or alias that doesn't match the schema
- `wrong_column` — Genie selected the wrong column(s)
- `wrong_date_handling` — Genie handled dates or time incorrectly
- `missing_example` — No example exists for this type of question
- `ambiguous_query` — The question is inherently ambiguous

The diagnoser also checks **all** non-error questions (including correct ones) for performance. If Genie's query takes more than 10x longer than the expected SQL, a performance warning is attached to the diagnosis.

## Suggestion Routing

The suggester routes each failure type to a targeted config change:

| Failure type | Config change |
|---|---|
| `missing_example`, `wrong_aggregation` | Add to `example_question_sqls` |
| `wrong_table`, `wrong_column`, `missing_join` | Add to `text_instructions` |
| `missing_filter`, `wrong_filter` | Add to `sql_snippets` (filter) |
| `entity_mismatch` | Update `column_configs` synonyms |
| `wrong_date_handling` | Add expression snippet + instruction |
| `ambiguous_query` (or unknown) | Add to `text_instructions` |
