# Databricks notebook source
# MAGIC %md
# MAGIC # Genie World — Tracing Demo
# MAGIC
# MAGIC Shows the current tracing capabilities: MLflow trace spans on every key operation, Genie state transitions, and benchmark timing data.

# COMMAND ----------

# MAGIC %pip install git+https://github.com/SamanthaBrimberry/genie-world.git --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup: MLflow Experiment
# MAGIC
# MAGIC All traces will be logged to this experiment. View them at **Machine Learning → Experiments → Traces**.

# COMMAND ----------

import mlflow

EXPERIMENT_NAME = "/Shared/genie-world-tracing-demo"
mlflow.set_experiment(EXPERIMENT_NAME)
print(f"Traces will appear in: {EXPERIMENT_NAME}")

# COMMAND ----------

CATALOG = "sammy_demo_workspace_catalog"
SCHEMA = "album"
WAREHOUSE_ID = "4dcffba4857fc161"

# An existing Genie Space to trace against
SPACE_ID = "01f120e0e7b918df817b5c7a53285f7d"

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Trace 1: Profile a Schema
# MAGIC
# MAGIC The profiler's `@trace` decorator logs spans for metadata extraction, synonym generation, and description enrichment.

# COMMAND ----------

from genie_world.profiler import profile_schema

with mlflow.start_span(name="demo_profile_schema") as span:
    span.set_inputs({"catalog": CATALOG, "schema": SCHEMA})

    profile = profile_schema(
        CATALOG, SCHEMA,
        synonyms=True,
        enrich_descriptions=True,
    )

    span.set_attributes({
        "tables_profiled": len(profile.tables),
        "relationships_found": len(profile.relationships),
        "columns_total": sum(len(t.columns) for t in profile.tables),
        "columns_with_synonyms": sum(1 for t in profile.tables for c in t.columns if c.synonyms),
    })
    span.set_outputs({"tables": [t.table for t in profile.tables]})

print(f"Profiled {len(profile.tables)} tables with {len(profile.relationships)} relationships")
print("→ Check MLflow Traces tab for span details")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Trace 2: Query Genie (State Transitions)
# MAGIC
# MAGIC `GenieClient.ask()` captures every state transition Genie goes through: `FETCHING_METADATA → FILTERING_CONTEXT → ASKING_AI → EXECUTING_QUERY → COMPLETED`.

# COMMAND ----------

from genie_world.core.genie_client import GenieClient

client = GenieClient(SPACE_ID)

questions = [
    "How many campaigns are there?",
    "What is the total spend by platform?",
    "Which artist has the most campaigns?",
]

for question in questions:
    with mlflow.start_span(name=f"genie_query") as span:
        span.set_inputs({"question": question, "space_id": SPACE_ID})

        response = client.ask(question)

        span.set_attributes({
            "status": response.status,
            "duration_seconds": response.duration_seconds,
            "states": response.states,
            "has_sql": response.generated_sql is not None,
            "row_count": response.result.get("row_count", 0) if response.result else 0,
        })

        if response.generated_sql:
            span.set_attributes({"generated_sql": response.generated_sql})

        span.set_outputs({
            "status": response.status,
            "generated_sql": response.generated_sql,
            "states": response.states,
            "duration": f"{response.duration_seconds:.1f}s",
        })

    states_str = " → ".join(response.states)
    sql_preview = response.generated_sql[:80] + "..." if response.generated_sql and len(response.generated_sql) > 80 else response.generated_sql
    print(f"\n  Q: {question}")
    print(f"  States: {states_str}")
    print(f"  Duration: {response.duration_seconds:.1f}s")
    print(f"  SQL: {sql_preview}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Trace 3: Benchmark Run
# MAGIC
# MAGIC `run_benchmarks()` traces the full evaluation pipeline — each question's Genie response, SQL execution, comparison, and LLM fallback.

# COMMAND ----------

from genie_world.benchmarks import run_benchmarks

with mlflow.start_span(name="demo_benchmark_run") as span:
    span.set_inputs({"space_id": SPACE_ID, "warehouse_id": WAREHOUSE_ID})

    results = run_benchmarks(SPACE_ID, WAREHOUSE_ID, max_workers=2)

    span.set_attributes({
        "accuracy": results.accuracy,
        "total": results.total,
        "correct": results.correct,
        "incorrect": results.incorrect,
        "no_sql": results.no_sql,
        "uncertain": results.uncertain,
    })
    span.set_outputs({
        "accuracy": f"{results.accuracy:.0%}",
        "results": [
            {"question": q.question[:50], "label": q.label.value, "confidence": q.confidence}
            for q in results.questions
        ],
    })

print(f"Accuracy: {results.accuracy:.0%} ({results.correct}/{results.total})")
for q in results.questions:
    status = "✓" if q.label.value == "correct" else "✗"
    print(f"  {status} [{q.label.value}] {q.question[:70]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Trace 4: Per-Question Timing Breakdown
# MAGIC
# MAGIC Shows latency data captured by the evaluator for each question.

# COMMAND ----------

print(f"{'Question':<55} {'Label':<12} {'Genie (ms)':<12} {'Expected (ms)':<14}")
print("-" * 93)
for q in results.questions:
    genie_ms = f"{q.genie_metrics.execution_time_ms:.0f}" if q.genie_metrics and q.genie_metrics.execution_time_ms else "—"
    expected_ms = f"{q.expected_metrics.execution_time_ms:.0f}" if q.expected_metrics and q.expected_metrics.execution_time_ms else "—"
    print(f"{q.question[:55]:<55} {q.label.value:<12} {genie_ms:<12} {expected_ms:<14}")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Trace 5: Build Space (LLM Calls Traced)
# MAGIC
# MAGIC `build_space()` traces each LLM generator — snippets, examples, benchmarks, instructions.

# COMMAND ----------

from genie_world.builder import build_space

with mlflow.start_span(name="demo_build_space") as span:
    span.set_inputs({"catalog": CATALOG, "schema": SCHEMA})

    build_result = build_space(
        profile,
        warehouse_id=WAREHOUSE_ID,
        include_tables=["campaigns", "campaign_groups", "campaign_platform_totals"],
        example_count=5,
        benchmark_count=5,
    )

    config = build_result.config
    span.set_attributes({
        "tables": len(config["data_sources"]["tables"]),
        "examples": len(config["instructions"]["example_question_sqls"]),
        "benchmarks": len(config.get("benchmarks", {}).get("questions", [])),
        "warnings": len(build_result.warnings),
    })
    span.set_outputs({"warnings": [w.message for w in build_result.warnings]})

print(f"Built config: {len(config['instructions']['example_question_sqls'])} examples, {len(config.get('benchmarks', {}).get('questions', []))} benchmarks")
print(f"→ Check MLflow Traces for individual LLM call spans")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## View Your Traces
# MAGIC
# MAGIC 1. Go to **Machine Learning → Experiments**
# MAGIC 2. Open the **genie-world-tracing-demo** experiment
# MAGIC 3. Click the **Traces** tab
# MAGIC 4. Click any trace to see the span tree:
# MAGIC    - `demo_profile_schema` → nested `profile_schema_metadata`, `generate_synonyms_for_table`, `enrich_descriptions_for_table` spans
# MAGIC    - `genie_query` → nested `genie_ask` span with state transitions in attributes
# MAGIC    - `demo_benchmark_run` → nested `evaluate_question` spans per question
# MAGIC    - `demo_build_space` → nested `generate_snippets`, `generate_example_sqls`, `generate_instructions` spans
# MAGIC
# MAGIC ### What's captured per span:
# MAGIC | Span | Attributes |
# MAGIC |------|-----------|
# MAGIC | `genie_ask` | question, status, states, duration, generated_sql, row_count |
# MAGIC | `evaluate_question` | label, confidence, comparison_detail |
# MAGIC | `generate_synonyms_for_table` | table name, synonym count |
# MAGIC | `validate_and_fix_sql` | sql, question, retry count |
# MAGIC
# MAGIC ### What's NOT yet captured (Observability block — planned):
# MAGIC - Persistent per-space trace history
# MAGIC - Accuracy trends over time
# MAGIC - Latency monitoring and regression detection
# MAGIC - Multi-space comparison dashboards
