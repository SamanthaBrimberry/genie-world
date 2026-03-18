"""Space Builder block for genie-world.

Generates complete Genie Space configurations from SchemaProfile data.
"""

from __future__ import annotations

import logging

from pydantic import BaseModel

from genie_world.builder.assembler import assemble_space
from genie_world.builder.benchmarks import generate_benchmarks
from genie_world.builder.data_sources import generate_data_sources, suggest_table_exclusions
from genie_world.builder.deployer import create_space
from genie_world.builder.example_sqls import generate_example_sqls
from genie_world.builder.instructions import generate_instructions
from genie_world.builder.join_specs import generate_join_specs
from genie_world.builder.snippets import generate_snippets
from genie_world.core.tracing import trace
from genie_world.profiler.models import SchemaProfile

logger = logging.getLogger(__name__)


class BuilderWarning(BaseModel):
    """Warning generated during space building."""
    section: str
    message: str
    detail: str | None = None


class BuildResult(BaseModel):
    """Result from build_space() containing config and any warnings."""
    config: dict
    warnings: list[BuilderWarning]


@trace(name="build_space", span_type="CHAIN")
def build_space(
    profile: SchemaProfile,
    *,
    warehouse_id: str | None = None,
    example_count: int = 10,
    benchmark_count: int = 10,
    include_tables: list[str] | None = None,
    exclude_tables: list[str] | None = None,
    sql_functions: list[dict] | None = None,
    metric_views: list[dict] | None = None,
) -> BuildResult:
    """Generate a complete Genie Space config from a SchemaProfile.

    Args:
        profile: SchemaProfile from the profiler.
        warehouse_id: If provided, generated SQL is validated and fixed.
        example_count: Number of example Q&A pairs to generate.
        benchmark_count: Number of benchmark questions to generate.
        include_tables: If set, only include these tables (by short name).
        exclude_tables: Table names to explicitly exclude.
        sql_functions: Optional pass-through UC function references.
        metric_views: Optional pass-through metric views.

    Returns BuildResult with config dict and list of BuilderWarnings.
    """
    warnings: list[BuilderWarning] = []

    if not warehouse_id:
        warnings.append(BuilderWarning(
            section="general",
            message="SQL validation skipped — no warehouse_id provided.",
        ))

    # Surface table exclusion suggestions
    suggestions = suggest_table_exclusions(profile)
    for s in suggestions:
        if not include_tables and s["table"].lower() not in set(t.lower() for t in (exclude_tables or [])):
            warnings.append(BuilderWarning(
                section="data_sources",
                message=f"Consider excluding table '{s['table']}': {s['reason']}",
            ))

    # 1. Deterministic generators
    data_sources = generate_data_sources(
        profile,
        include_tables=include_tables,
        exclude_tables=exclude_tables,
    )
    join_specs = generate_join_specs(profile)

    # 2. LLM: snippets
    snippets = generate_snippets(profile)

    # 3. LLM + validate: examples
    examples, example_warnings = generate_example_sqls(
        profile, join_specs, snippets, warehouse_id=warehouse_id, count=example_count,
    )
    for w in example_warnings:
        warnings.append(BuilderWarning(section="example_sqls", message=w))

    # 4. LLM + validate: benchmarks (receives examples to avoid overlap)
    benchmarks, bench_warnings = generate_benchmarks(
        profile, join_specs, snippets, examples,
        warehouse_id=warehouse_id, count=benchmark_count,
    )
    for w in bench_warnings:
        warnings.append(BuilderWarning(section="benchmarks", message=w))

    # 5. LLM: instructions LAST
    instructions = generate_instructions(profile, join_specs, snippets, examples)

    # 6. Assemble
    config = assemble_space(
        data_sources=data_sources,
        join_specs=join_specs,
        instructions=instructions,
        snippets=snippets,
        examples=examples,
        benchmarks=benchmarks if benchmarks.get("questions") else None,
        sql_functions=sql_functions,
        metric_views=metric_views,
    )

    return BuildResult(config=config, warnings=warnings)


__all__ = [
    "BuilderWarning",
    "BuildResult",
    "build_space",
    "create_space",
    "assemble_space",
    "generate_data_sources",
    "generate_join_specs",
    "generate_snippets",
    "generate_example_sqls",
    "generate_benchmarks",
    "generate_instructions",
]
