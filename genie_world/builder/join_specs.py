"""Deterministic generator: relationships → join_specs config section."""

from __future__ import annotations

from genie_world.profiler.models import SchemaProfile

MIN_CONFIDENCE = 0.6


def _short_name(full_table: str) -> str:
    """Extract short table name from fully qualified name."""
    parts = full_table.split(".")
    return parts[-1] if parts else full_table


def generate_join_specs(profile: SchemaProfile) -> list[dict]:
    """Transform high-confidence relationships into join_specs.

    Only includes relationships with confidence >= 0.6.
    Generates SQL join conditions, comments, and instructions.
    """
    specs = []

    for rel in profile.relationships:
        if rel.confidence < MIN_CONFIDENCE:
            continue

        left_alias = _short_name(rel.source_table)
        right_alias = _short_name(rel.target_table)

        confidence_label = "high" if rel.confidence >= 0.9 else "medium"
        instruction_strength = (
            f"Always use this join when combining {left_alias} and {right_alias} data."
            if rel.confidence >= 0.9
            else f"Use this join when {right_alias} attributes are needed for {left_alias} analysis."
        )

        specs.append({
            "left": {"identifier": rel.source_table, "alias": left_alias},
            "right": {"identifier": rel.target_table, "alias": right_alias},
            "sql": f"{left_alias}.{rel.source_column} = {right_alias}.{rel.target_column}",
            "comment": f"Join {left_alias} to {right_alias} on {rel.source_column} ({confidence_label} confidence, detected via {rel.detection_method.value}).",
            "instruction": instruction_strength,
        })

    return specs
