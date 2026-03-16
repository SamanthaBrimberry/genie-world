"""Deterministic generator: SchemaProfile → data_sources config section."""

from __future__ import annotations

from genie_world.profiler.models import ColumnProfile, SchemaProfile

_INTERNAL_COLUMN_PATTERNS = {"_metadata", "_rescued_data", "_commit_version", "_commit_timestamp"}
_DATE_TYPES = {"DATE", "TIMESTAMP"}
_STRING_TYPES = {"STRING", "CHAR", "VARCHAR"}
_NUMERIC_TYPES = {"INT", "BIGINT", "LONG", "SHORT", "BYTE", "FLOAT", "DOUBLE", "DECIMAL"}
_ENTITY_MATCHING_CARDINALITY_THRESHOLD = 100

# Column name patterns that strongly suggest entity matching should be enabled
_ENTITY_MATCHING_NAME_PATTERNS = {
    "name", "status", "type", "category", "region", "market", "platform",
    "gender", "country", "state", "city", "segment", "tier", "level",
    "group", "role", "format", "channel", "source", "label", "tag",
    "department", "division", "brand", "vendor", "supplier",
}


def _should_enable_entity_matching(col: ColumnProfile) -> bool:
    """Determine if entity matching should be enabled for a column.

    Enables entity matching for string columns that are likely categorical/dimension
    columns users will filter by name. Uses cardinality when available, falls back
    to name-based heuristics.
    """
    is_string = col.data_type.upper() in _STRING_TYPES
    if not is_string:
        return False

    # If we have cardinality data, use it
    if col.cardinality is not None:
        return col.cardinality < _ENTITY_MATCHING_CARDINALITY_THRESHOLD

    # If column has synonyms, it's likely a dimension column
    if col.synonyms:
        return True

    # Fall back to name-based heuristics
    col_lower = col.name.lower()
    for pattern in _ENTITY_MATCHING_NAME_PATTERNS:
        if pattern in col_lower:
            return True

    # Columns ending in common dimension suffixes
    if col_lower.endswith(("_name", "_type", "_status", "_category")):
        return True

    return False


def _should_enable_format_assistance(col: ColumnProfile) -> bool:
    """Determine if format assistance should be enabled for a column.

    Enables format assistance for date/timestamp columns and string columns
    that are likely categorical (helps Genie show format hints to users).
    """
    is_date = col.data_type.upper() in _DATE_TYPES
    if is_date:
        return True

    # String columns that qualify for entity matching also benefit from format assistance
    if _should_enable_entity_matching(col):
        return True

    return False


_NON_QUERYABLE_TYPES = {"BINARY", "STRUCT", "MAP", "ARRAY"}


def _table_recommendation(table) -> str | None:
    """Return a reason to consider excluding this table, or None if it looks fine.

    This does NOT auto-exclude — it surfaces recommendations for the user to decide.
    """
    if table.columns:
        non_queryable = sum(
            1 for c in table.columns
            if any(t in c.data_type.upper() for t in _NON_QUERYABLE_TYPES)
        )
        if non_queryable / len(table.columns) > 0.5:
            return f"Most columns ({non_queryable}/{len(table.columns)}) are non-queryable types (BINARY, STRUCT, etc.)"

    if not table.columns:
        return "Table has no columns"

    return None


def suggest_table_exclusions(profile: SchemaProfile) -> list[dict]:
    """Analyze profile and suggest tables that may not be useful in a Genie Space.

    Returns a list of {"table": name, "reason": why} recommendations.
    Users should review and decide — this does NOT auto-exclude anything.
    """
    suggestions = []
    for table in profile.tables:
        reason = _table_recommendation(table)
        if reason:
            suggestions.append({"table": table.table, "reason": reason})
    return suggestions


def generate_data_sources(
    profile: SchemaProfile,
    *,
    exclude_tables: list[str] | None = None,
    include_tables: list[str] | None = None,
) -> dict:
    """Transform a SchemaProfile into the data_sources config section.

    Args:
        profile: The SchemaProfile to transform.
        exclude_tables: Table names to explicitly exclude.
        include_tables: If set, ONLY include these tables.

    Produces table entries with column_configs including descriptions,
    synonyms, entity matching, format assistance, and exclusions.
    Tables sorted by identifier, column_configs sorted by column_name.
    """
    exclude_set = set(t.lower() for t in (exclude_tables or []))
    include_set = set(t.lower() for t in (include_tables or []))

    tables = []

    for table in profile.tables:
        # Table filtering — only explicit include/exclude, no silent auto-filtering
        if include_set and table.table.lower() not in include_set:
            continue
        if table.table.lower() in exclude_set:
            continue

        identifier = f"{table.catalog}.{table.schema_name}.{table.table}"

        column_configs = []
        for col in table.columns:
            config: dict = {"column_name": col.name}

            if col.description:
                config["description"] = [col.description]

            if col.synonyms:
                config["synonyms"] = col.synonyms

            if _should_enable_entity_matching(col):
                config["enable_entity_matching"] = True

            if _should_enable_format_assistance(col):
                config["enable_format_assistance"] = True

            # Exclude internal columns
            if col.name.lower() in _INTERNAL_COLUMN_PATTERNS or col.name.startswith("_"):
                config["exclude"] = True

            column_configs.append(config)

        # Sort column_configs by column_name
        column_configs.sort(key=lambda c: c["column_name"])

        entry: dict = {"identifier": identifier, "column_configs": column_configs}
        if table.description:
            entry["description"] = [table.description]

        tables.append(entry)

    # Sort tables by identifier
    tables.sort(key=lambda t: t["identifier"])

    return {"tables": tables}
