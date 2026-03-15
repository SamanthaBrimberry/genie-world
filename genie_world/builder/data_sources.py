"""Deterministic generator: SchemaProfile → data_sources config section."""

from __future__ import annotations

from genie_world.profiler.models import ColumnProfile, SchemaProfile

_INTERNAL_COLUMN_PATTERNS = {"_metadata", "_rescued_data", "_commit_version", "_commit_timestamp"}
_DATE_TYPES = {"DATE", "TIMESTAMP"}
_STRING_TYPES = {"STRING", "CHAR", "VARCHAR"}
_ENTITY_MATCHING_CARDINALITY_THRESHOLD = 100


def generate_data_sources(profile: SchemaProfile) -> dict:
    """Transform a SchemaProfile into the data_sources config section.

    Produces table entries with column_configs including descriptions,
    synonyms, entity matching, format assistance, and exclusions.
    Tables sorted by identifier, column_configs sorted by column_name.
    """
    tables = []

    for table in profile.tables:
        identifier = f"{table.catalog}.{table.schema_name}.{table.table}"

        column_configs = []
        for col in table.columns:
            config: dict = {"column_name": col.name}

            if col.description:
                config["description"] = [col.description]

            if col.synonyms:
                config["synonyms"] = col.synonyms

            # Entity matching: string columns with low cardinality and synonyms
            is_string = col.data_type.upper() in _STRING_TYPES
            low_card = col.cardinality is not None and col.cardinality < _ENTITY_MATCHING_CARDINALITY_THRESHOLD
            if is_string and (low_card or col.synonyms):
                config["enable_entity_matching"] = True

            # Format assistance: date/timestamp columns and low-cardinality strings
            is_date = col.data_type.upper() in _DATE_TYPES
            if is_date or (is_string and low_card):
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
