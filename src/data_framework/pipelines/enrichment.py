"""Enrichment — provenance columns, column sanitization, rename patterns."""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from data_framework.context.context import Context

# Source exports carry a 14-digit stamp in the file name: yyyyMMddHHmmss.
_EXPORT_DATE_REGEX = r"(\d{14})"
_EXPORT_DATE_FORMAT = "yyyyMMddHHmmss"

# Characters a Delta column name cannot carry.
_UNSAFE_CHARS = re.compile(r"[ ,;./]")

# Auto Loader's catch-all for values that did not fit the inferred schema.
_RESCUED_COLUMN = "_rescued_data"

_SOURCE = "enrichment"


def _export_date(file_path: Column) -> Column:
    """A file name with no 14-digit stamp yields NULL rather than failing the read."""
    stamp = F.regexp_extract(file_path, _EXPORT_DATE_REGEX, 1)
    return F.to_timestamp(stamp, _EXPORT_DATE_FORMAT)


def add_provenance(df: DataFrame) -> DataFrame:
    """Add __bronze_last_modified_dt, __filePath and __EXPORT_DATE.

    Runs before sanitization, so the patterns match the source's own casing and are
    uppercased into the metadata contract (__FILEPATH, __EXPORT_DATE, ...) there.
    """
    file_path = F.col("_metadata.file_path")
    return (
        df.withColumn("__bronze_last_modified_dt", F.current_timestamp())
        .withColumn("__filePath", file_path)
        .withColumn("__EXPORT_DATE", _export_date(file_path))
    )


def sanitize_column_names(df: DataFrame, ctx: Context) -> DataFrame:
    """Replace ' ,;./' with '_', uppercase every name, drop _rescued_data."""
    kept = [name for name in df.columns if name != _RESCUED_COLUMN]
    renames = {name: _UNSAFE_CHARS.sub("_", name).upper() for name in kept}

    changed = {old: new for old, new in renames.items() if old != new}
    if changed:
        ctx.logger.info(
            name="columns_sanitized",
            source=_SOURCE,
            description=f"Sanitized {len(changed)} column name(s)",
            total=len(changed),
            metadata=json.dumps(changed, sort_keys=True),
        )

    return _select_renamed(df, renames)


def apply_rename_patterns(df: DataFrame, patterns: list[str]) -> DataFrame:
    """Apply 'regex=replacement' renames, in order, to every column name.

    SourceConfig has already checked the shape and that each regex compiles.
    """
    if not patterns:
        return df

    rules = []
    for pattern in patterns:
        expression, _, replacement = pattern.partition("=")
        rules.append((re.compile(expression), replacement))

    renames = {}
    for name in df.columns:
        renamed = name
        for compiled, replacement in rules:
            renamed = compiled.sub(replacement, renamed)
        renames[name] = renamed

    return _select_renamed(df, renames)


def _select_renamed(df: DataFrame, renames: dict[str, str]) -> DataFrame:
    """Rename (and implicitly drop) in a single projection.

    Names are backticked: an unsanitized column containing '.' would otherwise
    read as nested-field access.
    """
    return df.select([F.col(f"`{old}`").alias(new) for old, new in renames.items()])
