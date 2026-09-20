"""Write mechanics shared by the Delta verbs: schema evolution, dedup, promotion."""

from __future__ import annotations

from typing import TYPE_CHECKING

from functools import reduce
from operator import and_

from pyspark.sql import Window
from pyspark.sql import functions as F

from data_framework.context.config import SchemaEvolution

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from data_framework.context.context import Context

METADATA_PREFIX = "__"
BRONZE_TIMESTAMP = "__BRONZE_LAST_MODIFIED_DT"
SILVER_TIMESTAMP = "__SILVER_LAST_MODIFIED_DT"
FILE_PATH = "__FILEPATH"

# History columns, written by SCD2 and COMPLETE_DELTA only.
START_DATE = "__START_DATE"
END_DATE = "__END_DATE"
CURRENT_FLAG = "__CURRENT_FLAG"
DELETED_FLAG = "__DELETED_FLAG"

CURRENT = "Y"
EXPIRED = "N"
DELETED = "Y"
LIVE = "N"

_DEDUP_RANK = "_dedup_rank"


class EmptySourceSchemaError(Exception):
    """Raised when the target does not exist and the batch has no columns to build it."""


# The modes under which a batch can arrive carrying a column the target lacks.
_EVOLVING = frozenset(
    {
        SchemaEvolution.ADD_NEW_COLUMNS,
        SchemaEvolution.ADD_NEW_COLUMNS_WITH_TYPE_WIDENING,
    }
)


def merge_schema(ctx: Context) -> str:
    """mergeSchema follows the declared schema evolution.

    Listed explicitly rather than compared against one mode: a mode that lets Auto
    Loader grow the batch needs the write to accept the growth, and an equality test
    would quietly answer "false" and refuse the very column Auto Loader just added.
    """
    return "true" if ctx.config.source.schema_evolution in _EVOLVING else "false"


def as_timestamp(column: str, fmt: str | None, alias: str | None = None) -> Column:
    """Normalize a date column for comparison without persisting a helper column.

    The format is passed to to_timestamp as a value, never interpolated into SQL text,
    so merge conditions stay free of string-built predicates.
    """
    value = F.col(f"{alias}.`{column}`" if alias else f"`{column}`")
    return F.to_timestamp(value, fmt) if fmt else value.cast("timestamp")


def require_creatable(df: DataFrame, ctx: Context) -> None:
    """A first run with no columns cannot define the target — fail rather than guess."""
    if df.columns:
        return
    if ctx.spark.catalog.tableExists(ctx.target_table):
        return
    raise EmptySourceSchemaError(
        f"Cannot create {ctx.target_table}: the source produced no columns"
    )


def deduplicate(df: DataFrame, ctx: Context) -> DataFrame:
    """Keep the latest row per key within the batch, when configured.

    Ordering uses an expression rather than a derived column, so nothing extra is
    written to the target.
    """
    dedup = ctx.config.output.dedup
    if not dedup.enabled or not dedup.order_by:
        return df

    columns = dedup.columns or [c for c in df.columns if not c.startswith(METADATA_PREFIX)]
    window = Window.partitionBy(*[F.col(f"`{c}`") for c in columns]).orderBy(
        as_timestamp(dedup.order_by, dedup.order_by_format).desc()
    )
    return (
        df.withColumn(_DEDUP_RANK, F.row_number().over(window))
        .filter(F.col(_DEDUP_RANK) == 1)
        .drop(_DEDUP_RANK)
    )


def promote(df: DataFrame) -> DataFrame:
    """Stamp the framework's write time and drop the previous layer's."""
    return df.withColumn(SILVER_TIMESTAMP, F.current_timestamp()).drop(BRONZE_TIMESTAMP)


def key_condition(keys: list[str], source: str, target: str) -> Column:
    """The join predicate for a merge, as a Column rather than built SQL text."""
    return reduce(
        and_,
        [F.col(f"{source}.`{key}`") == F.col(f"{target}.`{key}`") for key in keys],
    )


def open_history(df: DataFrame, start: Column) -> DataFrame:
    """Stamp a batch as the current, live version of each record it carries.

    The validity window opens at the record's own event time rather than at write
    time, so replaying a backlog reconstructs the real history instead of collapsing
    it onto the moment the job happened to run.
    """
    return (
        df.withColumn(START_DATE, start)
        .withColumn(END_DATE, F.lit(None).cast("timestamp"))
        .withColumn(CURRENT_FLAG, F.lit(CURRENT))
        .withColumn(DELETED_FLAG, F.lit(LIVE))
    )


def log_rows_written(ctx: Context, event: str, source: str) -> None:
    """Report the row count from Delta's own commit metrics.

    Counting the batch would mean a second pass over the source; the table's last
    commit already knows exactly how many rows landed.
    """
    history = ctx.spark.sql(f"DESCRIBE HISTORY {ctx.target_table} LIMIT 1").collect()
    metrics = (history[0]["operationMetrics"] or {}) if history else {}
    written = metrics.get("numOutputRows") or metrics.get("numTargetRowsInserted") or 0

    ctx.logger.kpi(
        name=event, total=int(written), description=f"{source} wrote to {ctx.target_table}"
    )
