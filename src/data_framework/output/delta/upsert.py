"""UPSERT verb — SCD Type 1: latest state per key, no history."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from delta.tables import DeltaTable

from data_framework.context.config import Verb
from data_framework.output.base import Requirements
from data_framework.output.mechanics import (
    as_timestamp,
    deduplicate,
    log_rows_written,
    merge_schema,
    promote,
    require_creatable,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

    from data_framework.context.context import Context

_SOURCE = "UpsertWriter"
_EVENT = "rows_upserted"


class UpsertWriter:
    verb: ClassVar[Verb] = Verb.UPSERT
    requires: ClassVar[Requirements] = Requirements(keys=True)

    def write(self, df: DataFrame, ctx: Context) -> None:
        prepared = promote(deduplicate(df, ctx))
        require_creatable(prepared, ctx)

        if not ctx.spark.catalog.tableExists(ctx.target_table):
            self._create(prepared, ctx)
            return

        target = DeltaTable.forName(ctx.spark, ctx.target_table)
        matched = " AND ".join(f"t.`{key}` = s.`{key}`" for key in ctx.config.output.keys)
        merge = target.alias("t").merge(prepared.alias("s"), matched)

        # Schema evolution on a merge needs spark.databricks.delta.schema.autoMerge;
        # the mergeSchema write option does not apply here. New columns on an UPSERT
        # target are out of scope until that is configured deliberately.
        # `if newer` would evaluate the Column's truthiness, which raises.
        newer = _newer_than_target(ctx)
        if newer is None:
            merge = merge.whenMatchedUpdateAll()
        else:
            merge = merge.whenMatchedUpdateAll(condition=newer)
        merge.whenNotMatchedInsertAll().execute()

        log_rows_written(ctx, event=_EVENT, source=_SOURCE)

    def _create(self, df: DataFrame, ctx: Context) -> None:
        """First load defines the table; there is nothing to merge against yet."""
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", merge_schema(ctx))
            .saveAsTable(ctx.target_table)
        )
        log_rows_written(ctx, event=_EVENT, source=_SOURCE)


def _newer_than_target(ctx: Context) -> Column | None:
    """Newer-wins when an event time is configured; otherwise the batch always wins."""
    event_time = ctx.config.output.event_time
    if not event_time:
        return None
    return as_timestamp(event_time.column, event_time.format, alias="s") > as_timestamp(
        event_time.column, event_time.format, alias="t"
    )
