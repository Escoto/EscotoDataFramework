"""FULL verb — replace the target with the current dataset."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from data_framework.context.config import Verb
from data_framework.output.base import Requirements
from data_framework.output.mechanics import log_rows_written, merge_schema, require_creatable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context

_SOURCE = "FullWriter"


class FullWriter:
    verb: ClassVar[Verb] = Verb.FULL
    requires: ClassVar[Requirements] = Requirements()

    def write(self, df: DataFrame, ctx: Context) -> None:
        # Never wipe a table because an upstream export was missing or empty.
        # isEmpty short-circuits on the first row rather than counting.
        if df.isEmpty():
            ctx.logger.warning(
                name="full_load_skipped",
                source=_SOURCE,
                description=(
                    f"Source produced no rows; left {ctx.target_table} as it was "
                    "rather than overwriting it with nothing"
                ),
            )
            return

        require_creatable(df, ctx)

        (
            df.write.format("delta")
            .mode("overwrite")
            .option("mergeSchema", merge_schema(ctx))
            .saveAsTable(ctx.target_table)
        )
        log_rows_written(ctx, event="rows_overwritten", source=_SOURCE)
