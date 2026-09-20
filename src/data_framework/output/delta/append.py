"""APPEND verb — add the incoming records to the target, no keys, no history."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from data_framework.context.config import Verb
from data_framework.output.base import Requirements
from data_framework.output.mechanics import log_rows_written, merge_schema, require_creatable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context

_SOURCE = "AppendWriter"


class AppendWriter:
    verb: ClassVar[Verb] = Verb.APPEND
    requires: ClassVar[Requirements] = Requirements()

    def write(self, df: DataFrame, ctx: Context) -> None:
        require_creatable(df, ctx)

        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", merge_schema(ctx))
            .saveAsTable(ctx.target_table)
        )
        log_rows_written(ctx, event="rows_appended", source=_SOURCE)
