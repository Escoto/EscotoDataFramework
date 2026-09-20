"""Policy runner — evaluate all configured rules and enforce fail severity."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


class PolicyRunner:
    """Evaluate configured policies in order, log results, raise on fail-severity violations."""

    def run(self, df: DataFrame, ctx: Context) -> None:
        raise NotImplementedError("P5")
