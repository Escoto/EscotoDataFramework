"""Native policy rules: not_null, schema_drift."""

from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar

from data_framework.policies.base import PolicyResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


class NotNullRule:
    """Check that specified columns contain no null values."""

    name: ClassVar[str] = "not_null"

    def __init__(self, columns: list[str]):
        self.columns = columns

    def evaluate(self, df: DataFrame, ctx: Context) -> PolicyResult:
        raise NotImplementedError("P5")


class SchemaDriftRule:
    """Report columns added or missing vs the target table."""

    name: ClassVar[str] = "schema_drift"

    def evaluate(self, df: DataFrame, ctx: Context) -> PolicyResult:
        raise NotImplementedError("P5")
