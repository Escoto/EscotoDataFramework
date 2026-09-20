"""SourcePipeline protocol — the contract every origin reader implements."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


@runtime_checkable
class SourcePipeline(Protocol):
    """Read from a configured origin and return a DataFrame (streaming or batch)."""

    def read(self, ctx: Context) -> DataFrame: ...
