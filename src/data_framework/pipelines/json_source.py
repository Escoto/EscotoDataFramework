"""JSON origin — Auto Loader with cloudFiles format."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from data_framework.context.context import Context


class JsonSource:
    """Read JSON files via Auto Loader streaming."""

    def read(self, ctx: Context) -> DataFrame:
        raise NotImplementedError("P6")
